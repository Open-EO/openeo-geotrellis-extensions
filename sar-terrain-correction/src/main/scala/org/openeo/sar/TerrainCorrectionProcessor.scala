package org.openeo.sar

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.vector.Extent
import org.openeo.geotrellis.layers.RasterTileLoader.SOFT_ERROR_MEGAPIXEL_COUNTER
import org.openeo.geotrelliscommon.BatchJobMetadataTracker
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.dem.{DemCatalog, DemGeometry, DemReader, ElevationModel}
import org.openeo.sar.metadata.{Polarisation, S1AnnotationParser, S1GrdMetadata}
import org.openeo.sar.stac.{StacAssets, StacItemLoader}
import org.slf4j.LoggerFactory

import java.net.URI

/** Top-level entry point.
 *
 *  The split into two stages is explicit to allow efficient multi-tile reads:
 *
 *  Stage 1  [[openScene]]     – O(1) per scene.  Parses the SAFE annotation
 *           XMLs (orbit, LUTs, SRGR polynomials), opens RasterSources for the
 *           measurement TIFFs / DEM / geoid.  Results are captured in a
 *           [[SceneContext]] which is immutable and cheap to share.
 *
 *  Stage 2  [[readExtents]]   – O(N) per tile.  Dispatches per-extent work to
 *           the backend.  Only windowed raster I/O and per-pixel geometry
 *           happen here; no XML parsing, no RasterSource construction.
 *
 *  The single-tile helper [[computeTile]] is retained for convenience
 *  (it calls both stages). */
final class TerrainCorrectionProcessor(
  backend: TerrainCorrectionBackend,
  geoidSourceFactory: Option[Extent => RasterSource],
  rasterSourceFactory: URI => RasterSource = TerrainCorrectionProcessor.defaultRasterSourceFactory,
  demCatalogFactory: SarProcessingConfig => DemCatalog = TerrainCorrectionProcessor.defaultDemCatalogFactory
) {

  // -------------------------------------------------------------------------
  // Stage 1: scene-level initialisation
  // -------------------------------------------------------------------------

  /** Parse a STAC item and open all required RasterSources.  Expensive; call
   *  once per scene, then reuse the returned [[SceneContext]] for all tiles.
   *
   *  `demSearchExtentWgs84` is the overall area of the request (all scenes): the DEM catalog is searched
   *  for that area once, and each scene keeps the DEM items overlapping it. */
  def openScene(stacItemUrl: URI,
                cellSize: CellSize,
                crs: CRS,
                polarisations: Seq[Polarisation],
                config: SarProcessingConfig = SarProcessingConfig.default,
                demSearchExtentWgs84: Option[Extent] = None): SceneContext = {

    val geoidFactory = geoidSourceFactory.getOrElse(throw new IllegalStateException(
      s"sar_backscatter: a geoid is required for terrain correction (item $stacItemUrl): " +
        "configure a geoid undulation GeoTIFF (e.g. S1_GEOID_TIFF_URI)"))

    val assets: StacAssets = StacItemLoader.load(stacItemUrl)

    val perPol = polarisations.map { pol =>
      val a = assets.perPol.getOrElse(pol.code,
        throw new IllegalArgumentException(s"sar_backscatter: polarisation ${pol.code} not present in item $stacItemUrl"))
      try S1AnnotationParser.parseProductAnnotation(
        a.productAnnotation, a.measurement, a.calibration, a.noise, pol)
      catch {
        case e: Exception => throw new IllegalStateException(
          s"sar_backscatter: failed to parse the ${pol.code} annotation of item $stacItemUrl: ${e.getMessage}", e)
      }
    }
    val meta: S1GrdMetadata =
      try S1AnnotationParser.assemble(perPol)
      catch {
        case e: Exception => throw new IllegalStateException(
          s"sar_backscatter: failed to assemble the metadata of item $stacItemUrl: ${e.getMessage}", e)
      }

    val sarSources: Map[Polarisation, RasterSource] = polarisations.map { pol =>
      pol -> rasterSourceFactory(assets.perPol(pol.code).measurement)
    }.toMap

    // The DEM catalog is searched for the whole request (once, cached); this scene keeps the items it overlaps.
    // Per-tile reads open only the items overlapping the tile.
    val (lonMin, latMin, lonMax, latMax) = assets.bboxWgs84
    val sceneParts = DemGeometry.splitAtAntimeridian(lonMin, latMin, lonMax, latMax, buffer = 0.05)
    // A projected extent crossing the antimeridian reprojects to ~-180..180: searching that would cover the whole
    // latitude band, and the items are filtered to the scene anyway, so search the scene instead.
    val searchParts = demSearchExtentWgs84.filter(_.width < 180.0)
      .map(DemGeometry.splitAtAntimeridian(_, buffer = 0.05)).getOrElse(sceneParts)
    val demItems = demCatalogFactory(config).items(searchParts)
      .filter(item => sceneParts.exists(_.intersects(item.bboxWgs84)))

    // The geoid source only needs an extent: the scene's overall box (the factory may be unaware of the antimeridian).
    val geoidSource = Some(geoidFactory(Extent(lonMin, latMin, lonMax, latMax).buffer(0.05)))

    SceneContext(meta, sarSources, demItems, geoidSource, cellSize, crs, polarisations, config, stacItemUrl.toString)
  }

  // -------------------------------------------------------------------------
  // Stage 2: per-tile compute
  // -------------------------------------------------------------------------

  /** Compute an output [[Raster]] for every extent in `extents`.
   *
   *  `bands` selects which output bands to include (0-based).
   *  The full band layout is determined by [[SarProcessingConfig]]; see
   *  [[org.openeo.sar.backend.TerrainCorrectionBackend]] for the index table.
   *
   *  Only per-tile windowed I/O and geometry happen inside this iterator;
   *  the scene-level [[SceneContext]] is closed over by reference. */
  def readExtents(scene: SceneContext,
                  extents: Traversable[Extent],
                  bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val totalBands = scene.config.bandCount(scene.polarisations.size)
    val effectiveBands = if (bands.isEmpty) (0 until totalBands) else bands
    extents.toIterator.map { extent =>
      val ctx  = scene.tileContext(extent)
      val full = backend.compute(ctx)           // always compute all bands; band selection is cheap???
      val selected = MultibandTile(effectiveBands.map(full.band))
      Raster(selected, extent)
    }
  }

  /** All bands, convenience overload. */
  def readExtents(scene: SceneContext,
                  extents: Traversable[Extent]): Iterator[Raster[MultibandTile]] =
    readExtents(scene, extents, Seq.empty)

  // -------------------------------------------------------------------------
  // Convenience: single tile (opens scene internally, fine for one-off calls)
  // -------------------------------------------------------------------------

  def computeTile(stacItemUrl: URI, request: TileRequest): MultibandTile = {
    val scene = openScene(stacItemUrl, request.cellSize, request.crs, request.polarisations, request.config)
    readExtents(scene, List(request.extent)).next().tile
  }
}

object TerrainCorrectionProcessor {

  private val logger = LoggerFactory.getLogger(getClass)

  def defaultRasterSourceFactory: URI => RasterSource = { uri =>
    // GeoTrellis RasterSource auto-dispatches on scheme: file:// http(s):// s3://
    geotrellis.raster.geotiff.GeoTiffRasterSource(uri.toString)
  }

  /** Factory wrapping a single GeoTIFF (typically the EGM2008 / EGM96
   *  undulation grid in EPSG:4326) as the geoid source. */
  def geoidFromTiff(uri: URI): Extent => RasterSource = {
    val rs = defaultRasterSourceFactory(uri)
    _ => rs
  }

  /** DEM per the `elevation_model` / `dem_asset` of the processing config. */
  def defaultDemCatalogFactory: SarProcessingConfig => DemCatalog =
    config => ElevationModel.catalog(config.elevationModel, config.demAsset)

  def withGeoid(backend: TerrainCorrectionBackend,
                geoidTiffUri: URI,
                demCatalogFactory: SarProcessingConfig => DemCatalog = defaultDemCatalogFactory): TerrainCorrectionProcessor =
    new TerrainCorrectionProcessor(
      backend            = backend,
      geoidSourceFactory = Some(geoidFromTiff(geoidTiffUri)),
      demCatalogFactory  = demCatalogFactory
    )

  /** Read the DEM window for the output tile, reprojected to the target CRS at
   *  target cell size; converted from orthometric to ellipsoidal heights via the
   *  geoid undulation grid. Returns a (rows x cols) array of metres.
   *
   *  DEM items that cannot be read give NaN heights and are counted as a soft error, unless
   *  `config.softErrors` is false, in which case reading fails. */
  def readDemEllipsoidal(ctx: TileComputeContext): Array[Array[Float]] = {
    val req = ctx.request
    val targetRe = RasterExtent(req.extent, req.cellSize.width, req.cellSize.height, req.cols, req.rows)
    val dem: Tile = try {
      DemReader.read(ctx.demItems, req, targetRe)
    } catch {
      case e: Exception if req.config.softErrors =>
        logger.warn(s"sar_backscatter: DEM read failed for item ${req.sceneId}, AOI ${req.extent}, using NaN heights: ${e.getMessage}", e)
        countSoftError(req.cols.toLong * req.rows / (1024 * 1024))
        FloatArrayTile.fill(Float.NaN, req.cols, req.rows)
      case e: Exception =>
        throw new IllegalStateException(s"sar_backscatter: DEM read failed for item ${req.sceneId}, AOI ${req.extent}: ${e.getMessage}", e)
    }

    val geoid: Option[Tile] = ctx.geoidSource.map { gs =>
      gs.reproject(req.crs, TargetRegion(targetRe.toGridType[Long]), method = Bilinear)
        .read().get.tile.band(0)
    }

    // Float is precise enough for heights (~1 mm at 8000 m) and halves the memory
    val out = Array.ofDim[Float](req.rows, req.cols)
    var r = 0
    while (r < req.rows) {
      var c = 0
      while (c < req.cols) {
        val ortho = dem.getDouble(c, r)
        val und   = geoid.map(_.getDouble(c, r)).getOrElse(0.0)
        out(r)(c) = if (java.lang.Double.isNaN(ortho)) Float.NaN else (ortho + und).toFloat
        c += 1
      }
      r += 1
    }
    out
  }

  private def countSoftError(megapixels: Long): Unit =
    try {
      BatchJobMetadataTracker.tracker("").add(SOFT_ERROR_MEGAPIXEL_COUNTER, math.max(1L, megapixels))
    } catch {
      case e: Exception => logger.warn(s"sar_backscatter: failed to increment the soft error counter: ${e.getMessage}", e)
    }

  /** Map an output (col, row) to (lon, lat) in radians. */
  def pixelToLonLatRad(col: Int, row: Int, req: TileRequest): (Double, Double) = {
    val xMap = req.extent.xmin + (col + 0.5) * req.cellSize.width
    val yMap = req.extent.ymax - (row + 0.5) * req.cellSize.height
    val (lon, lat) = geotrellis.proj4.Transform(req.crs, LatLng)(xMap, yMap)
    (math.toRadians(lon), math.toRadians(lat))
  }
}



