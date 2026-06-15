package org.openeo.sar

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.vector.Extent
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.metadata.{Polarisation, S1AnnotationParser, S1GrdMetadata}
import org.openeo.sar.stac.{StacAssets, StacItemLoader}

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
  demSourceFactory: Extent => RasterSource,
  geoidSourceFactory: Option[Extent => RasterSource] = None,
  rasterSourceFactory: URI => RasterSource = TerrainCorrectionProcessor.defaultRasterSourceFactory
) {

  // -------------------------------------------------------------------------
  // Stage 1: scene-level initialisation
  // -------------------------------------------------------------------------

  /** Parse a STAC item and open all required RasterSources.  Expensive; call
   *  once per scene, then reuse the returned [[SceneContext]] for all tiles. */
  def openScene(stacItemUrl: URI,
                cellSize: CellSize,
                crs: CRS,
                polarisations: Seq[Polarisation]): SceneContext = {

    val assets: StacAssets = StacItemLoader.load(stacItemUrl)

    val perPol = polarisations.map { pol =>
      val a = assets.perPol.getOrElse(pol.code,
        throw new IllegalArgumentException(s"polarisation ${pol.code} not present in STAC item"))
      S1AnnotationParser.parseProductAnnotation(
        a.productAnnotation, a.measurement, a.calibration, a.noise, pol)
    }
    val meta: S1GrdMetadata = S1AnnotationParser.assemble(perPol)

    val sarSources: Map[Polarisation, RasterSource] = polarisations.map { pol =>
      pol -> rasterSourceFactory(assets.perPol(pol.code).measurement)
    }.toMap

    // Use the scene bounding box from the STAC item to pre-open a DEM source
    // that covers the whole scene.  Per-tile reads will window into it cheaply.
    val (lonMin, latMin, lonMax, latMax) = assets.bboxWgs84
    val sceneBboxWgs84 = Extent(lonMin, latMin, lonMax, latMax).buffer(0.05)
    val demSource   = demSourceFactory(sceneBboxWgs84)
    val geoidSource = geoidSourceFactory.map(_(sceneBboxWgs84))

    SceneContext(meta, sarSources, demSource, geoidSource, cellSize, crs, polarisations)
  }

  // -------------------------------------------------------------------------
  // Stage 2: per-tile compute
  // -------------------------------------------------------------------------

  /** Compute an output [[Raster]] for every extent in `extents`.
   *
   *  `bands` selects which output bands to include (0-based):
   *    0 .. nPols-1 → sigma0 per polarisation
   *    nPols        → local incidence angle (degrees)
   *    nPols+1      → validity mask
   *
   *  This matches the [[RasterSource.readExtents]] contract so that a
   *  [[TerrainCorrectionRasterSource]] wrapper can delegate here without any
   *  additional logic.
   *
   *  Only per-tile windowed I/O and geometry happen inside this iterator;
   *  the scene-level [[SceneContext]] is closed over by reference. */
  def readExtents(scene: SceneContext,
                  extents: Traversable[Extent],
                  bands: Seq[Int]): Iterator[Raster[MultibandTile]] = {
    val totalBands = scene.polarisations.size + 2
    val effectiveBands = if (bands.isEmpty) (0 until totalBands) else bands
    extents.toIterator.map { extent =>
      val ctx  = scene.tileContext(extent)
      val full = backend.compute(ctx)           // always compute all bands; band selection is cheap
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
    val scene = openScene(stacItemUrl, request.cellSize, request.crs, request.polarisations)
    readExtents(scene, List(request.extent)).next().tile
  }
}

object TerrainCorrectionProcessor {
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

  def withDemAndGeoid(backend: TerrainCorrectionBackend,
                      demFactory: Extent => RasterSource,
                      geoidTiffUri: URI): TerrainCorrectionProcessor =
    new TerrainCorrectionProcessor(
      backend            = backend,
      demSourceFactory   = demFactory,
      geoidSourceFactory = Some(geoidFromTiff(geoidTiffUri))
    )

  /** Read the DEM window for the output tile, reprojected to the target CRS at
   *  target cell size; converted from orthometric to ellipsoidal heights via the
   *  optional geoid undulation grid. Returns a (rows x cols) array of metres. */
  def readDemEllipsoidal(ctx: TileComputeContext): Array[Array[Double]] = {
    val req = ctx.request
    val targetRe = RasterExtent(req.extent, req.cellSize.width, req.cellSize.height, req.cols, req.rows)
    val defaultTile = new Raster( MultibandTile(FloatConstantTile(10.0f, req.cols, req.rows)), req.extent)
    val dem = ctx.demSource
      .reproject(req.crs, method = Bilinear)
      .resampleToGrid(targetRe.toGridType[Long], Bilinear)
      .read(req.extent).getOrElse(
        defaultTile)
        //throw new IllegalStateException(s"DEM read returned no raster for AOI ${ctx.demSource.name}"))
      .tile.band(0)

    val geoid: Option[Tile] = ctx.geoidSource.map { gs =>
      gs.reproject(req.crs, TargetRegion(targetRe.toGridType[Long]), method = Bilinear)
        .read().get.tile.band(0)
    }

    val out = Array.ofDim[Double](req.rows, req.cols)
    var r = 0
    while (r < req.rows) {
      var c = 0
      while (c < req.cols) {
        val ortho = dem.getDouble(c, r)
        val und   = geoid.map(_.getDouble(c, r)).getOrElse(0.0)
        out(r)(c) = if (java.lang.Double.isNaN(ortho)) Double.NaN else ortho + und
        c += 1
      }
      r += 1
    }
    out
  }

  /** Map an output (col, row) to (lon, lat) in radians. */
  def pixelToLonLatRad(col: Int, row: Int, req: TileRequest): (Double, Double) = {
    val xMap = req.extent.xmin + (col + 0.5) * req.cellSize.width
    val yMap = req.extent.ymax - (row + 0.5) * req.cellSize.height
    val (lon, lat) = geotrellis.proj4.Transform(req.crs, LatLng)(xMap, yMap)
    (math.toRadians(lon), math.toRadians(lat))
  }
}



