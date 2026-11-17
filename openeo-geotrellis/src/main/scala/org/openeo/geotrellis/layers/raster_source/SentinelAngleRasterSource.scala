package org.openeo.geotrellis.layers.raster_source

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, CellType, FloatArrayTile, FloatConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields.{Angle, FilledViewGrid, fillViewGrid, pixelCentres, sunField, viewField}
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.{TileMetadata, loadTileMetadata}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap

/** One per-pixel angle output: the sun angles, or the viewing angles of a spectral band (0-based bandId). */
final case class AngleOutput(angle: Angle, viewBandId: Option[Int]) {
  require(viewBandId.forall(AngleOutput.bandNames.indices.contains), s"unknown bandId $viewBandId")

  /** For example "sunZenith" or "viewAzimuth_B05". */
  def id: String = {
    val angleName = angle match {
      case Angle.Zenith => "Zenith"
      case Angle.Azimuth => "Azimuth"
    }
    viewBandId.fold(s"sun$angleName")(bandId => s"view${angleName}_${AngleOutput.bandNames(bandId)}")
  }
}

object AngleOutput {
  val bandNames: IndexedSeq[String] =
    IndexedSeq("B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B10", "B11", "B12")
}

object SentinelAngleRasterSource {
  private val logger = LoggerFactory.getLogger(classOf[SentinelAngleRasterSource])

  /** Bound temporary coordinate, label and interpolation arrays, independently of the requested output size. */
  private val evaluationTileSize = 256L

  /** Per-pixel viewing angles are those of B05 (decision D2 of issue #916). */
  val viewReferenceBandId = 4

  /** A parsed tile with its filled viewing grids, shared by all outputs of the tile within one JVM. */
  final class TileAngles(val xmlPath: String, val metadata: TileMetadata) {
    /** Filled viewing grids by bandId and detectorId. Detectors whose grid cannot be filled are absent. */
    val filledViewGrids: Map[Int, Map[Int, FilledViewGrid]] = metadata.viewing.collect {
      case (bandId, Right(viewing)) =>
        bandId -> viewing.grids.flatMap { case (detectorId, grid) => fillViewGrid(grid).map(detectorId -> _) }
    }

    private val warnings = ConcurrentHashMap.newKeySet[String]()

    /** Logs `message` the first time `key` is used for this tile in this JVM. */
    def warnOnce(key: String, message: => String): Unit = if (warnings.add(key)) logger.warn(message)
  }

  private val cache: Cache[String, TileAngles] = Caffeine.newBuilder().maximumSize(64).build[String, TileAngles]()

  /**
   * The parsed tile metadata at `xmlPath`, loaded atomically per path into a bounded per-JVM cache.
   * Concurrent cold requests share the successful load. Throws as [[loadTileMetadata]] does; failures are not cached.
   */
  def tileAngles(xmlPath: String): TileAngles =
    tileAngles(xmlPath, path => loadTileMetadata(path, Set(viewReferenceBandId)))

  /** Loader injection for cache-concurrency and failure tests, without changing the JVM-wide cache. */
  private[layers] def tileAngles(xmlPath: String, load: String => TileMetadata): TileAngles =
    cache.get(xmlPath, path => new TileAngles(path, load(path)))
}

/**
 * Per-pixel Sentinel-2 sun or viewing angles of one tile, as float32 degrees with NaN as NoData
 * (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/916). Values are calculated at the pixel centres
 * of `gridExtent` in `crs`, so reprojecting or resampling only changes the grid.
 *
 * Equality depends on (xmlPath, output, crs, gridExtent) only. `openMask` opens detector masks; it is GDAL by
 * default and only replaced in tests.
 */
case class SentinelAngleRasterSource(xmlPath: String, output: AngleOutput, crs: CRS, gridExtent: GridExtent[Long])
                                    (val openMask: String => RasterSource = SentinelDetectorMask.openMask)
  extends RasterSource {

  import SentinelAngleRasterSource._

  @transient private lazy val tile: TileAngles = tileAngles(xmlPath)

  val targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = this

  override def name: SourceName = OpenEoSourcePath(s"$xmlPath#${output.id}")

  override def bandCount: Int = 1

  override def cellType: CellType = FloatConstantNoDataCellType

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty

  override def attributesForBand(band: Int): Map[String, String] = Map.empty

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod,
                                      strategy: OverviewStrategy): RasterSource =
    copy(crs = targetCRS, gridExtent = resampleTarget(gridExtent.reproject(crs, targetCRS)))(openMask)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    copy(gridExtent = resampleTarget(gridExtent))(openMask)

  /** Angles are always float32; like the constant angle bands, other cell types are ignored. */
  override def convert(targetCellType: TargetCellType): RasterSource = {
    if (targetCellType.cellType != cellType) logger.warn(s"ignoring conversion of $name to ${targetCellType.cellType}")
    this
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    extent.intersection(gridExtent.extent).flatMap(overlap => read(gridExtent.gridBoundsFor(overlap, clamp = true), bands))

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    require(bands == Seq(0), s"$name has a single band, but bands $bands were requested")
    bounds.intersection(gridExtent.dimensions).map { window =>
      val (width, height) = (window.width, window.height)
      require(width <= Int.MaxValue && height <= Int.MaxValue / width, s"window too large: $width x $height")
      val values =
        if (width <= evaluationTileSize && height <= evaluationTileSize) readWindow(window, openMask)
        else {
          // Only the returned float array scales with the whole window, including per-product union reads.
          val result = new Array[Float]((width * height).toInt)
          var maskSource: RasterSource = null
          val maskForRead: String => RasterSource = path => {
            if (maskSource == null) maskSource = openMask(path)
            maskSource
          }
          window.split(evaluationTileSize, evaluationTileSize).foreach { chunk =>
            val chunkValues = readWindow(chunk, maskForRead)
            val chunkWidth = chunk.width.toInt
            var row = 0
            while (row < chunk.height.toInt) {
              val offset = ((chunk.rowMin - window.rowMin + row) * width + chunk.colMin - window.colMin).toInt
              System.arraycopy(chunkValues, row * chunkWidth, result, offset, chunkWidth)
              row += 1
            }
          }
          result
        }
      Raster(MultibandTile(FloatArrayTile(values, width.toInt, height.toInt)), gridExtent.extentFor(window))
    }
  }

  private def readWindow(window: GridBounds[Long], maskOpener: String => RasterSource): Array[Float] = {
    val (xs, ys) = pixelCentres(gridExtent, window)
    toTileCrs(xs, ys)
    output.viewBandId match {
      case None => sunField(usable(tile.metadata.sunGrid), output.angle, tile.metadata.extent, xs, ys)
      case Some(bandId) => viewValues(bandId, xs, ys, maskOpener)
    }
  }

  private def usable[T](part: Either[String, T]): T =
    part.fold(message => throw new IllegalArgumentException(s"invalid Sentinel-2 tile metadata $xmlPath: $message"), identity)

  private def toTileCrs(xs: Array[Double], ys: Array[Double]): Unit =
    if (crs != tile.metadata.crs) {
      val transform = Transform(crs, tile.metadata.crs)
      for (k <- xs.indices) {
        val (x, y) = transform(xs(k), ys(k))
        xs(k) = x
        ys(k) = y
      }
    }

  private def viewValues(bandId: Int, xs: Array[Double], ys: Array[Double], maskOpener: String => RasterSource): Array[Float] = {
    val viewing = usable(tile.metadata.viewing.getOrElse(bandId, Left(s"bandId $bandId was not parsed")))
    val labels = SentinelDetectorMask.detectorLabels(xmlPath, tile.metadata, bandId, viewing.detectorMask, xs, ys, maskOpener)
    val grids = tile.filledViewGrids.getOrElse(bandId, Map.empty)
    for (label <- labels.distinct if label > 0 && !grids.contains(label))
      tile.warnOnce(s"no grid $bandId/$label",
        s"$xmlPath: detector $label of ${AngleOutput.bandNames(bandId)} has no usable viewing grid; its pixels are NoData")
    viewField(grids, output.angle, tile.metadata.extent, xs, ys, labels)
  }
}
