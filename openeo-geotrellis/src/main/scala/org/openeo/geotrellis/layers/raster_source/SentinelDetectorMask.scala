package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.Base
import geotrellis.raster.{GridBounds, GridExtent, RasterSource, isNoData}
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.{DetectorMaskFormat, DetectorMaskRef, TileMetadata}

/**
 * Detector labels from Sentinel-2 `MSK_DETFOO` masks
 * (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/916).
 *
 * From processing baseline 04.00, a band's mask is a single-band JP2 at the band's native resolution, with label 0
 * for no detector and 1 to 12 for the detector ID. Labels are only ever read at that native resolution: the JP2
 * reduced-resolution levels can contain labels that do not exist in the native data.
 */
object SentinelDetectorMask {

  /** Native resolution in metres of each band, by 0-based bandId (B01 = 0, B08 = 7, B8A = 8, B12 = 12). */
  val bandResolutions: Map[Int, Int] =
    Map(0 -> 60, 1 -> 10, 2 -> 10, 3 -> 10, 4 -> 20, 5 -> 20, 6 -> 20, 7 -> 10, 8 -> 20, 9 -> 60, 10 -> 60, 11 -> 20, 12 -> 20)

  val maxDetectorId = 12

  private val TileMetadataLocation = """(.*\.SAFE)/GRANULE/([^/]+)/MTD_TL\.xml""".r

  private def invalid(message: String): Nothing = throw new IllegalArgumentException(message)

  /**
   * The mask's location next to the tile metadata: the SAFE root of `xmlPath` joined with the mask's
   * `MASK_FILENAME`, which must lie in the same granule directory.
   */
  def maskPath(xmlPath: String, mask: DetectorMaskRef): String = xmlPath match {
    case TileMetadataLocation(safeRoot, granule) =>
      if (mask.path.split('/').contains("..") || !mask.path.startsWith(s"GRANULE/$granule/"))
        invalid(s"detector mask '${mask.path}' is not in the granule directory GRANULE/$granule of $xmlPath")
      s"$safeRoot/${mask.path}"
    case _ => invalid(s"unsupported tile metadata location $xmlPath, expected <product>.SAFE/GRANULE/<granule>/MTD_TL.xml")
  }

  /**
   * The path for GDAL to open, following the conventions for the band JP2s: `/eodata/` and `s3://eodata/` are read
   * from `/vsis3/eodata/` if AWS_DIRECT=TRUE, as the driver does, and from the `/eodata/` mount otherwise;
   * `/vsis3/EODATA/` is spelled `/vsis3/eodata/`, as in [[org.openeo.geotrellis.layers.provider.JPEGRasterSourceProvider]];
   * HTTPS goes through `/vsicurl/`. Local absolute paths are used as they are.
   */
  def gdalPath(path: String, awsDirect: Boolean = sys.env.get("AWS_DIRECT").contains("TRUE")): String =
    if (path.startsWith("/vsis3/") || path.startsWith("/vsicurl/")) path.replace("/vsis3/EODATA/", "/vsis3/eodata/")
    else if (path.startsWith("s3://eodata/")) gdalPath("/eodata/" + path.stripPrefix("s3://eodata/"), awsDirect)
    else if (path.startsWith("s3://")) "/vsis3/" + path.stripPrefix("s3://")
    else if (path.startsWith("/eodata/")) if (awsDirect) "/vsis3" + path else path
    else if (path.startsWith("https://")) "/vsicurl/" + path
    else if (path.startsWith("/")) path
    else invalid(s"unsupported detector mask location $path")

  /** Opens a mask with GDAL. Reduced-resolution levels are switched off, and callers only read native windows. */
  def openMask(gdalPath: String): RasterSource =
    GDALRasterSource(GDALPath(gdalPath), GDALWarpOptions(ovr = Some(Base)))

  /** The native pixel column of x: points on a pixel edge belong to the pixel east of it. */
  def nativeCol(grid: GridExtent[Long], x: Double): Long = math.floor((x - grid.extent.xmin) / grid.cellwidth).toLong

  /** The native pixel row of y: points on a pixel edge belong to the pixel south of it. */
  def nativeRow(grid: GridExtent[Long], y: Double): Long = math.floor((grid.extent.ymax - y) / grid.cellheight).toLong

  private def insideGrid(grid: GridExtent[Long], col: Long, row: Long): Boolean =
    col >= 0 && row >= 0 && col < grid.cols && row < grid.rows

  /** The smallest window of `grid` that holds the native pixels of all points inside it; None if there are none. */
  def nativeWindow(grid: GridExtent[Long], xs: Array[Double], ys: Array[Double]): Option[GridBounds[Long]] = {
    require(xs.length == ys.length, "xs and ys differ in length")
    var (colMin, rowMin, colMax, rowMax) = (Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue)
    for (k <- xs.indices) {
      val col = nativeCol(grid, xs(k))
      val row = nativeRow(grid, ys(k))
      if (insideGrid(grid, col, row)) {
        colMin = math.min(colMin, col)
        rowMin = math.min(rowMin, row)
        colMax = math.max(colMax, col)
        rowMax = math.max(rowMax, row)
      }
    }
    if (colMin > colMax) None else Some(GridBounds[Long](colMin, rowMin, colMax, rowMax))
  }

  private def sameGrid(a: GridExtent[Long], b: GridExtent[Long]): Boolean =
    a.cols == b.cols && a.rows == b.rows && a.extent.equalsExact(b.extent, 1e-6) &&
      math.abs(a.cellwidth - b.cellwidth) <= 1e-9 && math.abs(a.cellheight - b.cellheight) <= 1e-9

  /**
   * Detector labels at points (xs, ys) in the tile CRS, read from `mask` at its native resolution `grid`. Only the
   * window holding the points is read. Points outside the grid get label 0.
   * Throws IllegalArgumentException if the mask is not a single band on `grid` in `crs`, or if a label at one of
   * the points is not 0 to 12.
   */
  def labelsAt(mask: RasterSource, grid: GridExtent[Long], crs: CRS, xs: Array[Double], ys: Array[Double]): Array[Int] = {
    if (mask.crs != crs) invalid(s"detector mask CRS ${mask.crs} differs from the tile CRS $crs")
    if (!sameGrid(mask.gridExtent, grid)) invalid(s"detector mask grid ${mask.gridExtent} differs from the tile grid $grid")
    if (mask.bandCount != 1) invalid(s"detector mask has ${mask.bandCount} bands, expected 1")

    val labels = new Array[Int](xs.length)
    for (window <- nativeWindow(grid, xs, ys)) {
      val tile = mask.read(window, Seq(0)).getOrElse(invalid(s"detector mask window $window could not be read")).tile.band(0)
      if (tile.cols != window.width || tile.rows != window.height)
        invalid(s"detector mask window $window was read as ${tile.cols} x ${tile.rows} pixels")
      for (k <- xs.indices) {
        val col = nativeCol(grid, xs(k))
        val row = nativeRow(grid, ys(k))
        if (insideGrid(grid, col, row)) {
          val value = tile.get((col - window.colMin).toInt, (row - window.rowMin).toInt)
          val label = if (isNoData(value)) 0 else value
          if (label < 0 || label > maxDetectorId) invalid(s"detector mask has label $label, expected 0 to $maxDetectorId")
          labels(k) = label
        }
      }
    }
    labels
  }

  /**
   * Detector labels of band `bandId` at points (xs, ys) in the tile CRS, from its JP2 mask next to the tile metadata
   * at `xmlPath`, opened with `open` (GDAL by default) from its [[gdalPath]].
   */
  def detectorLabels(xmlPath: String, metadata: TileMetadata, bandId: Int, mask: DetectorMaskRef,
                     xs: Array[Double], ys: Array[Double], open: String => RasterSource = openMask): Array[Int] = {
    if (mask.format != DetectorMaskFormat.Jp2) invalid(s"unsupported detector mask format ${mask.format}: ${mask.path}")
    val resolution = bandResolutions.getOrElse(bandId, invalid(s"unknown bandId $bandId"))
    labelsAt(open(gdalPath(maskPath(xmlPath, mask))), metadata.gridExtents(resolution), metadata.crs, xs, ys)
  }
}
