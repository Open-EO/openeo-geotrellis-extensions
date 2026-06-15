package org.openeo.geotrellis.testutil.stac

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import ucar.ma2.{ArrayDouble, ArrayFloat, DataType}
import ucar.nc2.{Dimension, NetcdfFileWriter}

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.ZonedDateTime
import java.util

// ---------------------------------------------------------------------------
// Raster fill patterns – each produces a visually distinct gradient that
// makes it easy to verify correct spatial alignment after loading.
// ---------------------------------------------------------------------------

sealed trait RasterPattern
/** Value increases left-to-right (0 – 10 000). Verifies X-axis mapping. */
case object XGradient extends RasterPattern
/** Value increases top-to-bottom (0 – 10 000). Verifies Y-axis mapping. */
case object YGradient extends RasterPattern
/** Alternating square blocks of 0 and 10 000. Verifies tiling / block size. */
case object Checkerboard extends RasterPattern
/** Diagonal gradient ((col+row) / (cols+rows-2) * 10 000). Verifies both axes simultaneously. */
case object Diagonal extends RasterPattern

// ---------------------------------------------------------------------------
// Asset specs – describe a single file that should be written per item
// ---------------------------------------------------------------------------

sealed trait AssetSpec {
  def assetKey: String
  def fileName: String
  /** Stable, human-readable string used when computing the spec hash. */
  private[stac] def specString: String
}

/**
 * Specification for a (single- or multi-band) GeoTIFF asset.
 *
 * @param bandNames  One name per band; length determines band count.
 * @param resolution Pixel size in CRS units (metres for UTM projections).
 * @param crs        Coordinate reference system of the output file.
 * @param extent     Spatial extent in the given CRS.
 * @param pattern    Fill pattern for every band (band index is added as offset).
 */
case class GeoTiffAssetSpec(
  assetKey:  String,
  fileName:  String,
  bandNames: Seq[String],
  resolution: Double,
  crs:        CRS,
  extent:     Extent,
  pattern:    RasterPattern = XGradient,
  cellType:   CellType      = FloatConstantNoDataCellType,
) extends AssetSpec {
  private[stac] def specString: String =
    s"GeoTiff|$assetKey|$fileName|${bandNames.mkString(",")}|$resolution|${StacTestUtils.crsId(crs)}|$extent|$pattern|$cellType"
}

/**
 * Specification for a CF-compliant NetCDF asset (pure-Java NetCDF-3 format).
 *
 * @param variables  Variable names (one variable per logical band).
 * @param resolution Pixel size in CRS units (degrees for LatLng).
 * @param crs        Coordinate reference system (defaults to EPSG:4326 / LatLng).
 * @param extent     Spatial extent in the given CRS.
 * @param pattern    Fill pattern applied per variable (variable index as band offset).
 */
case class NetCDFAssetSpec(
  assetKey:   String,
  fileName:   String,
  variables:  Seq[String],
  resolution: Double,
  crs:        CRS   = LatLng,
  extent:     Extent,
  pattern:    RasterPattern = YGradient,
  cellType:   CellType      = FloatConstantNoDataCellType,
) extends AssetSpec {
  private[stac] override def specString: String =
    s"NetCDF|$assetKey|$fileName|${variables.mkString(",")}|$resolution|${StacTestUtils.crsId(crs)}|$extent|$pattern|$cellType"
}

// ---------------------------------------------------------------------------
// Item and collection specs
// ---------------------------------------------------------------------------

/**
 * Specification for a single STAC item.
 * The item directory inside the collection is named after the item id.
 */
case class TestItemSpec(id: String, datetime: String, assets: Seq[AssetSpec]) {
  private[stac] def specString: String =
    s"$id|$datetime|${assets.map(_.specString).mkString(";")}"
}

/** Specification for an entire STAC test collection. */
case class TestCollectionSpec(id: String, items: Seq[TestItemSpec]) {
  private[stac] def specString: String =
    s"$id|${items.map(_.specString).mkString("\n")}"
}

// ---------------------------------------------------------------------------
// GeneratedCollection – result handle with helper accessors
// ---------------------------------------------------------------------------

/**
 * A collection that has been (or is assumed to be) fully generated on disk.
 *
 * Provides path helpers and a converter to [[OpenSearchResponses.Feature]]
 * objects for direct use with [[org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient]].
 */
case class GeneratedCollection(spec: TestCollectionSpec, outputDir: Path) {

  def itemDir(itemId: String): Path = outputDir.resolve(itemId)

  def assetPath(itemId: String, fileName: String): Path =
    itemDir(itemId).resolve(fileName)

  def collectionJsonPath: Path = outputDir.resolve("collection.json")

  /**
   * Converts each [[TestItemSpec]] into an [[OpenSearchResponses.Feature]] that can be
   * registered with [[org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient]].
   *
   * The feature's `bbox` is the WGS-84 envelope of the primary GeoTIFF asset
   * (or the NetCDF asset when no GeoTIFF is present). `rasterExtent` and `crs`
   * are set to the native projection of the primary asset.
   *
   * Each asset file becomes one [[Link]] whose `bandNames` lists the band /
   * variable names declared in the corresponding [[AssetSpec]].
   */
  def toOpenSearchFeatures: Seq[Feature] =
    spec.items.map { item =>
      val primaryTiff = item.assets.collectFirst { case g: GeoTiffAssetSpec => g }
      val primaryNc   = item.assets.collectFirst { case n: NetCDFAssetSpec  => n }

      val links: Array[Link] = item.assets.flatMap {
        case g: GeoTiffAssetSpec =>
          val uri = assetPath(item.id, g.fileName).toUri
          Seq(Link(uri, title = Some(g.assetKey), bandNames = Some(g.bandNames)))
        case n: NetCDFAssetSpec =>
          val uri = assetPath(item.id, n.fileName).toUri
          Seq(Link(uri, title = Some(n.assetKey), bandNames = Some(n.variables)))
      }.toArray

      val (wgs84bbox, assetCrs, nativeExtent) = primaryTiff match {
        case Some(t) =>
          val reprojected = ProjectedExtent(t.extent, t.crs).reproject(LatLng)
          (reprojected, Some(t.crs), Some(t.extent))
        case None =>
          val n = primaryNc.get
          (n.extent, Some(n.crs), Some(n.extent))
      }

      Feature(
        id          = item.id,
        bbox        = wgs84bbox,
        nominalDate = ZonedDateTime.parse(item.datetime),
        links       = links,
        resolution  = primaryTiff.map(_.resolution).orElse(primaryNc.map(_.resolution)),
        crs         = assetCrs,
        rasterExtent = nativeExtent,
      )
    }
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/**
 * Generates (and caches) a STAC test collection on disk.
 *
 * Call [[StacTestGenerator.ensureGenerated]] at the start of a test (or in a
 * `@BeforeAll` method). The generator
 *
 *  1. Computes a SHA-256 hash of the [[TestCollectionSpec]].
 *  2. Reads `.collection_hash` in the output directory, if present.
 *  3. Skips all file I/O when the hashes match (already up to date).
 *  4. Otherwise deletes the stale directory and regenerates everything.
 *
 * This makes repeated test runs fast while guaranteeing consistency when the
 * spec changes.
 */
object StacTestGenerator {

  /** Default output root: `<java.io.tmpdir>/stac-test-data/<collectionId>`. */
  def defaultOutputDir(spec: TestCollectionSpec): Path =
    Paths.get(System.getProperty("java.io.tmpdir"), "stac-test-data", spec.id)

  /**
   * Ensures the collection described by `spec` is present and up to date in
   * `outputDir`, generating it if necessary.
   */
  def ensureGenerated(spec: TestCollectionSpec, outputDir: Path): GeneratedCollection = {
    val hash     = computeHash(spec)
    val hashFile = outputDir.resolve(".collection_hash")

    if (Files.exists(hashFile)) {
      val stored = new String(Files.readAllBytes(hashFile), "UTF-8").trim
      if (stored == hash)
        return GeneratedCollection(spec, outputDir)
    }

    if (Files.exists(outputDir)) deleteRecursively(outputDir)
    Files.createDirectories(outputDir)

    generateCollection(spec, outputDir)
    Files.write(hashFile, hash.getBytes("UTF-8"))
    GeneratedCollection(spec, outputDir)
  }

  /** Convenience overload that uses [[defaultOutputDir]]. */
  def ensureGenerated(spec: TestCollectionSpec): GeneratedCollection =
    ensureGenerated(spec, defaultOutputDir(spec))

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private def computeHash(spec: TestCollectionSpec): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(spec.specString.getBytes("UTF-8"))
    digest.digest().map("%02x".format(_)).mkString
  }

  private def generateCollection(spec: TestCollectionSpec, outputDir: Path): Unit = {
    for (item <- spec.items) {
      val itemDir = outputDir.resolve(item.id)
      Files.createDirectories(itemDir)
      item.assets.foreach {
        case tiff: GeoTiffAssetSpec => writeGeoTiff(itemDir, tiff)
        case nc:   NetCDFAssetSpec  => writeNetCDF(itemDir, nc)
      }
      writeItemJson(outputDir, item)
    }
    writeCollectionJson(outputDir, spec)
  }

  // -- GeoTIFF ---------------------------------------------------------------

  private def writeGeoTiff(itemDir: Path, spec: GeoTiffAssetSpec): Unit = {
    val re   = RasterExtent(spec.extent, CellSize(spec.resolution, spec.resolution))
    val cols = re.cols
    val rows = re.rows
    val out  = itemDir.resolve(spec.fileName).toString

    if (spec.bandNames.size == 1) {
      SinglebandGeoTiff(makeTile(cols, rows, 0, spec.pattern, spec.cellType), spec.extent, spec.crs)
        .write(out)
    } else {
      val bands = spec.bandNames.indices.map(i => makeTile(cols, rows, i, spec.pattern, spec.cellType))
      MultibandGeoTiff(ArrayMultibandTile(bands: _*), spec.extent, spec.crs)
        .write(out)
    }
  }

  /**
   * Creates a raster tile filled according to the requested pattern.
   *
   * The `bandIndex` is added as a 10 000-unit offset so every band in a
   * multi-band file has a distinct value range, making individual bands easy
   * to tell apart visually or in assertions.
   */
  private def makeTile(cols: Int, rows: Int, bandIndex: Int,
                       pattern: RasterPattern, cellType: CellType): MutableArrayTile = {
    val tile = ArrayTile.empty(cellType, cols, rows)
    for (row <- 0 until rows; col <- 0 until cols) {
      val base: Double = pattern match {
        case XGradient =>
          col.toDouble / math.max(1, cols - 1) * 10000.0
        case YGradient =>
          row.toDouble / math.max(1, rows - 1) * 10000.0
        case Checkerboard =>
          val blockSize = math.max(1, math.max(cols, rows) / 8)
          if (((col / blockSize) + (row / blockSize)) % 2 == 0) 0.0 else 10000.0
        case Diagonal =>
          (col + row).toDouble / math.max(1, cols + rows - 2) * 10000.0
      }
      tile.setDouble(col, row, base + bandIndex * 10000.0)
    }
    tile
  }

  // -- NetCDF ----------------------------------------------------------------

  /**
   * Writes a CF-compliant NetCDF-3 file (pure-Java, no native HDF5 required).
   * Each variable in `spec.variables` is written as a 2-D float array (y × x).
   * Coordinate variables `x` / `y` follow CF conventions with `degrees_east` /
   * `degrees_north` units for LatLng and metres for projected CRS.
   * A scalar `crs` variable carries the WKT string for GDAL compatibility.
   */
  private def writeNetCDF(itemDir: Path, spec: NetCDFAssetSpec): Unit = {
    val re   = RasterExtent(spec.extent, CellSize(spec.resolution, spec.resolution))
    val cols = re.cols
    val rows = re.rows
    val out  = itemDir.resolve(spec.fileName).toString

    val writer: NetcdfFileWriter =
      NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, out, null)

    writer.addGlobalAttribute("Conventions", "CF-1.9")
    writer.addGlobalAttribute("title", s"Test NetCDF: ${spec.assetKey}")

    val yDim = writer.addDimension("y", rows)
    val xDim = writer.addDimension("x", cols)

    val xDims = new util.ArrayList[Dimension](); xDims.add(xDim)
    val yDims = new util.ArrayList[Dimension](); yDims.add(yDim)
    val xyDims = new util.ArrayList[Dimension](); xyDims.add(yDim); xyDims.add(xDim)

    val isLatLng = spec.crs == LatLng ||
      spec.crs.epsgCode.contains(4326)

    if (isLatLng) {
      addCoordVar(writer, xDims, "x", "longitude",  "longitude",  "degrees_east",  "X")
      addCoordVar(writer, yDims, "y", "latitude",   "latitude",   "degrees_north", "Y")
    } else {
      addCoordVar(writer, xDims, "x", "projection_x_coordinate", "x coordinate of projection", "m", "X")
      addCoordVar(writer, yDims, "y", "projection_y_coordinate", "y coordinate of projection", "m", "Y")
    }

    writer.addVariable("crs", DataType.CHAR, "")
    spec.crs.toWKT().foreach { wkt =>
      writer.addVariableAttribute("crs", "crs_wkt",    wkt)
      writer.addVariableAttribute("crs", "spatial_ref", wkt)
    }

    for (varName <- spec.variables) {
      writer.addVariable(varName, DataType.FLOAT, xyDims)
      writer.addVariableAttribute(varName, "long_name",    varName)
      writer.addVariableAttribute(varName, "grid_mapping", "crs")
      writer.addVariableAttribute(varName, "_FillValue",   Float.NaN.asInstanceOf[Number])
    }

    writer.create()

    // Coordinate values – pixel-centre convention
    val xVals = (0 until cols)
      .map(c => re.extent.xmin + c * re.cellwidth  + re.cellwidth  / 2.0).toArray
    val yVals = (0 until rows)
      .map(r => re.extent.ymax - r * re.cellheight - re.cellheight / 2.0).toArray

    val xArr = new ArrayDouble.D1(cols)
    xVals.zipWithIndex.foreach { case (v, i) => xArr.set(i, v) }
    val yArr = new ArrayDouble.D1(rows)
    yVals.zipWithIndex.foreach { case (v, i) => yArr.set(i, v) }
    writer.write("x", xArr)
    writer.write("y", yArr)

    for ((varName, bandIdx) <- spec.variables.zipWithIndex) {
      val data = new ArrayFloat.D2(rows, cols)
      for (row <- 0 until rows; col <- 0 until cols) {
        val base: Float = {
        val patternValue = spec.pattern match {
          case XGradient   => col.toDouble / math.max(1, cols - 1) * 10000.0
          case YGradient   => row.toDouble / math.max(1, rows - 1) * 10000.0
          case Checkerboard =>
            val bs = math.max(1, math.max(cols, rows) / 8)
            if (((col / bs) + (row / bs)) % 2 == 0) 0.0 else 10000.0
          case Diagonal    =>
            (col + row).toDouble / math.max(1, cols + rows - 2) * 10000.0
        }
        (patternValue + bandIdx * 10000.0).toFloat
      }
        data.set(row, col, base)
      }
      writer.write(varName, data)
    }

    writer.close()
  }

  private def addCoordVar(writer: NetcdfFileWriter, dims: util.ArrayList[Dimension],
                          name: String, standardName: String, longName: String,
                          units: String, axis: String): Unit = {
    writer.addVariable(name, DataType.DOUBLE, dims)
    writer.addVariableAttribute(name, "standard_name", standardName)
    writer.addVariableAttribute(name, "long_name",     longName)
    writer.addVariableAttribute(name, "units",         units)
    writer.addVariableAttribute(name, "axis",          axis)
  }

  // -- STAC JSON -------------------------------------------------------------

  private def writeItemJson(collectionDir: Path, item: TestItemSpec): Unit = {
    val itemDir  = collectionDir.resolve(item.id)
    val wgs84Box = computeWgs84Box(item)

    val assetsJson = item.assets.map { spec =>
      val absPath  = itemDir.resolve(spec.fileName).toAbsolutePath.toUri.toString
      val mimeType = spec match {
        case _: GeoTiffAssetSpec => "image/tiff; application=geotiff"
        case _: NetCDFAssetSpec  => "application/x-netcdf"
      }
      val (assetCrs, assetExtent, assetResolution, bandNames) = spec match {
        case g: GeoTiffAssetSpec => (g.crs, g.extent, g.resolution, g.bandNames)
        case n: NetCDFAssetSpec  => (n.crs, n.extent, n.resolution, n.variables)
      }

      val re          = RasterExtent(assetExtent, CellSize(assetResolution, assetResolution))
      val epsgJson    = assetCrs.epsgCode.map(e => s""","proj:epsg":$e""").getOrElse("")
      val projShape   = s"""[${re.rows},${re.cols}]"""
      val projBbox    = s"""[${assetExtent.xmin},${assetExtent.ymin},${assetExtent.xmax},${assetExtent.ymax}]"""

      val bandsJson = bandNames.map(b => s"""{"name":"$b"}""").mkString(",")
      s""""${spec.assetKey}":{
         |      "href":"$absPath",
         |      "type":"$mimeType",
         |      "roles":["data"],
         |      "bands":[$bandsJson]$epsgJson,
         |      "proj:shape":$projShape,
         |      "proj:bbox":$projBbox
         |    }""".stripMargin
    }.mkString(",\n    ")

    val json =
      s"""{
         |  "type":"Feature",
         |  "stac_version":"1.0.0",
         |  "stac_extensions":[
         |    "https://stac-extensions.github.io/eo/v1.1.0/schema.json",
         |    "https://stac-extensions.github.io/projection/v1.1.0/schema.json"
         |  ],
         |  "id":"${item.id}",
         |  "geometry":{
         |    "type":"Polygon",
         |    "coordinates":[[[${wgs84Box.xmin},${wgs84Box.ymin}],[${wgs84Box.xmax},${wgs84Box.ymin}],[${wgs84Box.xmax},${wgs84Box.ymax}],[${wgs84Box.xmin},${wgs84Box.ymax}],[${wgs84Box.xmin},${wgs84Box.ymin}]]]
         |  },
         |  "bbox":[${wgs84Box.xmin},${wgs84Box.ymin},${wgs84Box.xmax},${wgs84Box.ymax}],
         |  "properties":{"datetime":"${item.datetime}"},
         |  "links":[
         |    {"rel":"self","href":"./item.json","type":"application/json"},
         |    {"rel":"collection","href":"../collection.json","type":"application/json"}
         |  ],
         |  "assets":{
         |    $assetsJson
         |  }
         |}""".stripMargin

    Files.write(itemDir.resolve("item.json"), json.getBytes("UTF-8"))
  }

  private def writeCollectionJson(outputDir: Path, spec: TestCollectionSpec): Unit = {
    val itemLinks = spec.items.map { item =>
      s"""{"rel":"item","href":"./${item.id}/item.json","type":"application/geo+json"}"""
    }.mkString(",\n    ")

    val json =
      s"""{
         |  "type":"Collection",
         |  "stac_version":"1.0.0",
         |  "id":"${spec.id}",
         |  "description":"Auto-generated test collection",
         |  "license":"proprietary",
         |  "links":[
         |    $itemLinks
         |  ]
         |}""".stripMargin

    Files.write(outputDir.resolve("collection.json"), json.getBytes("UTF-8"))
  }

  private def computeWgs84Box(item: TestItemSpec): Extent = {
    val boxes: Seq[Extent] = item.assets.map {
      case g: GeoTiffAssetSpec =>
        if (g.crs == LatLng || g.crs.epsgCode.contains(4326)) g.extent
        else ProjectedExtent(g.extent, g.crs).reproject(LatLng)
      case n: NetCDFAssetSpec =>
        if (n.crs == LatLng || n.crs.epsgCode.contains(4326)) n.extent
        else ProjectedExtent(n.extent, n.crs).reproject(LatLng)
    }
    boxes.reduce(_ combine _)
  }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path))
      Files.list(path).forEach(deleteRecursively)
    Files.delete(path)
  }
}

private[stac] object StacTestUtils {
  def crsId(crs: CRS): String =
    crs.epsgCode.map("EPSG:" + _).getOrElse(crs.toProj4String)
}
