package org.openeo.geotrellishealpix

import healpix.{HealpixBase, Pointing, Scheme}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.LoggerFactory
import ucar.nc2.dataset.NetcdfDatasets

import java.net.URI
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util

/**
 * Reads Sentinel-3 (or any swath / scanline) products stored as NetCDF files
 * with per-pixel latitude/longitude arrays and bins the measurements into a
 * HEALPix grid, producing a [[HealpixDatacube]].
 *
 * This is the Scala equivalent of the Python `binning.py` from
 * `openeo-geopyspark-driver`. The algorithm for each product file:
 *
 *  1. Open the NetCDF and read the `latitude` and `longitude` variables
 *     (configurable names).
 *  2. Read the requested measurement/band variables.
 *  3. For each valid pixel convert `(lon, lat)` → HEALPix NESTED cell id
 *     via `HealpixBase.ang2pix`.
 *  4. Emit `(cell_id, timestamp, band_values…)` rows into a Spark DataFrame.
 *
 * Aggregation (multiple pixels falling into the same HEALPix cell) is handled
 * by [[HealpixBinner]].
 *
 * === Typical Sentinel-3 products ===
 *
 *  - '''OLCI L1 / L2''' – radiance / reflectance in `instrument_data.nc`,
 *    geo-coordinates in `geo_coordinates.nc` (`latitude`, `longitude`).
 *  - '''SLSTR L1 / L2''' – similar layout.
 *  - '''SYN VG1''' – already gridded (0.00893° plate-carrée), can be read
 *    directly with GDAL. This reader is mainly useful for non-gridded
 *    products.
 *
 * The reader intentionally supports arbitrary NetCDF files as long as
 * they provide lat, lon, and band variables of matching shape.
 *
 * === Object storage support ===
 *
 * NetCDF files on S3-compatible object storage can be referenced using the
 * `cdms3://` URI scheme (e.g. `cdms3://bucket/key`). This uses the
 * `ucar.unidata.io.s3.CdmS3Uri` support in the netcdf-java library.
 * Ensure that the `cdm-s3` module is on the classpath and AWS credentials
 * are configured (environment, instance profile, or `~/.aws/credentials`).
 */
object Sentinel3BinningReader {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Configuration describing how to read one Sentinel-3 product.
   *
   * @param latVariable  name of the latitude variable (default `"latitude"`)
   * @param lonVariable  name of the longitude variable (default `"longitude"`)
   * @param bandVariables names of the band/measurement variables to extract
   * @param scaleFactor  optional per-band scale factor; if `None` the raw
   *                     values are used (some S-3 products store `short`
   *                     with a scale/offset)
   * @param fillValue    value treated as NoData / fill (default NaN)
   * @param geoFileSuffix when the lat/lon lives in a separate file inside
   *                      a `.SEN3` directory, the suffix appended to the
   *                      product root to find the geo-coordinates file
   *                      (e.g. `"geo_coordinates.nc"`). If `None`, lat/lon
   *                      are expected in the same file as the bands.
   * @param s3Endpoint   S3 endpoint for `cdms3://` URI construction when
   *                     reading from object storage (default: CDSE EODATA).
   */
  case class ProductConfig(
    latVariable: String = "latitude",
    lonVariable: String = "longitude",
    assetVariables: scala.collection.Map[String, Seq[String]],
    fillValue: Double = Double.NaN,
    geoFileSuffix: Option[String] = None,
    s3Endpoint: String = "eodata.dataspace.copernicus.eu"
  ) {
    lazy val allVariables: Seq[String] = assetVariables.values.flatten.toSeq
  }

  /**
   * Describes a single input product to be binned.
   *
   * @param path      path to the NetCDF file (or directory for `.SEN3`).
   *                  Supports local filesystem paths and `cdms3://` URIs
   *                  for S3-hosted products.
   * @param timestamp observation time for this product
   */
  case class ProductRef(path: String, timestamp: Timestamp)

  // ---- PyramidFactory-style public API --------------------------------------

  /**
   * Load a HEALPix datacube from an OpenSearch catalogue, following the same
   * interface pattern as [[org.openeo.geotrellis.file.PyramidFactory#datacube_seq]].
   *
   * Product discovery is performed via the provided [[OpenSearchClient]],
   * which queries a STAC / CreoDIAS / OSCARS catalogue for matching features.
   * Each feature's data links are resolved to NetCDF file paths which are then
   * read and binned on the executors.
   *
   * @param openSearchClient     catalogue client
   * @param openSearchCollectionId collection identifier in the catalogue
   * @param polygons             spatial filter (bounding polygon + CRS)
   * @param from_date            start of temporal range (ISO 8601)
   * @param to_date              end of temporal range (ISO 8601)
   * @param metadata_properties  additional metadata filters
   * @param correlationId        tracking ID for the request
   * @param dataCubeParameters   datacube parameters (unused fields are ignored)
   * @param nside                HEALPix NSIDE for the output grid
   * @param config               product reading configuration
   * @param aggregation          aggregation strategy (default: mean)
   * @return a HealpixDatacube binned from all matched products
   */
  def datacube_seq(openSearchClient: OpenSearchClient,
                   openSearchCollectionId: String,
                   polygons: ProjectedPolygons,
                   from_date: String,
                   to_date: String,
                   metadata_properties: util.Map[String, Any],
                   correlationId: String,
                   dataCubeParameters: DataCubeParameters,
                   nside: Int,
                   config: ProductConfig,
                   aggregation: HealpixBinner.Aggregation = HealpixBinner.Aggregation.Mean
                  ): ScalarHealpixDatacube = {

    val spark = SparkSession.active

    // 1. Discover products via OpenSearch
    val products = discoverProducts(
      openSearchClient, openSearchCollectionId,
      polygons, from_date, to_date,
      metadata_properties, correlationId, config)

    logger.info(s"Sentinel3BinningReader.datacube_seq: discovered ${products.size} products " +
      s"for collection=$openSearchCollectionId, date range [$from_date, $to_date]")

    if (products.isEmpty) {
      // Return empty datacube
      return ScalarHealpixDatacube.empty(spark, nside, config.allVariables.map(_ -> (DoubleType: DataType)))
    }

    // 2. Read and bin on executors
    loadCollection(spark, products, nside, config, aggregation)
  }

  // ---- Product discovery via OpenSearch --------------------------------------

  /**
   * Discover Sentinel-3 products via OpenSearch and convert them to
   * [[ProductRef]] instances.
   */
  def discoverProducts(openSearchClient: OpenSearchClient,
                       openSearchCollectionId: String,
                       polygons: ProjectedPolygons,
                       from_date: String,
                       to_date: String,
                       metadata_properties: util.Map[String, Any],
                       correlationId: String,
                       config: ProductConfig): Seq[ProductRef] = {

    import scala.jdk.CollectionConverters._

    val from = ZonedDateTime.parse(from_date)
    val to   = ZonedDateTime.parse(to_date)


    val features: Seq[Feature] = openSearchClient.getProducts(
      openSearchCollectionId,
      Some((from, to)),
      polygons.extent,
      metadata_properties.asScala.toMap,
      correlationId = correlationId,
      processingLevel = ""
    )

    features.flatMap(featureToProductRef(_, config))
  }

  /**
   * Convert an OpenSearch Feature to ProductRef(s).
   * Extracts the data link href and the nominal date from the feature.
   * The link title is matched against the configured band variables to find
   * the correct NetCDF file.
   */
  private def featureToProductRef(feature: Feature,
                                  config: ProductConfig): Option[ProductRef] = {
    // Find the first link whose title matches one of the band variables,
    // or fall back to the first available data link.
    val link = feature.links.find { l =>
      l.title.exists(t => config.assetVariables.contains(t))
    }.orElse(feature.links.headOption)

    link.map { l =>
      val path = l.href.getScheme match {
        case "file" => l.href.getPath
        case _      => l.href.toString  // cdms3://, https://, etc.
      }
      val ts = Timestamp.from(feature.nominalDate.toInstant)
      ProductRef(path, ts)
    }
  }

  // ---- public API (direct product list) -------------------------------------

  /**
   * Read one or more Sentinel-3 products and bin them into a
   * [[ScalarHealpixDatacube]].
   *
   * Reading is performed on Spark executors (one task per product) so that
   * products distributed across a cluster (e.g. on HDFS or object storage)
   * are read in parallel.
   *
   * The returned datacube contains '''raw''' (un-aggregated) rows. If
   * multiple pixels map to the same HEALPix cell (which is common for
   * higher-resolution source products) you typically want to follow up with
   * [[HealpixBinner.aggregate]].
   *
   * @param spark     active SparkSession
   * @param products  list of (path, timestamp) references
   * @param nside     HEALPix NSIDE (power of two)
   * @param config    column mapping / scaling configuration
   * @return datacube with one row per valid source pixel
   */
  def readRaw(spark: SparkSession,
              products: Seq[ProductRef],
              nside: Int,
              config: ProductConfig): ScalarHealpixDatacube = {


    val bandDefs: Seq[(String, DataType)] = config.allVariables.map(_ -> DoubleType)
    val schema = HealpixSchema.scalarSchema(bandDefs)

    // Distribute product reading across executors: one partition per product.
    // Each executor reads its NetCDF file(s) independently.
    val productRdd = spark.sparkContext.parallelize(products, math.max(products.size, 1))

    // Broadcast config and nside to executors
    val configBC = spark.sparkContext.broadcast(config)
    val nsideBC  = spark.sparkContext.broadcast(nside)

    val rowsRdd = productRdd.flatMap { ref =>
      readProduct(ref, nsideBC.value, configBC.value)
    }

    val df = spark.createDataFrame(rowsRdd, schema)

    logger.info(s"Sentinel3BinningReader: distributed read of ${products.size} " +
      s"products into NSIDE=$nside grid.")

    ScalarHealpixDatacube(nside, bandDefs, df)
  }

  /**
   * Convenience entry point: read + aggregate in one call.
   *
   * @param spark      active SparkSession
   * @param products   list of (path, timestamp) references
   * @param nside      HEALPix NSIDE
   * @param config     product config
   * @param aggregation aggregation strategy (default: mean)
   * @return aggregated HealpixDatacube
   */
  def loadCollection(spark: SparkSession,
                     products: Seq[ProductRef],
                     nside: Int,
                     config: ProductConfig,
                     aggregation: HealpixBinner.Aggregation =
                       HealpixBinner.Aggregation.Mean): ScalarHealpixDatacube = {
    val raw = readRaw(spark, products, nside, config)
    HealpixBinner.aggregate(raw, aggregation)
  }

  // ---- NetCDF reading (single product, runs on executor) --------------------

  /**
   * Read a single product into Row objects. This method runs on executors.
   * Supports local paths, `s3://` URLs (converted to `cdms3://` via the
   * netcdf-java cdm-s3 module), and `cdms3://` URIs directly.
   */
  private[geotrellishealpix] def readProduct(ref: ProductRef,
                          nside: Int,
                          config: ProductConfig): Seq[Row] = {
    val base = new HealpixBase(nside, Scheme.NESTED)

    val bandPath = toNetcdfPath(ref.path, config.s3Endpoint)
    logger.debug(s"Opening band file: $bandPath (original: ${ref.path})")
    val bandFile = NetcdfDatasets.openDataset(bandPath)
    val geoFile  = config.geoFileSuffix match {
      case Some(suffix) =>
        val geoPath = resolveGeoFilePath(bandPath, suffix)
        logger.debug(s"Opening geo file: $geoPath")
        NetcdfDatasets.openDataset(geoPath)
      case None => bandFile
    }

    try {
      val latVar = geoFile.findVariable(config.latVariable)
      val lonVar = geoFile.findVariable(config.lonVariable)

      require(latVar != null, s"Variable '${config.latVariable}' not found in ${geoFile.getLocation}")
      require(lonVar != null, s"Variable '${config.lonVariable}' not found in ${geoFile.getLocation}")

      val latData = latVar.read()
      val lonData = lonVar.read()
      val totalPixels = latData.getSize.toInt

      // Read band data
      //TODO support reading multiple bands from different assets
      val bandArrays: Seq[ucar.ma2.Array] = config.allVariables.map { name =>
        val v = bandFile.findVariable(name)
        require(v != null, s"Band variable '$name' not found in ${bandFile.getLocation}, available variables: ${bandFile.getVariables.stream().map(_.getNameAndDimensions).toList}")
        v.read()
      }

      val rows = new scala.collection.mutable.ArrayBuffer[Row](totalPixels)
      val fill = config.fillValue

      var i = 0
      while (i < totalPixels) {
        val lat = latData.getDouble(i)
        val lon = lonData.getDouble(i)

        // Skip fill / NaN coordinates
        if (!java.lang.Double.isNaN(lat) && !java.lang.Double.isNaN(lon) &&
            lat >= -90.0 && lat <= 90.0) {

          val bandValues = new Array[Any](config.allVariables.size)
          var allFill = true
          var b = 0
          while (b < bandArrays.size) {
            val raw = bandArrays(b).getDouble(i)
            if (java.lang.Double.isNaN(raw) || raw == fill) {
              bandValues(b) = null
            } else {
              bandValues(b) = raw
              allFill = false
            }
            b += 1
          }

          if (!allFill) {
            val cellId = ang2pix(base, lon, lat)
            val values: Seq[Any] = Seq[Any](cellId, ref.timestamp) ++ bandValues.toSeq
            rows += Row.fromSeq(values)
          }
        }
        i += 1
      }

      rows.toSeq
    } finally {
      bandFile.close()
      if (config.geoFileSuffix.isDefined) geoFile.close()
    }
  }

  /** (lonDeg, latDeg) -> HEALPix NESTED cell id (same as HealpixToGeotrellis). */
  private def ang2pix(base: HealpixBase, lonDeg: Double, latDeg: Double): Long = {
    val lonNorm = ((lonDeg % 360.0) + 360.0) % 360.0
    val phi     = math.toRadians(lonNorm)
    val theta   = math.toRadians(90.0 - math.max(-90.0, math.min(90.0, latDeg)))
    base.ang2pix(new Pointing(theta, phi))
  }

  // ---- S3 URI handling (cdm-s3 module) --------------------------------------

  /**
   * Convert an S3-style URL to a `cdms3://` URI that the netcdf-java `cdm-s3`
   * module can open via `NetcdfFile.open()`.
   *
   * Supported input formats:
   *  - `s3://bucket/key`            → `cdms3://endpoint#bucket/key`
   *  - `https://endpoint/bucket/key`→ `cdms3://endpoint#bucket/key`
   *  - `/local/path`                → returned unchanged
   *  - `cdms3://...`                → returned unchanged
   *
   * The S3 endpoint is configurable; it defaults to the Copernicus Data Space
   * EODATA endpoint (`eodata.dataspace.copernicus.eu`).
   *
   * @param path        the original path or URL
   * @param s3Endpoint  S3 endpoint hostname (used when input is `s3://` scheme)
   * @return a path that `NetcdfFile.open()` can handle
   */
  def toNetcdfPath(path: String,
                   s3Endpoint: String = "eodata.dataspace.copernicus.eu"): String = {
    if (path.startsWith("cdms3://")) {
      path
    } else if (path.startsWith("s3://")) {
    val withoutScheme = path.stripPrefix("s3://")
    val slashIdx = withoutScheme.indexOf('/')
    if (slashIdx < 0) {
      s"cdms3://cdse@$s3Endpoint/$withoutScheme#delimiter=/"
    } else {
      val bucket = withoutScheme.substring(0, slashIdx)
      val objectKey = withoutScheme.substring(slashIdx + 1)
      s"cdms3://cdse@${s3Endpoint.replace("eodata.","")}/$bucket?eodata/$objectKey#delimiter=/"
    }
    }else if (path.startsWith("https://") || path.startsWith("http://")) {
      // https://endpoint/bucket/key/file.nc → cdms3://endpoint/bucket?key/file.nc#delimiter=/
      val uri = new URI(path)
      val endpoint = uri.getHost + (if (uri.getPort > 0) s":${uri.getPort}" else "")
      val segments = uri.getPath.stripPrefix("/")
      val slashIdx = segments.indexOf('/')
      if (slashIdx < 0) {
        s"cdms3://$endpoint/$segments#delimiter=/"
      } else {
        val bucket = segments.substring(0, slashIdx)
        val objectKey = segments.substring(slashIdx + 1)
        s"cdms3://$endpoint/$bucket?$objectKey#delimiter=/"
      }
    } else {
      path
    }
  }

  /**
   * Resolve a geo-coordinate file path relative to a band file path.
   * Handles local paths, s3:// URLs, and cdms3:// URIs.
   */
  private def resolveGeoFilePath(bandPath: String, suffix: String): String = {
    if (bandPath.startsWith("cdms3://")) {
      // cdms3://endpoint/bucket?dir/file.nc#delimiter=/ → cdms3://endpoint/bucket?dir/suffix#delimiter=/
      // Strip fragment first
      val withoutFragment = if (bandPath.contains('#')) bandPath.substring(0, bandPath.indexOf('#')) else bandPath
      val fragment = if (bandPath.contains('#')) bandPath.substring(bandPath.indexOf('#')) else "#delimiter=/"
      val qIdx = withoutFragment.indexOf('?')
      if (qIdx >= 0) {
        val prefix = withoutFragment.substring(0, qIdx + 1) // "cdms3://endpoint/bucket?"
        val objectKey = withoutFragment.substring(qIdx + 1) // "dir/file.nc"
        val parentKey = objectKey.substring(0, objectKey.lastIndexOf('/'))
        s"$prefix$parentKey/$suffix$fragment"
      } else {
        bandPath
      }
    } else if (bandPath.contains("://")) {
      val uri = new URI(bandPath)
      val parentPath = uri.getPath.substring(0, uri.getPath.lastIndexOf('/'))
      new URI(uri.getScheme, uri.getAuthority, parentPath + "/" + suffix, null, null).toString
    } else {
      new java.io.File(bandPath).getParent + "/" + suffix
    }
  }

  // ---- Utility: create a synthetic NetCDF for testing -----------------------

  /**
   * Creates a small NetCDF-3 file at `path` with lat, lon, and band
   * variables of the given size. Useful for unit tests.
   */
  def createSyntheticNetCDF(path: String,
                            nPixels: Int,
                            latitudes: Array[Float],
                            longitudes: Array[Float],
                            bands: Map[String, Array[Float]]): Unit = {
    import ucar.ma2.{DataType => NcDataType}
    import ucar.nc2.NetcdfFileWriter

    val writer = NetcdfFileWriter.createNew(
      NetcdfFileWriter.Version.netcdf3, path, null)

    val pixelDim = writer.addDimension(null, "pixel", nPixels)
    val dims = new java.util.ArrayList[ucar.nc2.Dimension]()
    dims.add(pixelDim)

    writer.addVariable(null, "latitude", NcDataType.FLOAT, dims)
    writer.addVariable(null, "longitude", NcDataType.FLOAT, dims)
    bands.keys.foreach { name =>
      writer.addVariable(null, name, NcDataType.FLOAT, dims)
    }

    writer.create()

    val latArray = ucar.ma2.Array.factory(NcDataType.FLOAT, Array(nPixels), latitudes)
    val lonArray = ucar.ma2.Array.factory(NcDataType.FLOAT, Array(nPixels), longitudes)
    writer.write(writer.findVariable("latitude"), latArray)
    writer.write(writer.findVariable("longitude"), lonArray)

    bands.foreach { case (name, values) =>
      val arr = ucar.ma2.Array.factory(NcDataType.FLOAT, Array(nPixels), values)
      writer.write(writer.findVariable(name), arr)
    }

    writer.close()
  }
}

