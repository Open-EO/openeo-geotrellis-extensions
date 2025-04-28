package org.openeo

import _root_.geotrellis.proj4._
import _root_.geotrellis.raster._
import _root_.geotrellis.vector._
import _root_.geotrellis.vector.io.json._
import _root_.geotrellis.vector.reproject.Reproject._
import net.jodah.failsafe.event.{ExecutionAttemptedEvent, ExecutionCompletedEvent}
import net.jodah.failsafe.{ExecutionContext, Failsafe, RetryPolicy => FailsafeRetryPolicy}
import org.apache.spark.SparkContext
import org.locationtech.jts.geom.Geometry
import org.locationtech.proj4j.{BasicCoordinateTransform, ProjCoordinate}
import org.openeo.geotrellis.ProjectedPolygons.{reprojectGeometryRefined, reprojectPolygonRefined}
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection}
import org.slf4j.Logger
import scalaj.http.{HttpResponse, HttpStatusException}
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.core.retry.{RetryPolicy => AwsRetryPolicy}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.lang
import java.net.{SocketException, SocketTimeoutException, URI}
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.concurrent.ConcurrentHashMap
import scala.compat.java8.FunctionConverters._
import scala.reflect.io.Directory


package object geotrellis {
  def logTiming[R](context: String)(action: => R)(implicit logger: Logger): R = {
    if (logger.isDebugEnabled()) {
      val start = Instant.now()
      logger.debug(s"$context: start")

      try
        action
      finally {
        val end = Instant.now()
        val elapsed = Duration.between(start, end)

        logger.debug(s"$context: end, elapsed $elapsed")
      }
    } else action
  }

  private val s3ClientCache = new ConcurrentHashMap[(Region, URI), S3Client]

  private[geotrellis] def s3Client(region: Region = null, endpoint: URI = null): S3Client =
    s3ClientCache.computeIfAbsent((region, endpoint), s3Client.asJava)

  private val s3Client: ((Region, URI)) => S3Client = { case (region, endpoint) =>
    val retryCondition =
      OrRetryCondition.create(
        RetryCondition.defaultRetryCondition(),
        RetryOnErrorCodeCondition.create("RequestTimeout")
      )
    val backoffStrategy =
      FullJitterBackoffStrategy.builder()
        .baseDelay(Duration.ofMillis(50))
        .maxBackoffTime(Duration.ofMillis(15))
        .build()
    val retryPolicy =
      AwsRetryPolicy.defaultRetryPolicy()
        .toBuilder()
        .retryCondition(retryCondition)
        .backoffStrategy(backoffStrategy)
        .build()
    val overrideConfig =
      ClientOverrideConfiguration.builder()
        //.putHeader("x-amz-request-payer", "requester")
        .retryPolicy(retryPolicy)
        .build()

    val clientBuilder = S3Client.builder()
      .overrideConfiguration(overrideConfig)
      .region(if(region != null) region else Region.EU_CENTRAL_1)

    val theClient = if(endpoint != null) {
      clientBuilder.serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build()).forcePathStyle(true).endpointOverride(endpoint).build()
    }else{
      clientBuilder.build()
    }

    theClient
  }

  private val bucketRegionCache = new ConcurrentHashMap[String, Region]

  private[geotrellis] def bucketRegion(bucketName: String): Region =
    bucketRegionCache.computeIfAbsent(bucketName, fetchBucketRegion.asJava)

  private val fetchBucketRegion: String => Region = bucketName => {
    // might not be allowed unless owner of the bucket (403)
    val getBucketLocationRequest = GetBucketLocationRequest.builder()
      .bucket(bucketName)
      .build()

    val regionName = s3Client()
      .getBucketLocation(getBucketLocationRequest)
      .locationConstraint()
      .toString

    Region.of(regionName)
  }

  def toSigned(cellType: CellType): CellType = {
    cellType match {
      case UByteCellType => ByteCellType
      case UByteConstantNoDataCellType => ByteConstantNoDataCellType
      case UByteUserDefinedNoDataCellType(noDataValue) => ByteUserDefinedNoDataCellType(noDataValue)
      case UShortCellType => ShortCellType
      case UShortConstantNoDataCellType => ShortConstantNoDataCellType
      case UShortUserDefinedNoDataCellType(noDataValue) => ShortUserDefinedNoDataCellType(noDataValue)
      case FloatConstantNoDataCellType => cellType
      case ShortConstantNoDataCellType => cellType
      case BitCellType => cellType
      case ByteConstantNoDataCellType => cellType
      case ByteCellType => cellType
      case ByteUserDefinedNoDataCellType(_) => cellType
      case ShortCellType => cellType
      case ShortUserDefinedNoDataCellType(_) => cellType
      case IntConstantNoDataCellType => cellType
      case IntCellType => cellType
      case IntUserDefinedNoDataCellType(_) => cellType
      case FloatCellType => cellType
      case FloatUserDefinedNoDataCellType(_) => cellType
      case DoubleConstantNoDataCellType => cellType
      case DoubleCellType => cellType
      case DoubleUserDefinedNoDataCellType(_) => cellType
      case _ => throw new IllegalArgumentException("Cannot convert to unsigned equivalent: '" + cellType.getClass.getName + "'.")
    }
  }

  /**
   * Inspired on 'Files.createTempFile', but does not create an empty file.
   * The default permissions of 'createTempFile' are a bit too strict too: 600, which is not accessible by other users.
   * This function could have default 664 for example.
   */
  def getTempFile(prefix: String, suffix: String): Path = {
    val prefixNonNull = if (prefix == null) "" else prefix
    val suffixNonNull = if (suffix == null) ".tmp" else suffix
    val tmpdirProp = sun.security.action.GetPropertyAction.privilegedGetProperty("java.io.tmpdir")
    val tmpdir = Paths.get(tmpdirProp)
    val random = new java.security.SecureRandom()

    def generatePath(prefix: String, suffix: String, dir: Path) = {
      val n = random.nextLong
      val s = prefix + java.lang.Long.toUnsignedString(n) + suffix
      val name = dir.getFileSystem.getPath(s)
      if (name.getParent != null) throw new IllegalArgumentException("Invalid prefix or suffix")
      else dir.resolve(name)
    }

    generatePath(prefixNonNull, suffixNonNull, tmpdir)
  }

  def sortableSourceName(sourceName: SourceName): String = sourceName match {
    case s: SourcePath => s.value
    case s: StringName => s.value
    case s => s.toString // ex: EmptyName
  }

  object TemporalResolution extends Enumeration {
    val seconds, days, undefined = Value
  }


  /**
   * taken from DefaultProcessApi
   * Specific to retry HttpResponse, without throwError logic.
   */
  def withRetryAfterRetries[R](context: String)(httpResponseCallback: => HttpResponse[R])(implicit logger: Logger): HttpResponse[R] = {
    val retryableResult: HttpResponse[R] => Boolean = r => r.is5xx
    val retryableException: Throwable => Boolean = {
      case _: HttpStatusException =>
        logger.warn("withRetryAfterRetries does not support .throwError logic.")
        false
      case _: SocketTimeoutException => true
      case _: SocketException => true // Got this when terminating test server in the middle of a request
      case e => logger.error(s"Not attempting to retry unrecoverable error in context: '$context' " + e.getMessage); false
    }

    val shakyConnectionRetryPolicy = new FailsafeRetryPolicy[HttpResponse[R]]()
      .handleResultIf(retryableResult.asJava)
      .handleIf(retryableException.asJava)
      .withBackoff(1, 1000, ChronoUnit.SECONDS) // should not reach maxDelay because of maxAttempts 5
      .withJitter(0.5)
      .withMaxAttempts(5)
      .onFailedAttempt((attempt: ExecutionAttemptedEvent[HttpResponse[R]]) => {
        val msg = if (attempt.getLastFailure != null) {
          attempt.getLastFailure
        } else if (attempt.getLastResult != null) {
          "result code: " + attempt.getLastResult.code
        } else ""
        logger.warn(s"Attempt ${attempt.getAttemptCount} failed in context: '$context' " + msg)
      })
      .onFailure((execution: ExecutionCompletedEvent[HttpResponse[R]]) => {
        val e = execution.getFailure
        logger.error(s"Failed after ${execution.getAttemptCount} attempt(s) in context: '$context'" + e.getMessage)
      })

    val isRateLimitingResponse: (HttpResponse[R], Throwable) => Boolean =
      (response, _ /* ignore exceptions, those are handled in shakyConnectionRetryPolicy */) => response.code == 429
    val rateLimitingRetryPolicy = new FailsafeRetryPolicy[HttpResponse[R]]()
      .handleIf(isRateLimitingResponse.asJava)
      .withMaxAttempts(5)
      .withDelay((lastResponse: HttpResponse[R], _: Throwable, executionContext: ExecutionContext) => {
        val retryAfterSecondsCalculated = (20 * math.pow(1.6, executionContext.getAttemptCount - 1)).toLong
        val retryAfterSeconds = lastResponse.headers
          .find { case (header, _) => header equalsIgnoreCase "retry-after" }
          .map { case (_, values) => values.head.toLong }
          .getOrElse(retryAfterSecondsCalculated)

        val duration = Duration.ofSeconds(retryAfterSeconds)
        logger.warn(s"Attempt ${executionContext.getAttemptCount} failed in context: '$context' Scheduled retry in $duration")
        duration
      })

    Failsafe
      .`with`(java.util.Arrays.asList(shakyConnectionRetryPolicy, rateLimitingRetryPolicy))
      .get(() => {
        httpResponseCallback
      })
  }


  def healthCheckExtentAssert(projectedExtent: ProjectedExtent, messagePrefix: String): Unit = {
    val message = healthCheckExtentMessage(projectedExtent)
    // Ideally this would log the current load_collection / load_stac ID that is being executed
    if (message.isDefined) {
      throw new IllegalArgumentException(messagePrefix + message.get)
    }
  }

  def healthCheckExtentWarn(projectedExtent: ProjectedExtent, messagePrefix: String)(implicit logger: Logger): Unit = {
    val message = healthCheckExtentMessage(projectedExtent)
    // Ideally this would log the current load_collection / load_stac ID that is being executed
    if (message.isDefined) {
      logger.warn(messagePrefix + message.get)
    }
  }

  def healthCheckExtent(projectedExtent: ProjectedExtent): Boolean = {
    healthCheckExtentMessage(projectedExtent).isEmpty
  }

  def isCrsCoveredInHealthCheck(crs: CRS): Boolean = {
    if (crs.proj4jCrs.getProjection.getName == "utm") return true
    Seq(LatLng, CRS.fromName("EPSG:3035"), CRS.fromName("EPSG:31370"), WebMercator).contains(crs)
  }

  /**
   * Python equivalent: health_check_extent
   */
  private def healthCheckExtentMessage(projectedExtent: ProjectedExtent): Option[String] = {
    // TODO: Find way to use general library https://github.com/locationtech/proj4j/issues/113
    // A quick workaround might be to use pyproj trough Jep
    // positive width and height is already enforced in Extent.
    if (lang.Double.isNaN(projectedExtent.extent.xmin) || lang.Double.isNaN(projectedExtent.extent.xmax) ||
      lang.Double.isNaN(projectedExtent.extent.ymin) || lang.Double.isNaN(projectedExtent.extent.ymax)) {
      return Some("Extent contains NaN values: " + projectedExtent)
    }
    if (lang.Double.isInfinite(projectedExtent.extent.xmin) || lang.Double.isInfinite(projectedExtent.extent.xmax) ||
      lang.Double.isInfinite(projectedExtent.extent.ymin) || lang.Double.isInfinite(projectedExtent.extent.ymax)) {
      return Some("Extent contains infinite values: " + projectedExtent)
    }
    val polygonIsUTM = projectedExtent.crs.proj4jCrs.getProjection.getName == "utm"
    if (polygonIsUTM) {
      val horizontal_tolerance = 4.0

      // This is an extent that has the highest sensible values for northern and/or southern hemisphere UTM zones
      val utmProjectedBoundsOriginal = Extent(166021.44, 0000000.00, 833978.56, 10000000)
      val utmProjectedBounds = utmProjectedBoundsOriginal.buffer(
        utmProjectedBoundsOriginal.width * horizontal_tolerance, 0)
      if (!utmProjectedBounds.contains(projectedExtent.extent)) {
        return Some("Extent not within its CRS limits: " + projectedExtent)
      }
    } else if (projectedExtent.crs == LatLng) { // EPSG:4326
      val horizontal_tolerance = 1.1
      val vertical_tolerance = 1.1
      if ((projectedExtent.extent.xmin < -180 * horizontal_tolerance)
        || (projectedExtent.extent.xmax > +360 * horizontal_tolerance) // Allow 0-360 range too
        || (projectedExtent.extent.ymin < -90 * vertical_tolerance)
        || (projectedExtent.extent.ymax > +90 * vertical_tolerance)) {
        return Some("Extent not within its CRS limits: " + projectedExtent)
      }
    } else if (projectedExtent.crs == CRS.fromName("EPSG:3035")) {
      val horizontal_buffer = 0.1
      val vertical_buffer = 0.1
      val projectedBoundsOriginal = Extent(1908523.29, 1137678.21, 6901611.5, 6872461.46)
      val projectedBounds = projectedBoundsOriginal.buffer(
        projectedBoundsOriginal.width * horizontal_buffer, projectedBoundsOriginal.height * vertical_buffer)
      if (!projectedBounds.contains(projectedExtent.extent)) {
        return Some("Extent not within its CRS limits: " + projectedExtent)
      }
    } else if (projectedExtent.crs == CRS.fromName("EPSG:31370")) { // Lambert
      val horizontal_buffer = 2.0
      val vertical_buffer = 2.0
      val projectedBoundsOriginal = Extent(14637.25, 20909.21, 297133.13, 246424.28)
      val projectedBounds = projectedBoundsOriginal.buffer(
        projectedBoundsOriginal.width * horizontal_buffer, projectedBoundsOriginal.height * vertical_buffer)
      if (!projectedBounds.contains(projectedExtent.extent)) {
        return Some("Extent not within its CRS limits: " + projectedExtent)
      }
    } else if (projectedExtent.crs == WebMercator) { // EPSG:3857 same as EPSG:900913?
      val horizontal_tolerance = 1.1
      val vertical_tolerance = 1.1
      val projectedBoundsOriginal = Extent(-20037508.34, -20048966.1, 20037508.34, 20048966.1)
      val projectedBounds = projectedBoundsOriginal.buffer(
        projectedBoundsOriginal.width * horizontal_tolerance, projectedBoundsOriginal.height * vertical_tolerance)
      if (!projectedBounds.contains(projectedExtent.extent)) {
        return Some("Extent not within its CRS limits: " + projectedExtent)
      }
    }
    None
  }

  def isExtentValidInCrs(extent: ProjectedExtent, targetCrs: CRS)(implicit logger: Logger): Boolean = {
    if (extent.crs == targetCrs) return true
    if (targetCrs == CRS.fromEpsgCode(4326) || targetCrs == Sinusoidal) {
      // LatLon and Sinusoidal cover the whole world, so it's always valid
      // Function would work fine without this check too.
      return true
    }
    try {
      // Instead of reprojecting and back to check the validity, is would be
      // better to project the extent to LatLng and see if it fits in the CRS.area_of_use extent.
      // A valid extent in UTM EPSG:32660 might be invalid in LatLng tough, because it crosses the 180deg border
      val reprojected = safeReproject(extent, targetCrs)
      if (!healthCheckExtent(reprojected)) return false
      val reprojectedBack = safeReproject(reprojected, extent.crs)
      if (!healthCheckExtent(reprojectedBack)) return false
      reprojectedBack.extent.intersects(extent.extent) // Easy check for unknown CRSes
    } catch {
      case e: Throwable =>
        // TODO: Might give too mush logs, so keep disabled
        // logger.warn(s"Error while checking if extent is valid in CRS: $targetCrs. " +
        //   s"Extent: $extent. Error: ${e.getMessage}")
        false
    }
  }

  /**
   * Will give actual angle, but as positive value.
   */
  def to_0_360_range(x: Double): Double = {
    if (x <= 360 && x >= 0) return x // 0/360 should be kept, even if it means the same
    (x + 360 * 10) % 360
  }

  /**
   * Will give actual angle, but in the LatLng extent.
   */
  def to_min180_180_range(x: Double): Double = {
    if (x <= 180 && x >= -180) return x // The sign of 180 should be kept, even if it means the same
    val n = (x + 360 * 10) % 360
    if (n > 180) n - 360 else n
  }

  object SafeTransform {
    object SafeProj4Transform {
      def apply(src: CRS, dest: CRS): Transform =
        if (src == dest) {
          (x: Double, y: Double) => (x, y)
        } else {
          val t = new BasicCoordinateTransform(src.proj4jCrs, dest.proj4jCrs)

          { (x: Double, y: Double) =>
            val xNew = if (src == LatLng) to_min180_180_range(x) else x
            val srcP = new ProjCoordinate(xNew, y)
            val destP = new ProjCoordinate
            t.transform(srcP, destP)
            (destP.x, destP.y)
          }
        }
    }

    def apply(src: CRS, dest: CRS): (Double, Double) => (Double, Double) =
      src.alternateTransform(dest) match {
        case Some(f) => f
        case None => SafeProj4Transform(src, dest)
      }
  }

  def polygonWrapAntimeridian(polygon: Polygon): Polygon = {
    // This trick is mainly for GLOBAL-MOSAICS, where a product geometry is not split on the antimeridian
    // Should only be called when you know the Polygon should not be covering more than half of the world.
    // For example when projecting from UTM.
    val polygonCopy = polygon.copy().asInstanceOf[Polygon]
    if (polygon.extent.width > 180) {
      // Documentation says CoordinateSequenceFilter should be used, but that has a complex interface
      polygonCopy.getCoordinates.foreach(c => c.x = to_0_360_range(c.x))
    }
    polygonCopy
  }

  def projectedPolygonWrapAntimeridian(inputProjectedPolygons: ProjectedPolygons): ProjectedPolygons = {
    if (inputProjectedPolygons.crs != LatLng) throw new IllegalArgumentException("Only LatLng CRS is supported for wrapping")
    val geometries = inputProjectedPolygons.geometries.map {
      case polygon: Polygon => polygonWrapAntimeridian(polygon)
      case multiPolygon: MultiPolygon =>
        MultiPolygon(multiPolygon.polygons.map(polygonWrapAntimeridian))
      case geometry: Geometry =>
        // logger.info("Was only expecting (Multi)Polygon, but got: " + geometry)
        geometry
    }
    ProjectedPolygons(geometries, inputProjectedPolygons.crs)
  }

  def safeReprojectPolygons(inputProjectedPolygons: ProjectedPolygons, targetCrs: CRS, refine:Boolean = false): ProjectedPolygons = {
    if (inputProjectedPolygons.crs == targetCrs) return inputProjectedPolygons
    val transform = SafeTransform(inputProjectedPolygons.crs, targetCrs)
    val inputIsUTM = inputProjectedPolygons.crs.proj4jCrs.getProjection.getName == "utm"
    val treatXIn360 = inputIsUTM && targetCrs == LatLng && inputProjectedPolygons.riskOfCrossingAntimeridian
    val geometries = if (refine) {
      inputProjectedPolygons.geometries.map {
        // TODO: Make allowed error dependent on resolution.
        reprojectGeometryRefined(_, transform, 0.001, treatXIn360 = treatXIn360)
      }
    } else {
      inputProjectedPolygons.geometries.map(_.reproject(transform))
    }
    var pp = ProjectedPolygons(geometries, targetCrs)
    if (treatXIn360) {
      // When the extent was utm, some wrapping may have occurred
      pp = projectedPolygonWrapAntimeridian(pp)
    }
    pp
  }

  def projectedPolygonsEquals(p1: ProjectedPolygons, p2: ProjectedPolygons, tolerance: Double = 0.001): Boolean = {
    if (p1.crs != p2.crs) return false
    if (p1.polygons.length != p2.polygons.length) return false
    p1.polygons.zip(p2.polygons)
      .map { case (a, b) => a.equalsExact(b, tolerance) }
      .forall(identity)
  }

  def safeReproject(inputProjectedExtent: ProjectedExtent, targetCrs: CRS)(implicit logger: Logger): ProjectedExtent = {
    if (inputProjectedExtent.crs == targetCrs) return inputProjectedExtent
    val transform = SafeTransform(inputProjectedExtent.crs, targetCrs)
    val reprojectedPolygon = reprojectExtentAsPolygon(inputProjectedExtent.extent, transform, 0.001) // TODO: Adapt relError to CRS
    val envelope = reprojectedPolygon.getEnvelopeInternal
    var reprojected = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val inputIsUTM = inputProjectedExtent.crs.proj4jCrs.getProjection.getName == "utm"
    // utm for sure does not cover the world width. TODO: Also swap when coming from WebMercator or some other niche projections
    if (targetCrs == LatLng && inputIsUTM && reprojected.width > 180 && reprojected.width < 360) {
      // Fix width wrap when projecting over anti meridian in LatLon.
      // Reprojecting an extent could make the left and the right side swap differently over the antimeridian.
      // A single point will always work, so consider that as the source of truth
      // When we experience a problem because the extent does not fit in [-180, 180] range, convert it to the [0-360] range.
      val centerReprojected = inputProjectedExtent.extent.center.reproject(inputProjectedExtent.crs, targetCrs)
      val reprojectedCenter = reprojected.center
      if (Math.abs(centerReprojected.x - reprojectedCenter.x) > 10) {
        val swapped = Extent(to_0_360_range(reprojected.xmax), reprojected.ymin, to_0_360_range(reprojected.xmin), reprojected.ymax)
        val swappedCenter = swapped.center
        if (Math.abs(centerReprojected.x - swappedCenter.x) < 10) {
          reprojected = swapped
        }
      }
    }
    ProjectedExtent(reprojected, targetCrs)
  }

  def safeReprojectToPolygon(inputProjectedExtent: ProjectedExtent, targetCrs: CRS)(implicit logger: Logger): ProjectedPolygons = {
    if (inputProjectedExtent.crs == targetCrs) return ProjectedPolygons(inputProjectedExtent.extent.toPolygon(), inputProjectedExtent.crs)
    val transform = SafeTransform(inputProjectedExtent.crs, targetCrs)
    val reprojectedPolygon = reprojectExtentAsPolygon(inputProjectedExtent.extent, transform, 0.001) // TODO: Adapt relError to CRS
    val inputIsUTM = inputProjectedExtent.crs.proj4jCrs.getProjection.getName == "utm"

    var pp = ProjectedPolygons(reprojectedPolygon, targetCrs)
    if (inputIsUTM && targetCrs == LatLng) {
      // When the extent was utm, some wrapping may have occurred
      pp = projectedPolygonWrapAntimeridian(pp)
    }
    pp
  }

  def toGeoJsonDebug(polygons: ProjectedPolygons): String = {
    val str = polygons.getFlatMultiPolygon.toGeoJson()
    var j = SimpleJson.parse(str)
    val crs = polygons.crs.proj4jCrs.getName
    // GeoJson officially only supports LatLng, but qgis works with custom CRSes too:
    j = j + ("crs" -> Map("type" -> "name", "properties" -> Map("name" -> crs)))
    SimpleJson.serialize(j)
  }

  def toGeoJsonDebug(featureCollection: FeatureCollection): String = {
    // Only supports LatLng
    val featureJSON = featureCollection.features.map(f => _root_.geotrellis.vector.Feature(f.geometry.get, Map("id" -> f.id)))
    JsonFeatureCollection(featureJSON).asJson.toString()
  }

  def toGeoJsonDebug(geometry: Geometry): String = {
    toGeoJsonDebug(ProjectedPolygons(Array(geometry), LatLng))
  }

  def toGeoJsonDebug(inputProjectedExtent: ProjectedExtent): String = {
    // Does not reproject refined. Could also have antimeridian issues. For debugging only
    toGeoJsonDebug(ProjectedPolygons(inputProjectedExtent))
  }

  def toGeoJsonDebug(extent: Extent): String = {
    toGeoJsonDebug(ProjectedExtent(extent, LatLng))
  }

  def dumpGeoJson(str: String, name: Option[String] = None): Unit = {
    val outDir = Paths.get("tmp/")
    Files.createDirectories(outDir)
    var path = name.getOrElse("dumpGeoJson")
    if (!path.contains("tmp/")) {
      path = "tmp/" + path
    }
    if (!path.endsWith("json")) {
      path = path + ".geojson"
    }

    Files.write(Paths.get(path), str.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  /**
   * DANGER. Might crash system if no memory limit is specified.
   */
  def trigger_JVM_OOM(): Unit = {
    val oneGBinBytes = 1024 * 1024 * 1024
    val arr = (0 until 200).map(_ => {
      val memoryBlob = new Array[Int](oneGBinBytes) // 4Gb
      // Optionally, fill the array with some data to ensure it's actually allocated
      // This step is not strictly necessary for allocation but can be useful for testing
      for (i <- memoryBlob.indices) {
        memoryBlob(i) = 1
      }
      memoryBlob
    })
    print(arr) // use arr to make sure it does not get optimized away
  }

  //noinspection ScalaUnusedSymbol
  def trigger_JVM_OOM_executor(): Unit = {
    val sc = SparkContext.getOrCreate()
    val rdd = sc.parallelize(1 to 4)
    rdd.map(x => {
      trigger_JVM_OOM()
      x + 1
    }
    ).collect()
  }
}
