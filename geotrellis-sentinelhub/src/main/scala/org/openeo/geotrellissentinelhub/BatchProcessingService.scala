package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.LoggerFactory

import java.time.ZoneOffset.UTC
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable

object BatchProcessingService {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingService])
  
  case class BatchProcess(id: String, status: String, value_estimate: java.math.BigDecimal,
                          @deprecated("incorrect, derive from value_estimate") processing_units_spent: java.math.BigDecimal)
}

class BatchProcessingService(endpoint: String, val bucketName: String, authorizer: Authorizer) {
  import BatchProcessingService._

  // convenience methods for Python client
  def this(endpoint: String, bucket_name: String, client_id: String, client_secret: String) =
    this(endpoint, bucket_name,
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(client_id, client_secret))

  def this(endpoint: String, bucket_name: String, client_id: String, client_secret: String,
           zookeeper_connection_string: String, zookeeper_access_token_path: String) =
    this(endpoint, bucket_name,
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(
        zookeeper_connection_string, zookeeper_access_token_path,
        client_id, client_secret))

  private def authorized[R](fn: String => R): R = authorizer.authorized(fn)

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names, sampleType,
      metadata_properties, processing_options, subfolder)
  }

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any]): String = {
    start_batch_process(collection_id, dataset_id, bbox, bbox_srs, from_date, to_date, band_names, sampleType,
      metadata_properties, processing_options, subfolder = null)
  }

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any]): String =
    start_batch_process(collection_id, dataset_id, polygons, crs, from_date, to_date, band_names,
      sampleType, metadata_properties, processing_options, subfolder = null)

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    // TODO: implement retries
    // workaround for bug where upper bound is considered inclusive in OpenEO
    val (from, to) = includeEndDay(from_date, to_date)

    val multiPolygon = simplify(polygons)
    val multiPolygonCrs = crs

    val dateTimes = {
      if (from isAfter to) Seq()
      else authorized { accessToken =>
        val catalogApi = if (collection_id == null) new MadeToMeasureCatalogApi else new DefaultCatalogApi(endpoint)
        catalogApi.dateTimes(collection_id, multiPolygon, multiPolygonCrs, from, to,
          accessToken, Criteria.toQueryProperties(metadata_properties))
      }
    }

    if (dateTimes.isEmpty)
      throw NoSuchFeaturesException(message =
        s"""no features found for criteria:
           |collection ID "$collection_id"
           |${polygons.length} polygon(s)
           |[$from_date, $to_date]
           |metadata properties $metadata_properties""".stripMargin)

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    val batchRequestId = authorized { accessToken =>
      batchProcessingApi.createBatchProcess(
        dataset_id,
        multiPolygon,
        multiPolygonCrs,
        dateTimes,
        band_names.asScala,
        sampleType,
        additionalDataFilters = Criteria.toDataFilters(metadata_properties, band_names.asScala),
        processing_options,
        bucketName,
        description = s"$dataset_id ${polygons.length} $from_date $to_date $band_names",
        accessToken,
        subfolder
      ).id
    }

    authorized { accessToken => batchProcessingApi.startBatchProcess(batchRequestId, accessToken) }

    batchRequestId
  }

  def start_batch_process_cached(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                 from_date: String, to_date: String, band_names: util.List[String],
                                 sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                                 processing_options: util.Map[String, Any], subfolder: String,
                                 collecting_folder: String): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process_cached(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names,
      sampleType, metadata_properties, processing_options, subfolder, collecting_folder)
  }

  // TODO: move to the CachingService? What about the call to start_batch_process() then?
  def start_batch_process_cached(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                 crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                                 sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                                 processing_options: util.Map[String, Any], subfolder: String,
                                 collecting_folder: String): String = {
    require(metadata_properties.isEmpty, "metadata_properties are not supported yet")

    val (from, to) = includeEndDay(from_date, to_date)

    val cacheOperation =
      if (Set("sentinel-2-l2a", "S2L2A") contains dataset_id)
        new Sentinel2L2AInitialCacheOperation(dataset_id)
      else if (Set("sentinel-1-grd", "S1GRD") contains dataset_id) {
        // https://forum.sentinel-hub.com/t/sentinel-hub-december-2022-improvements/6198
        val defaultDemInstance =
          if (LocalDateTime.now() isBefore LocalDate.of(2022, 12, 5).atStartOfDay()) "MAPZEN"
          else "COPERNICUS"
        new Sentinel1GrdInitialCacheOperation(dataset_id, defaultDemInstance)
      } else throw new IllegalArgumentException(
        """only datasets "sentinel-2-l2a" (previously "S2L2A") and
          | "sentinel-1-grd" (previously "S1GRD") are supported""".stripMargin)

    cacheOperation.startBatchProcess(collection_id, dataset_id, polygons, crs, from, to, band_names,
      sampleType, metadata_properties, processing_options, bucketName, subfolder, collecting_folder, this)
  }

  def get_batch_process_status(batch_request_id: String): String = authorized { accessToken =>
    new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken).status
  }

  def get_batch_process(batch_request_id: String): BatchProcess = authorized { accessToken =>
    val response = new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken)

    // eventually this should just return the exact value that the API gives us; until then it's an approximation and
    // small deviations are of no consequence but we already install a return type of BigDecimal
    val defaultTemporalInterval = 3
    val actualPuToEstimateRatio = 1.0 / 3
    val estimateSecureFactor = actualPuToEstimateRatio * 2
    val temporalInterval = response.temporalIntervalInDays
      .map(_.ceil.toInt max 1)
      .getOrElse(defaultTemporalInterval)

    val slightlyMoreAccurateProcessingUnitsSpent: Option[BigDecimal] = response.valueEstimate
      .map(_ * estimateSecureFactor * defaultTemporalInterval / temporalInterval)

    BatchProcess(response.id, response.status, response.valueEstimate.map(_.bigDecimal).orNull,
      slightlyMoreAccurateProcessingUnitsSpent.map(_.bigDecimal).orNull)
  }

  def restart_partially_failed_batch_process(batch_request_id: String): Unit = authorized { accessToken =>
    new BatchProcessingApi(endpoint).restartPartiallyFailedBatchProcess(batch_request_id, accessToken)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                   from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, util.Map[String, Any]],
                                   subfolder: String, request_group_uuid: String): util.List[String] = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_card4l_batch_processes(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names,
      dem_instance, metadata_properties, subfolder, request_group_uuid)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                   crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, util.Map[String, Any]],
                                   subfolder: String, request_group_uuid: String): util.List[String] = {
    // TODO: add error handling
    val card4lId = fixUuid(request_group_uuid) // TODO: remove workaround for invalid UUID

    val geometry = simplify(polygons).reproject(crs, LatLng)
    val geometryCrs = LatLng

    // from should be start of day, to should be end of day (23:59:59)
    val (from, to) = includeEndDay(from_date, to_date)

    // original features that overlap in space and time
    val features = authorized { accessToken =>
      new DefaultCatalogApi(endpoint).searchCard4L(collection_id, geometry, geometryCrs, from, to,
        accessToken, Criteria.toQueryProperties(metadata_properties))
    }

    // their intersections with input polygons (all should be in LatLng)
    val intersectionFeatures = features.mapValues(feature =>
      feature.mapGeom(geom => geom intersection geometry)
    )

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    // TODO: the web tool creates one batch process, analyses it, polls until ANALYSIS_DONE, then creates the remaining
    //  processes and starts them all
    val batchRequestIds =
      for ((id, Feature(intersection, featureData)) <- intersectionFeatures)
        yield authorized { accessToken =>
          batchProcessingApi.createCard4LBatchProcess(
            dataset_id,
            bounds = intersection,
            dateTime = featureData.dateTime,
            band_names.asScala,
            dataTakeId(id),
            card4lId,
            dem_instance,
            additionalDataFilters = Criteria.toDataFilters(metadata_properties, band_names.asScala),
            bucketName,
            subfolder,
            accessToken
          ).id
        }

    for (batchRequestId <- batchRequestIds) {
      authorized { accessToken => batchProcessingApi.startBatchProcess(batchRequestId, accessToken) }
    }

    batchRequestIds.toIndexedSeq.asJava
  }

  private def fixUuid(id: String): UUID = {
    try UUID.fromString(id)
    catch {
      case _: IllegalArgumentException =>
        logger.warn(s"invalid UUID $id, maybe hyphens will work")

        val withHyphens = new mutable.StringBuilder(id)
          .insert(20, '-')
          .insert(16, '-')
          .insert(12, '-')
          .insert( 8, '-')
          .result()

        UUID.fromString(withHyphens)
    }
  }

  private def dataTakeId(featureId: String): String = {
    val penultimatePart = featureId.split("_").reverse(1) // from source at https://apps.sentinel-hub.com/s1-card4l/
    penultimatePart
  }

  private def includeEndDay(from_date: String, to_date: String): (ZonedDateTime, ZonedDateTime) = {
    val from = ZonedDateTime.parse(from_date)
    val to = {
      val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
      ZonedDateTime.parse(to_date).toLocalDate.atTime(endOfDay).toZonedDateTime
    }

    (from, to)
  }
}
