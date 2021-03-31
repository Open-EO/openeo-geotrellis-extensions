package org.openeo.geotrellissentinelhub

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.{Extent, Feature, ProjectedExtent}
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.LoggerFactory
import scalaj.http.HttpStatusException

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit.MINUTES
import scala.collection.JavaConverters._

object BatchProcessingService {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingService])

  // TODO: invalidate key on 401 Unauthorized
  private val accessTokenCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(30, MINUTES) // TODO: depend on expires_in in response
    .build(new CacheLoader[(String, String, String), String] {
      override def load(credentials: (String, String, String)): String = credentials match {
        case (endpoint, clientId, clientSecret) => new AuthApi(endpoint).authenticate(clientId, clientSecret).access_token
      }
    })
}

class BatchProcessingService(endpoint: String, val bucketName: String, clientId: String, clientSecret: String) {
  import BatchProcessingService._

  private def accessToken: String = accessTokenCache.get((endpoint, clientId, clientSecret))

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, Any], processing_options: util.Map[String, Any])
  : String = try {
    // TODO: implement retries
    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))

    // workaround for bug where upper bound is considered inclusive in OpenEO
    val (from, to) = includeEndDay(from_date, to_date)

    val dateTimes = new CatalogApi(endpoint).dateTimes(collection_id, boundingBox, from, to, accessToken,
      queryProperties = mapDataFilters(metadata_properties))

    val batchProcessingApi = new BatchProcessingApi(endpoint)
    val batchRequestId = batchProcessingApi.createBatchProcess(
      dataset_id,
      boundingBox,
      dateTimes,
      band_names.asScala,
      sampleType,
      additionalDataFilters = metadata_properties,
      processing_options,
      bucketName,
      description = s"$dataset_id $bbox $bbox_srs $from_date $to_date $band_names",
      accessToken
    ).id

    batchProcessingApi.startBatchProcess(batchRequestId, accessToken)

    batchRequestId
  } catch {
    case e: HttpStatusException =>
      logger.error(e.body, e) // TODO: include context (request details), move this to the Apis instead
      throw e
  }

  def get_batch_process_status(batchRequestId: String): String =
    new BatchProcessingApi(endpoint).getBatchProcess(batchRequestId, accessToken).status

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                   from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, Any], subfolder: String,
                                   request_group_id: String): util.List[String] = {
    // TODO: add error handling
    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(LatLng), LatLng)

    // from should be start of day, to should be end of day (23:59:59)
    val (from, to) = includeEndDay(from_date, to_date)

    // original features that overlap in space and time
    val features = new CatalogApi(endpoint).searchCard4L(collection_id, reprojectedBoundingBox, from, to, accessToken,
      queryProperties = mapDataFilters(metadata_properties))

    // their intersections with bounding box (all should be in LatLng)
    val intersectionFeatures = features.mapValues(feature =>
      feature.mapGeom(geom => geom intersection reprojectedBoundingBox.extent)
    )

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    // TODO: the web tool creates one batch process, analyses it, polls until ANALYSIS_DONE, then creates the remaining
    //  processes and starts them all
    val batchRequestIds =
      for ((id, Feature(intersection, datetime)) <- intersectionFeatures)
        yield batchProcessingApi.createCard4LBatchProcess(
          dataset_id,
          bounds = intersection,
          dateTime = datetime,
          band_names.asScala,
          dataTakeId(id),
          card4lId = request_group_id,
          dem_instance,
          additionalDataFilters = metadata_properties,
          bucketName,
          subfolder,
          accessToken
        ).id

    for (batchRequestId <- batchRequestIds) {
      batchProcessingApi.startBatchProcess(batchRequestId, accessToken)
    }

    batchRequestIds.toIndexedSeq.asJava
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

  private def mapDataFilters(metadataProperties: util.Map[String, Any]): collection.Map[String, String] =
    metadataProperties.asScala
      .map {
        case ("orbitDirection", value: String) => "sat:orbit_state" -> value
        case (property, _) => throw new IllegalArgumentException(s"unsupported metadata property $property")
      }
}
