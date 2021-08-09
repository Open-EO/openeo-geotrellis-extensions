package org.openeo.geotrellissentinelhub

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.SampleType.SampleType

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.MINUTES
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object BatchProcessingService {
  // TODO: invalidate key on 401 Unauthorized
  private val accessTokenCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(30, MINUTES) // TODO: depend on expires_in in response
    .build(new CacheLoader[(String, String), String] {
      override def load(credentials: (String, String)): String = credentials match {
        case (clientId, clientSecret) => new AuthApi().authenticate(clientId, clientSecret).access_token
      }
    })
}

class BatchProcessingService(endpoint: String, val bucketName: String, clientId: String, clientSecret: String) {
  import BatchProcessingService._

  private def accessToken: String = accessTokenCache.get((clientId, clientSecret))

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any]): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names, sampleType,
      metadata_properties, processing_options)
  }

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any]): String = {
    // TODO: implement retries
    // workaround for bug where upper bound is considered inclusive in OpenEO
    val (from, to) = includeEndDay(from_date, to_date)

    val polygonsSimplification = new LocalGridSparseMultiPolygonSimplification(gridCrs = CRS.fromEpsgCode(32631), gridTileSize = 10000 * 0.95)
    val (multiPolygon, multiPolygonCrs) = polygonsSimplification.simplify(polygons, crs)

    val dateTimes = new DefaultCatalogApi(endpoint).dateTimes(collection_id, multiPolygon, multiPolygonCrs, from, to,
      accessToken, queryProperties = mapDataFilters(metadata_properties))

    val batchProcessingApi = new BatchProcessingApi(endpoint)
    val batchRequestId = batchProcessingApi.createBatchProcess(
      dataset_id,
      multiPolygon,
      multiPolygonCrs,
      dateTimes,
      band_names.asScala,
      sampleType,
      additionalDataFilters = metadata_properties,
      processing_options,
      bucketName,
      description = s"$dataset_id ${polygons.size} $from_date $to_date $band_names",
      accessToken
    ).id

    batchProcessingApi.startBatchProcess(batchRequestId, accessToken)

    batchRequestId
  }

  def get_batch_process_status(batch_request_id: String): String =
    new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken).status

  def restart_partially_failed_batch_process(batch_request_id: String): Unit =
    new BatchProcessingApi(endpoint).restartPartiallyFailedBatchProcess(batch_request_id, accessToken)

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
    val features = new DefaultCatalogApi(endpoint).searchCard4L(collection_id, reprojectedBoundingBox, from, to, accessToken,
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
}
