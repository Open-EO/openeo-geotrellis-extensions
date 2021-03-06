package org.openeo.geotrellissentinelhub

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.LoggerFactory
import scalaj.http.HttpStatusException

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.MINUTES
import scala.collection.JavaConverters._

object BatchProcessingService {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingService])

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

class BatchProcessingService(bucketName: String, clientId: String, clientSecret: String) {
  import BatchProcessingService._

  private def accessToken: String = accessTokenCache.get((clientId, clientSecret))

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType): String = try {
    // TODO: implement retries
    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = {
      // workaround for bug where upper bound is considered inclusive in OpenEO
      val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
      ZonedDateTime.parse(to_date).toLocalDate.atTime(endOfDay).toZonedDateTime
    }

    val catalogApi = new CatalogApi
    val dateTimes = catalogApi.dateTimes(collection_id, boundingBox, from, to, accessToken)

    val batchProcessingApi = new BatchProcessingApi
    val batchRequestId = batchProcessingApi.createBatchProcess(
      dataset_id,
      boundingBox,
      dateTimes,
      band_names.asScala,
      sampleType,
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
    new BatchProcessingApi().getBatchProcess(batchRequestId, accessToken).status
}
