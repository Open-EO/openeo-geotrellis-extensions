package org.openeo.geotrellissentinelhub

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import org.slf4j.LoggerFactory
import scalaj.http.HttpStatusException

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._

object BatchProcessingService {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingService])
}

class BatchProcessingService(clientId: String, clientSecret: String) {
  import BatchProcessingService._

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String]): String = try {
    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = {
      // workaround for bug where upper bound is considered inclusive in OpenEO
      val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
      ZonedDateTime.parse(to_date).toLocalDate.atTime(endOfDay).toZonedDateTime
    }

    val catalogApi = new CatalogApi(clientId, clientSecret)
    val dateTimes = catalogApi.dateTimes(collection_id, boundingBox, from, to)

    val batchProcessingApi = new BatchProcessingApi(clientId, clientSecret)
    val batchRequestId = batchProcessingApi.createBatchProcess(
      dataset_id,
      boundingBox,
      dateTimes,
      band_names.asScala,
      bucketName = "openeo-sentinelhub-vito-test", // TODO: configure this somewhere (layercatalog.json?)
      description = s"$dataset_id $bbox $bbox_srs $from_date $to_date $band_names"
    ).id

    batchProcessingApi.startBatchProcess(batchRequestId)

    batchRequestId
  } catch {
    case e: HttpStatusException =>
      logger.error(e.body, e) // TODO: include context (request details), move this to the Apis instead
      throw e
  }

  def get_batch_process_status(batchRequestId: String): String = {
    val batchProcessingApi = new BatchProcessingApi(clientId, clientSecret)
    batchProcessingApi.getBatchProcess(batchRequestId).status
  }
}
