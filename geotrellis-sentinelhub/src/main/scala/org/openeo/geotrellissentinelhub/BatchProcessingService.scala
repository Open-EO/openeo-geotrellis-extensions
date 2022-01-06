package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.SampleType.SampleType

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._

// TODO: snake_case for these arguments
class BatchProcessingService(endpoint: String, val bucketName: String, clientId: String, clientSecret: String) {
  private val catalogApi: CatalogApi = new DefaultCatalogApi(endpoint)

  private def authorized[R](fn: String => R): R =
    org.openeo.geotrellissentinelhub.authorized[R](clientId, clientSecret)(fn)

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

    val multiPolygon = multiPolygonFromPolygonExteriors(polygons)
    val multiPolygonCrs = crs

    val dateTimes = authorized { accessToken =>
      catalogApi.dateTimes(collection_id, multiPolygon, multiPolygonCrs, from, to,
        accessToken, Criteria.toQueryProperties(metadata_properties))
    }

    if (dateTimes.isEmpty) return null

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    val batchRequestId = authorized { accessToken =>
      batchProcessingApi.createBatchProcess(
        dataset_id,
        multiPolygon,
        multiPolygonCrs,
        dateTimes,
        band_names.asScala,
        sampleType,
        additionalDataFilters = Criteria.toDataFilters(metadata_properties),
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
      else if (Set("sentinel-1-grd", "S1GRD") contains dataset_id)
        new Sentinel1GrdInitialCacheOperation(dataset_id)
      else throw new IllegalArgumentException(
        """only datasets "sentinel-2-l2a" (previously "S2L2A") and
          | "sentinel-1-grd" (previously "S1GRD") are supported""".stripMargin)

    cacheOperation.startBatchProcess(collection_id, dataset_id, polygons, crs, from, to, band_names,
      sampleType, metadata_properties, processing_options, bucketName, subfolder, collecting_folder, this)
  }

  def get_batch_process_status(batch_request_id: String): String = authorized { accessToken =>
    new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken).status
  }

  def restart_partially_failed_batch_process(batch_request_id: String): Unit = authorized { accessToken =>
    new BatchProcessingApi(endpoint).restartPartiallyFailedBatchProcess(batch_request_id, accessToken)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                   from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, util.Map[String, Any]],
                                   subfolder: String, request_group_id: String): util.List[String] = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_card4l_batch_processes(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names,
      dem_instance, metadata_properties, subfolder, request_group_id)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                   crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, util.Map[String, Any]],
                                   subfolder: String, request_group_id: String): util.List[String] = {
    // TODO: add error handling
    val multiPolygon = multiPolygonFromPolygonExteriors(polygons).reproject(crs, LatLng)
    val multiPolygonCrs = LatLng

    // from should be start of day, to should be end of day (23:59:59)
    val (from, to) = includeEndDay(from_date, to_date)

    // original features that overlap in space and time
    val features = authorized { accessToken =>
      catalogApi.searchCard4L(collection_id, multiPolygon, multiPolygonCrs, from, to,
        accessToken, Criteria.toQueryProperties(metadata_properties))
    }

    // their intersections with input polygons (all should be in LatLng)
    val intersectionFeatures = features.mapValues(feature =>
      feature.mapGeom(geom => geom intersection multiPolygon)
    )

    val batchProcessingApi = new BatchProcessingApi(endpoint)

    // TODO: the web tool creates one batch process, analyses it, polls until ANALYSIS_DONE, then creates the remaining
    //  processes and starts them all
    val batchRequestIds =
      for ((id, Feature(intersection, datetime)) <- intersectionFeatures)
        yield authorized { accessToken =>
          batchProcessingApi.createCard4LBatchProcess(
            dataset_id,
            bounds = intersection,
            dateTime = datetime,
            band_names.asScala,
            dataTakeId(id),
            card4lId = request_group_id,
            dem_instance,
            additionalDataFilters = Criteria.toDataFilters(metadata_properties),
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
