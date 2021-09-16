package org.openeo.geotrellissentinelhub

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.ElasticsearchRepository.GridTile
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
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

  // TODO: put this in a central place
  private def sequentialDays(from: ZonedDateTime, to: ZonedDateTime): Stream[ZonedDateTime] = {
    def sequentialDays0(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDays0(from plusDays 1)

    sequentialDays0(from)
      .takeWhile(date => !(date isAfter to))
  }
}

// TODO: snake_case for these arguments
class BatchProcessingService(endpoint: String, val bucketName: String, clientId: String, clientSecret: String) {
  import BatchProcessingService._

  private def accessToken: String = accessTokenCache.get((clientId, clientSecret))

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_batch_process(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names, sampleType,
      metadata_properties, processing_options, subfolder)
  }

  def start_batch_process(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String, from_date: String,
                          to_date: String, band_names: util.List[String], sampleType: SampleType,
                          metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any]): String = {
    start_batch_process(collection_id, dataset_id, bbox, bbox_srs, from_date, to_date, band_names, sampleType,
      metadata_properties, processing_options, subfolder = null)
  }

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any]): String =
    start_batch_process(collection_id, dataset_id, polygons, crs, from_date, to_date, band_names,
      sampleType, metadata_properties, processing_options, subfolder = null)

  def start_batch_process(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                          crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                          sampleType: SampleType, metadata_properties: util.Map[String, Any],
                          processing_options: util.Map[String, Any], subfolder: String): String = {
    // TODO: implement retries
    // workaround for bug where upper bound is considered inclusive in OpenEO
    val (from, to) = includeEndDay(from_date, to_date)

    val multiPolygon = multiPolygonFromPolygonExteriors(polygons)
    val multiPolygonCrs = crs

    val dateTimes = new DefaultCatalogApi(endpoint).dateTimes(collection_id, multiPolygon, multiPolygonCrs, from, to,
      accessToken, queryProperties = mapDataFilters(metadata_properties))

    if (dateTimes.isEmpty) return null

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
      accessToken,
      subfolder
    ).id

    batchProcessingApi.startBatchProcess(batchRequestId, accessToken)

    batchRequestId
  }

  // TODO: overload for polygons
  // TODO: remove code duplication
  // TODO: move to the CachingService? What about the call to start_batch_process() then?
  def start_batch_process_cached(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                 from_date: String, to_date: String, band_names: util.List[String],
                                 sampleType: SampleType, metadata_properties: util.Map[String, Any],
                                 processing_options: util.Map[String, Any], subfolder: String,
                                 collecting_folder: String): String = {
    require(Set("S2L2A", "sentinel-2-l2a") contains dataset_id, """at the moment, only dataset_id "sentinel-2-l2a" (previously "S2L2A") is supported""")
    require(metadata_properties.isEmpty, "metadata_properties are not supported yet")
    require(processing_options.isEmpty, "processing_options are not supported yet")

    // TODO: make this uri configurable
    val elasticsearchRepository = new ElasticsearchRepository("https://es-apps-dev.vgt.vito.be:443")
    val tilingGridIndex = "sentinel-hub-tiling-grid-1"
    val cacheIndex = "sentinel-hub-s2l2a-cache"
    val collectingFolder = Paths.get(collecting_folder)

    val polygon = bbox.toPolygon()
    val polygonCrs = CRS.fromName(bbox_srs)

    val geometry = polygon.reproject(polygonCrs, LatLng)
    val (from, to) = includeEndDay(from_date, to_date)
    val bandNames = band_names.asScala

    val s3Service = new S3Service

    val expectedTiles = for {
      GridTile(gridTileId, geometry) <- elasticsearchRepository.intersectingGridTiles(tilingGridIndex, geometry)
      date <- sequentialDays(from, to)
      bandName <- bandNames
    } yield (gridTileId, geometry, date, bandName)

    logger.debug(s"start_batch_process_cached: expecting ${expectedTiles.size} tiles for the initial request")

    val cacheEntries = elasticsearchRepository.queryCache(cacheIndex, geometry, from, to, bandNames)

    // = read single band tiles from cache directory (a tree) and put them in collectingFolder (flat)
    for {
      entry <- cacheEntries
      filePath <- entry.filePath
    } {
      val tileId = filePath.getParent.getFileName
      val date = filePath.getParent.getParent.getFileName

      Files.createSymbolicLink(collectingFolder.resolve(s"$date-$tileId-${filePath.getFileName}"), filePath)
      logger.debug(s"start_batch_process_cached: symlinked $filePath from the distant past to $collectingFolder")
    }

    val missingTiles = expectedTiles
      .filterNot { case (tileId, _, date, bandName) =>
        cacheEntries.exists(cachedTile =>
          cachedTile.tileId == tileId && cachedTile.date.isEqual(date) && cachedTile.bandName == bandName)
      }

    val initialBatchProcessContext = BatchProcessContext(bandNames, None, None, None, None)
    s3Service.saveBatchProcessContext(initialBatchProcessContext, bucketName, subfolder)

    if (missingTiles.isEmpty) {
      logger.debug("start_batch_process_cached: everything's cached, no need for additional requests")
      return null
    }

    logger.debug(s"start_batch_process_cached: ${missingTiles.size} tiles are not in the cache")

    val (incompleteTiles, lower, upper, missingBandNames) = narrowRequest(missingTiles, bandNames)

    val narrowBatchProcessContext = initialBatchProcessContext.copy(
      incompleteTiles = Some(incompleteTiles),
      lower = Some(lower),
      upper = Some(upper),
      missingBandNames = Some(missingBandNames)
    )
    s3Service.saveBatchProcessContext(narrowBatchProcessContext, bucketName, subfolder)

    val multiPolygons = shrink(incompleteTiles)
    val multiPolygonsCrs = LatLng

    start_batch_process(
      collection_id,
      dataset_id,
      multiPolygons.toArray,
      multiPolygonsCrs,
      from_date = ISO_OFFSET_DATE_TIME format lower,
      to_date = ISO_OFFSET_DATE_TIME format upper,
      band_names = missingBandNames.asJava,
      sampleType,
      metadata_properties,
      processing_options,
      subfolder
    )
  }

  // TODO: rename this method to e.g. batchRequestConstraints
  // TODO: introduce some (case) classes for argument and return type?
  private def narrowRequest(tiles: Iterable[(String, Geometry, ZonedDateTime, String)], bandNames: Seq[String]):
  (Seq[(String, Geometry)], ZonedDateTime, ZonedDateTime, Seq[String]) = {
    // turn incomplete tiles into a SHub request:
    // determine all dates with missing positions (includes missing bands)
    // do a request for:
    // - [lower, upper]
    // - positions with missing bands
    // - missing bands

    val missingMultibandTiles = tiles
      .groupBy { case (tileId, _, date, _) => (tileId, date) }
      .mapValues { cacheKeys =>
        val (_, geometry, _, _) = cacheKeys.head // same tile ID so same geometry
        val bandNames = cacheKeys.map { case (_, _, _, bandName) => bandName }.toSet
        (geometry, bandNames)
      }
      .toSeq

    val datesWithIncompleteBands = missingMultibandTiles
      .map { case ((_, date), _) => date }
      .distinct
      .sorted

    val lower = datesWithIncompleteBands.head
    val upper = datesWithIncompleteBands.last

    val incompleteTiles = missingMultibandTiles
      .map { case ((tileId,  _), (geometry, _)) => (tileId, geometry) }
      .distinct

    val missingBandNames = missingMultibandTiles
      .map { case (_, (_, bandNames)) => bandNames }
      .reduce {_ union _}

    val missingBandNamesOrdered = bandNames.filter(missingBandNames.contains)

    // TODO: is it necessary to return tile IDs?
    (incompleteTiles, lower, upper, missingBandNamesOrdered)
  }

  // TODO: get rid of tile ID arguments?
  private def shrink(tiles: Seq[(String, Geometry)]): Seq[MultiPolygon] = {
    tiles
      .map { case (_, geometry) =>
        val shrinkDistance = geometry.getEnvelopeInternal.getWidth * 0.05
        MultiPolygon(geometry.buffer(-shrinkDistance).asInstanceOf[Polygon])
      } // TODO: make it explicit that all grid tiles are MultiPolygons?
  }

  // flattens n MultiPolygons into 1 by taking their polygon exteriors
  // drawback: can result in big GeoJSON to send over the wire
  private def multiPolygonFromPolygonExteriors(multiPolygons: Array[MultiPolygon]): MultiPolygon =  {
    val polygonExteriors = for {
      multiPolygon <- multiPolygons
      polygon <- multiPolygon.polygons
    } yield Polygon(polygon.getExteriorRing)

    MultiPolygon(polygonExteriors)
  }

  def get_batch_process_status(batch_request_id: String): String =
    new BatchProcessingApi(endpoint).getBatchProcess(batch_request_id, accessToken).status

  def restart_partially_failed_batch_process(batch_request_id: String): Unit =
    new BatchProcessingApi(endpoint).restartPartiallyFailedBatchProcess(batch_request_id, accessToken)

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, bbox: Extent, bbox_srs: String,
                                   from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, Any], subfolder: String,
                                   request_group_id: String): util.List[String] = {
    val polygons = Array(MultiPolygon(bbox.toPolygon()))
    val polygonsCrs = CRS.fromName(bbox_srs)

    start_card4l_batch_processes(collection_id, dataset_id, polygons, polygonsCrs, from_date, to_date, band_names,
      dem_instance, metadata_properties, subfolder, request_group_id)
  }

  def start_card4l_batch_processes(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                                   crs: CRS, from_date: String, to_date: String, band_names: util.List[String],
                                   dem_instance: String, metadata_properties: util.Map[String, Any], subfolder: String,
                                   request_group_id: String): util.List[String] = {
    // TODO: add error handling
    val multiPolygon = multiPolygonFromPolygonExteriors(polygons).reproject(crs, LatLng)
    val multiPolygonCrs = LatLng

    // from should be start of day, to should be end of day (23:59:59)
    val (from, to) = includeEndDay(from_date, to_date)

    // original features that overlap in space and time
    val features = new DefaultCatalogApi(endpoint).searchCard4L(collection_id, multiPolygon, multiPolygonCrs, from, to,
      accessToken, queryProperties = mapDataFilters(metadata_properties))

    // their intersections with input polygons (all should be in LatLng)
    val intersectionFeatures = features.mapValues(feature =>
      feature.mapGeom(geom => geom intersection multiPolygon)
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
