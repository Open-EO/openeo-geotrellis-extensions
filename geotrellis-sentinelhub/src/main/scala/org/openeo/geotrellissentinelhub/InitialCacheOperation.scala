package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.ElasticsearchCacheRepository.{CacheEntry, Sentinel1GrdCacheEntry, Sentinel2L2aCacheEntry}
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.openeo.geotrellissentinelhub.ElasticsearchTilingGridRepository.GridTile
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util
import scala.collection.JavaConverters._

// TODO: rename this
abstract class AbstractInitialCacheOperation[C <: CacheEntry] {
  // TODO: make this uri configurable
  protected val elasticsearchUri = "https://es-apps-dev.vgt.vito.be:443"

  protected def logger: Logger
  protected def normalize(processingOptions: collection.Map[String, Any]): collection.Map[String, Any]
  protected def queryCache(geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                           bandNames: Seq[String], processingOptions: collection.Map[String, Any]): Iterable[C]

  protected def saveInitialBatchProcessContext(bandNames: Seq[String], processingOptions: collection.Map[String, Any],
                                               bucketName: String, subfolder: String): Unit

  private def flatFileName(filePath: Path): String = {
    val tileId = filePath.getParent.getFileName
    val date = filePath.getParent.getParent.getFileName
    s"$date-$tileId-${filePath.getFileName}"
  }

  // TODO: better name
  protected def saveExtendedBatchProcessContext(bandNames: Seq[String], incompleteTiles: Seq[(String, Geometry)],
                                                lower: ZonedDateTime, upper: ZonedDateTime,
                                                missingBandNames: Seq[String],
                                                processingOptions: collection.Map[String, Any],
                                                bucketName: String, subfolder: String): Unit

  def startBatchProcess(collection_id: String, dataset_id: String, polygons: Array[MultiPolygon],
                        crs: CRS, from: ZonedDateTime, to: ZonedDateTime, band_names: util.List[String],
                        sampleType: SampleType, metadata_properties: util.Map[String, util.Map[String, Any]],
                        processing_options: util.Map[String, Any], bucketName: String, subfolder: String,
                        collecting_folder: String, batchProcessingService: BatchProcessingService): String = {
    // important: caching requires more strict processing options because specifying an option unknown to the cache
    // might return a cached result that does not match the option
    val processingOptions = normalize(processing_options.asScala)

    val tilingGridRepository = new ElasticsearchTilingGridRepository(elasticsearchUri)
    val tilingGridIndex = "sentinel-hub-tiling-grid-1"

    val collectingFolder = Paths.get(collecting_folder)

    val geometry = GeometryCollection(polygons).reproject(crs, LatLng)
    val bandNames = band_names.asScala

    val expectedTiles = for {
      gridTile <- tilingGridRepository.intersectingGridTiles(tilingGridIndex, geometry)
      date <- sequentialDays(from, to)
      bandName <- bandNames
    } yield (gridTile, date, bandName)

    logger.debug(s"expecting ${expectedTiles.size} tiles for the initial request")

    val cacheEntries = queryCache(geometry, from, to, bandNames, processingOptions)

    // = read single band tiles from cache directory (a tree) and put them in collectingFolder (flat)
    for {
      entry <- cacheEntries
      cachedFile <- entry.filePath
      flatFileName = this.flatFileName(cachedFile)
    } {
      val link = collectingFolder.resolve(flatFileName)
      val target = cachedFile

      if (!Files.exists(target)) {
        throw new IllegalStateException(s"symlink target $target does not exist")
      }

      Files.createSymbolicLink(link, target)
      logger.debug(s"symlinked cached file from the distant past: $link -> $target")
    }

    val missingTiles = expectedTiles
      .filterNot { case (GridTile(tileId, _), date, bandName) =>
        cacheEntries.exists(_.matchesExpected(tileId, date, bandName))
      }

    saveInitialBatchProcessContext(bandNames, processingOptions, bucketName, subfolder)

    if (missingTiles.isEmpty) {
      logger.debug("everything's cached, no need for additional requests")
      return null
    }

    logger.debug(s"${missingTiles.size} tiles are not in the cache")

    val (incompleteTiles, lower, upper, missingBandNames) = narrowRequest(missingTiles, bandNames)

    saveExtendedBatchProcessContext(bandNames, incompleteTiles, lower, upper, missingBandNames, processingOptions,
      bucketName, subfolder)

    val multiPolygons = Array(simplify(gridGeometries = incompleteTiles.map { case (_, geometry) => geometry }))
    val multiPolygonsCrs = LatLng

    batchProcessingService.start_batch_process(
      collection_id,
      dataset_id,
      multiPolygons,
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
  private def narrowRequest(tiles: Iterable[(GridTile, ZonedDateTime, String)], bandNames: Seq[String]):
  (Seq[(String, Geometry)], ZonedDateTime, ZonedDateTime, Seq[String]) = {
    // turn incomplete tiles into a SHub request:
    // determine all dates with missing positions (includes missing bands)
    // do a request for:
    // - [lower, upper]
    // - positions with missing bands
    // - missing bands

    val missingMultibandTiles = tiles
      .groupBy { case (GridTile(tileId, _), date, _) => (tileId, date) }
      .mapValues { cacheKeys =>
        val (GridTile(_, geometry), _, _) = cacheKeys.head // same tile ID so same geometry
        val bandNames = cacheKeys.map { case (_, _, bandName) => bandName }.toSet
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

    (incompleteTiles, lower, upper, missingBandNamesOrdered)
  }

  // TODO: make it explicit that all grid tiles are MultiPolygons?
  private def simplify(gridGeometries: Seq[Geometry]): MultiPolygon = {
    // make sure they dissolve properly
    val growDistance = gridGeometries.head.getEnvelopeInternal.getWidth * 0.05
    val quadrantSegments = -5 // TODO: what does this mean, exactly?
    val slightlyOverlappingGridGeometries =
      gridGeometries.map(_.buffer(growDistance, quadrantSegments).asInstanceOf[Polygon])

    def shrink(gridGeometry: Polygon): Polygon =
      gridGeometry.buffer(-growDistance * 3, quadrantSegments).asInstanceOf[Polygon]

    dissolve(slightlyOverlappingGridGeometries) match {
      case polygon: Polygon => MultiPolygon(shrink(polygon))
      case multiPolygon: MultiPolygon => MultiPolygon(multiPolygon.polygons.map(shrink))
    }
  }
}

object Sentinel2L2AInitialCacheOperation {
  private val logger = LoggerFactory.getLogger(classOf[Sentinel2L2AInitialCacheOperation])
}

class Sentinel2L2AInitialCacheOperation(dataset_id: String) extends AbstractInitialCacheOperation[Sentinel2L2aCacheEntry] {
  override protected def logger: Logger = Sentinel2L2AInitialCacheOperation.logger

  override protected def normalize(processingOptions: collection.Map[String, Any]): collection.Map[String, Any] = {
    require(processingOptions.isEmpty, s"processing_options are not supported for dataset $dataset_id")
    processingOptions
  }

  override protected def queryCache(geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
                                    processingOptions: collection.Map[String, Any]): Iterable[Sentinel2L2aCacheEntry] =
    new ElasticsearchCacheRepository(elasticsearchUri).query(cacheIndex = "sentinel-hub-s2l2a-cache", geometry, from, to, bandNames)

  override protected def saveInitialBatchProcessContext(bandNames: Seq[String],
                                                        processingOptions: collection.Map[String, Any],
                                                        bucketName: String, subfolder: String): Unit = {
    val initialBatchProcessContext = Sentinel2L2aBatchProcessContext(bandNames, None, None, None, None)
    new S3BatchProcessContextRepository(bucketName).saveTo(initialBatchProcessContext, subfolder)
  }

  override protected def saveExtendedBatchProcessContext(bandNames: Seq[String],
                                                         incompleteTiles: Seq[(String, Geometry)], lower: ZonedDateTime,
                                                         upper: ZonedDateTime, missingBandNames: Seq[String],
                                                         processingOptions: collection.Map[String, Any],
                                                         bucketName: String, subfolder: String): Unit = {
    val extendedBatchProcessContext = Sentinel2L2aBatchProcessContext(bandNames, Some(incompleteTiles), Some(lower),
      Some(upper), Some(missingBandNames))
    new S3BatchProcessContextRepository(bucketName).saveTo(extendedBatchProcessContext, subfolder)
  }
}

object Sentinel1GrdInitialCacheOperation {
  private val logger = LoggerFactory.getLogger(classOf[Sentinel1GrdInitialCacheOperation])
}

class Sentinel1GrdInitialCacheOperation(dataset_id: String, defaultDemInstance: String) extends AbstractInitialCacheOperation[Sentinel1GrdCacheEntry] {
  override protected def logger: Logger = Sentinel1GrdInitialCacheOperation.logger

  override protected def normalize(processingOptions: collection.Map[String, Any]): collection.Map[String, Any] = {
    val backCoeff = processingOptions.getOrElse("backCoeff",
      throw new IllegalArgumentException(s"processing_options for dataset $dataset_id must contain backCoeff"))
      .asInstanceOf[String].toUpperCase

    val orthorectify = processingOptions.getOrElse("orthorectify",
      throw new IllegalArgumentException(s"processing_options for dataset $dataset_id must contain orthorectify"))
      .asInstanceOf[Boolean]

    val demInstance = processingOptions.getOrElse("demInstance", defaultDemInstance).asInstanceOf[String].toUpperCase

    Map(
      "backCoeff" -> backCoeff,
      "orthorectify" -> orthorectify,
      "demInstance"-> demInstance
    )
  }

  override protected def queryCache(geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
                                    processingOptions: collection.Map[String, Any]): Iterable[Sentinel1GrdCacheEntry] = {
    val backCoeff = processingOptions("backCoeff").asInstanceOf[String]
    val orthorectify = processingOptions("orthorectify").asInstanceOf[Boolean]
    val demInstance = processingOptions("demInstance").asInstanceOf[String]

    new ElasticsearchCacheRepository(elasticsearchUri).querySentinel1(cacheIndex = "sentinel-hub-s1grd-cache", geometry, from, to,
      bandNames, backCoeff, orthorectify, demInstance)
  }

  override protected def saveInitialBatchProcessContext(bandNames: Seq[String],
                                                        processingOptions: collection.Map[String, Any],
                                                        bucketName: String, subfolder: String): Unit = {
    val (backCoeff, orthorectify, demInstance) = extractOptions(processingOptions)
    val initialBatchProcessContext = Sentinel1GrdBatchProcessContext(bandNames, backCoeff, orthorectify, demInstance,
      None, None, None, None)

    new S3BatchProcessContextRepository(bucketName).saveTo(initialBatchProcessContext, subfolder)
  }

  override protected def saveExtendedBatchProcessContext(bandNames: Seq[String],
                                                         incompleteTiles: Seq[(String, Geometry)], lower: ZonedDateTime,
                                                         upper: ZonedDateTime, missingBandNames: Seq[String],
                                                         processingOptions: collection.Map[String, Any],
                                                         bucketName: String, subfolder: String): Unit = {
    val (backCoeff, orthorectify, demInstance) = extractOptions(processingOptions)
    val extendedBatchProcessContext = Sentinel1GrdBatchProcessContext(bandNames, backCoeff, orthorectify, demInstance,
      Some(incompleteTiles), Some(lower), Some(upper), Some(missingBandNames))
    new S3BatchProcessContextRepository(bucketName).saveTo(extendedBatchProcessContext, subfolder)
  }

  // TODO: better name
  private def extractOptions(processingOptions: collection.Map[String, Any]): (String, Boolean, String) = {
    val backCoeff = processingOptions("backCoeff").asInstanceOf[String]
    val orthorectify = processingOptions("orthorectify").asInstanceOf[Boolean]
    val demInstance = processingOptions("demInstance").asInstanceOf[String]

    (backCoeff, orthorectify, demInstance)
  }
}
