package org.openeo.geotrellissentinelhub

import geotrellis.vector.Geometry
import org.apache.commons.io.FileUtils.deleteDirectory
import org.openeo.geotrellissentinelhub.ElasticsearchRepository.CacheEntry
import org.slf4j.LoggerFactory

import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.time.ZonedDateTime
import java.util.stream.Collectors.toList
import scala.collection.JavaConverters._

object CachingService {
  private val logger = LoggerFactory.getLogger(classOf[CachingService])

  // TODO: put this in a central place
  private def sequentialDays(from: ZonedDateTime, to: ZonedDateTime): Stream[ZonedDateTime] = {
    def sequentialDays0(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDays0(from plusDays 1)

    sequentialDays0(from)
      .takeWhile(date => !(date isAfter to))
  }
}

class CachingService {
  import CachingService._

  def download_and_cache_results(bucket_name: String, subfolder: String, collecting_folder: String): Unit = {
    val elasticsearchRepository = new ElasticsearchRepository("https://es-apps-dev.vgt.vito.be:443")
    val cacheIndex = "sentinel-hub-s2l2a-cache"
    val tilingGridIndex = "sentinel-hub-tiling-grid-1"
    val collectingDir = Paths.get(collecting_folder)

    val actualTiles = collection.mutable.Set[CacheEntry]() // TODO: avoid mutation

    def cacheTile(tileId: String, date: ZonedDateTime, bandName: String, geometry: Geometry = null,
                  empty: Boolean = false): CacheEntry = {
      val location = if (geometry != null) geometry else elasticsearchRepository.getGeometry(tilingGridIndex, tileId)
      val entry = CacheEntry(tileId, date, bandName, location, empty)
      elasticsearchRepository.cache(cacheIndex, entry)
      actualTiles += entry
      entry
    }

    val batchProcessContext = new S3Service().loadBatchProcessContext(bucket_name, subfolder)

    val Some(incompleteTiles) = batchProcessContext.incompleteTiles
    val Some(lower) = batchProcessContext.lower
    val Some(upper) = batchProcessContext.upper
    val Some(missingBandNames) = batchProcessContext.missingBandNames

    new S3Service().downloadBatchProcessResults(
      bucket_name,
      subfolder,
      Paths.get("/data/projects/OpenEO/sentinel-hub-s2l2a-cache"),
      missingBandNames,
      onDownloaded = (tileId, date, bandName) => {
        val entry = cacheTile(tileId, date, bandName)

        entry.filePath.foreach { filePath =>
          try {
            Files.createSymbolicLink(collectingDir.resolve(filePath.getFileName), filePath)
            logger.debug(s"symlinked $filePath from the recent past to $collectingDir")
          } catch {
            case _: FileAlreadyExistsException => /* ignore */
          }
        }
      }
    )

    logger.debug(s"cached ${actualTiles.size} tiles from the narrow request")

    val expectedTiles = for { // tiles expected from the narrow request
      (gridTileId, geometry) <- incompleteTiles
      date <- sequentialDays(lower, upper)
      bandName <- missingBandNames
    } yield (gridTileId, geometry, date, bandName)

    logger.debug(s"was expecting ${expectedTiles.size} tiles for the narrow request")

    val emptyTiles = expectedTiles.filterNot { case (tileId, _, date, bandName) =>
      actualTiles.exists(actualTile => actualTile.tileId == tileId && actualTile.date.isEqual(date) && actualTile.bandName == bandName)
    }

    emptyTiles foreach { case (tileId, geometry, date, bandName) =>
      cacheTile(tileId, date, bandName, geometry, empty = true)
    }

    logger.debug(s"cached ${emptyTiles.size} tiles missing from the narrow request")
  }

  def upload_multiband_tiles(subfolder: String, collecting_folder: String, bucket_name: String): String = {
    val collectingFolder = Paths.get(collecting_folder)
    val assembledFolder = Files.createTempDirectory(Paths.get("/tmp_epod/openeo_assembled"), "assembled_")

    val s3Service = new S3Service

    try {
      val bandNames = s3Service.loadBatchProcessContext(bucket_name, subfolder).bandNames

      assembleMultibandTiles(collectingFolder, assembledFolder, bandNames)

      val prefix = s3Service.uploadRecursively(assembledFolder, bucket_name)
      logger.debug(s"uploaded $assembledFolder to s3://$bucket_name/$prefix")
      prefix
    } finally deleteDirectory(assembledFolder.toFile)
  }

  private def assembleMultibandTiles(collectingDir: Path, assembledDir: Path, bandNames: Seq[String]): Unit = {
    import scala.sys.process._

    // TODO: only include tiffs?
    val singleBandTiffs = Files.list(collectingDir).collect(toList[Path]).asScala

    val bandsGrouped = singleBandTiffs.groupBy { singleBandTiff =>
      val Array(tileId, date, _) = singleBandTiff.getFileName.toString.split("-")
      (tileId, date)
    }.mapValues(bands => bands.sortBy { singleBandTiff =>
      val bandName = singleBandTiff.getFileName.toString.split("-")(2).takeWhile(_ != '.')
      bandNames.indexOf(bandName)
    })

    bandsGrouped foreach { case ((tileId, date), bandTiffs) =>
      val vrtFile = collectingDir.resolve("combined.vrt").toAbsolutePath.toString
      val outputFile = assembledDir.resolve(s"$tileId-$date.tif").toAbsolutePath.toString

      logger.debug(s"combining $bandTiffs to $outputFile")

      val gdalbuildvrt = Seq("gdalbuildvrt", "-q", "-separate", vrtFile) ++ bandTiffs.map(_.toAbsolutePath.toString)
      if (gdalbuildvrt.! != 0) {
        throw new IllegalStateException(s"${gdalbuildvrt mkString " "} returned non-zero exit status") // TODO: better error handling; also: gdalbuildvrt silently skips nonexistent files
      }

      val gdal_translate = Seq("gdal_translate", vrtFile, outputFile)
      if (gdal_translate.! != 0) {
        throw new IllegalStateException(s"${gdal_translate mkString " "} returned non-zero exit status") // TODO: better error handling
      }
    }
  }
}
