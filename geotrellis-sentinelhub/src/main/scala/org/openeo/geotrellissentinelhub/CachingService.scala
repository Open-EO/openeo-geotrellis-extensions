package org.openeo.geotrellissentinelhub

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.Geometry
import org.apache.commons.io.FileUtils.deleteDirectory
import org.openeo.geotrellissentinelhub.ElasticsearchCacheRepository.{Sentinel1GrdCacheEntry, Sentinel2L2aCacheEntry}
import org.slf4j.LoggerFactory

import java.net.URI
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.time.format.DateTimeFormatter.BASIC_ISO_DATE
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.stream.Collectors.toList
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

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

  // TODO: reduce code duplication
  def download_and_cache_results(bucket_name: String, subfolder: String, collecting_folder: String): Unit = {
    val batchProcessContext = new S3BatchProcessContextRepository(bucket_name)
      .loadFrom(subfolder)

    batchProcessContext match {
      case context: Sentinel2L2aBatchProcessContext => downloadAndCacheResults(context, bucket_name, subfolder, collecting_folder)
      case context: Sentinel1GrdBatchProcessContext => downloadAndCacheResults(context, bucket_name, subfolder, collecting_folder)
    }
  }

  private def downloadAndCacheResults(batchProcessContext: Sentinel2L2aBatchProcessContext, bucket_name: String, subfolder: String, collecting_folder: String): Unit = {
    // TODO: make this uri configurable
    val elasticsearchUri = "https://es-apps-dev.vgt.vito.be:443"
    val cacheRepository = new ElasticsearchCacheRepository(elasticsearchUri)
    val cacheIndex = "sentinel-hub-s2l2a-cache"

    val tilingGridRepository = new ElasticsearchTilingGridRepository(elasticsearchUri)
    val tilingGridIndex = "sentinel-hub-tiling-grid-1"

    val collectingDir = Paths.get(collecting_folder)

    val actualTiles = collection.mutable.Set[Sentinel2L2aCacheEntry]() // TODO: avoid mutation

    def cacheTile(tileId: String, date: ZonedDateTime, bandName: String, geometry: Geometry = null,
                  empty: Boolean = false): Sentinel2L2aCacheEntry = {
      val location = if (geometry != null) geometry else tilingGridRepository.getGeometry(tilingGridIndex, tileId)
      val entry = Sentinel2L2aCacheEntry(tileId, date, bandName, location, empty)
      cacheRepository.save(cacheIndex, entry)
      actualTiles += entry
      entry
    }

    if (batchProcessContext.includesNarrowRequest) {
      val Some(incompleteTiles) = batchProcessContext.incompleteTiles
      val Some(lower) = batchProcessContext.lower
      val Some(upper) = batchProcessContext.upper
      val Some(missingBandNames) = batchProcessContext.missingBandNames

      downloadBatchProcessResults(
        bucket_name,
        subfolder,
        cacheDir = Paths.get("/data/projects/OpenEO/sentinel-hub-s2l2a-cache"),
        missingBandNames,
        onDownloaded = (tileId, date, bandName) => { // TODO: move this lambda into downloadBatchProcessResults?
          val entry = cacheTile(tileId, date, bandName)

          entry.filePath.foreach { cachedFile =>
            try {
              val flatFileName = s"${BASIC_ISO_DATE format entry.date.toLocalDate}-$tileId-${cachedFile.getFileName}"
              val link = collectingDir.resolve(flatFileName)
              val target = cachedFile

              if (!Files.exists(target)) {
                throw new IllegalStateException(s"symlink target $target does not exist")
              }

              Files.createSymbolicLink(link, target)
              logger.debug(s"symlinked cached file from the recent past: $link -> $target")
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
  }

  private def downloadAndCacheResults(batchProcessContext: Sentinel1GrdBatchProcessContext, bucket_name: String, subfolder: String, collecting_folder: String): Unit = {
    // TODO: make this uri configurable
    val elasticsearchUri = "https://es-apps-dev.vgt.vito.be:443"
    val cacheRepository = new ElasticsearchCacheRepository(elasticsearchUri)
    val cacheIndex = "sentinel-hub-s1grd-cache"

    val tilingGridRepository = new ElasticsearchTilingGridRepository(elasticsearchUri)
    val tilingGridIndex = "sentinel-hub-tiling-grid-1"

    val collectingDir = Paths.get(collecting_folder)

    val actualTiles = collection.mutable.Set[Sentinel1GrdCacheEntry]() // TODO: avoid mutation

    def cacheTile(backCoeff: String, orthorectify: Boolean, demInstance: String, tileId: String,
                  date: ZonedDateTime, bandName: String, geometry: Geometry = null,
                  empty: Boolean = false): Sentinel1GrdCacheEntry = {
      val location = if (geometry != null) geometry else tilingGridRepository.getGeometry(tilingGridIndex, tileId)
      val entry = Sentinel1GrdCacheEntry(tileId, date, bandName, backCoeff, orthorectify, demInstance, location, empty)
      cacheRepository.saveSentinel1(cacheIndex, entry)
      actualTiles += entry
      entry
    }

    if (batchProcessContext.includesNarrowRequest) {
      val backCoeff = batchProcessContext.backCoeff
      val orthorectify = batchProcessContext.orthorectify
      val demInstance = batchProcessContext.demInstance
      val Some(incompleteTiles) = batchProcessContext.incompleteTiles
      val Some(lower) = batchProcessContext.lower
      val Some(upper) = batchProcessContext.upper
      val Some(missingBandNames) = batchProcessContext.missingBandNames

      downloadBatchProcessResults(
        bucket_name,
        subfolder,
        cacheDir = Paths.get("/data/projects/OpenEO/sentinel-hub-s1grd-cache"),
        missingBandNames,
        backCoeff,
        orthorectify,
        demInstance,
        onDownloaded = (tileId, date, bandName) => { // TODO: move this lambda into downloadBatchProcessResults?
          val entry = cacheTile(backCoeff, orthorectify, demInstance, tileId, date, bandName)

          entry.filePath.foreach { cachedFile =>
            try {
              val flatFileName = s"${BASIC_ISO_DATE format entry.date.toLocalDate}-$tileId-${cachedFile.getFileName}"
              val link = collectingDir.resolve(flatFileName)
              val target = cachedFile

              if (!Files.exists(target)) {
                throw new IllegalStateException(s"symlink target $target does not exist")
              }

              Files.createSymbolicLink(link, target)
              logger.debug(s"symlinked cached file from the recent past: $link -> $target")
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
        cacheTile(backCoeff, orthorectify, demInstance, tileId, date, bandName, geometry, empty = true)
      }

      logger.debug(s"cached ${emptyTiles.size} tiles missing from the narrow request")
    }
  }

  // = download multiband tiles and write them as single band tiles to the cache directory (a tree)
  private def downloadBatchProcessResults(bucketName: String, subfolder: String, cacheDir: Path, bandNames: Seq[String],
                                  onDownloaded: (String, ZonedDateTime, String) => Unit): Unit = S3.withClient { s3Client =>
    import java.time.format.DateTimeFormatter.BASIC_ISO_DATE

    val tiffKeys = S3.listObjectIdentifiers(s3Client, bucketName, prefix = subfolder).iterator
      .map(_.key())
      .filter(_.endsWith(".tif"))

    val keyParts = tiffKeys.map { key =>
      // this specifically handles the tilePath for regular (non-CARD4L) batch processes to a particular subfolder,
      // i.e. s3://$bucketName/$subfolder/$tileId/_$date.tif (see BatchProcessingApi#createBatchProcess)
      val Array(_, tileId, fileName) = key.split("/")
      val date = fileName.split(raw"\.").head.drop(1)

      key -> (tileId, date)
    }.toMap

    // TODO: reduce code duplication with org.openeo.geotrellissentinelhub.ElasticsearchCacheRepository.Sentinel2L2aCacheEntry.filePath
    def subdirectory(date: String, tileId: String): Path = cacheDir.resolve(date).resolve(tileId)

    // create all subdirectories with a minimum of effort
    for {
      (tileId, date) <- keyParts.values.toSet
    } Files.createDirectories(subdirectory(date, tileId))

    for ((key, (tileId, date)) <- keyParts) {
      // TODO: avoid intermediary/temp geotiff
      val tempMultibandFile = Files.createTempFile(subfolder, null)

      try {
        S3.download(s3Client, bucketName, key, outputFile = tempMultibandFile)

        val multibandGeoTiff = GeoTiffReader.readMultiband(tempMultibandFile.toString)

        for ((bandName, singleBandTile) <- bandNames zip multibandGeoTiff.tile.bands) {
          val outputFile = subdirectory(date, tileId).resolve(s"$bandName.tif")

          SinglebandGeoTiff(singleBandTile, multibandGeoTiff.extent, multibandGeoTiff.crs)
            .write(outputFile.toString)

          onDownloaded(tileId, LocalDate.parse(date, BASIC_ISO_DATE).atStartOfDay(ZoneId.of("UTC")), bandName)
        }
      } finally Files.delete(tempMultibandFile)
    }
  }

  // = download multiband tiles and write them as single band tiles to the cache directory (a tree)
  private def downloadBatchProcessResults(bucketName: String, subfolder: String, cacheDir: Path, bandNames: Seq[String],
                                  backCoeff: String, orthorectify: Boolean, demInstance: String,
                                  onDownloaded: (String, ZonedDateTime, String) => Unit): Unit = S3.withClient { s3Client =>
    import java.time.format.DateTimeFormatter.BASIC_ISO_DATE

    val tiffKeys = S3.listObjectIdentifiers(s3Client, bucketName, prefix = subfolder).iterator
      .map(_.key())
      .filter(_.endsWith(".tif"))

    val keyParts = tiffKeys.map { key =>
      val Array(_, tileId, fileName) = key.split("/")
      val date = fileName.split(raw"\.").head.drop(1)

      key -> (tileId, date)
    }.toMap

    def subdirectory(date: String, tileId: String): Path = {
      // TODO: reduce code duplication with org.openeo.geotrellissentinelhub.ElasticsearchCacheRepository.Sentinel1GrdCacheEntry.filePath
      val orthorectifyFlag = if (orthorectify) "orthorectified" else "non-orthorectified"
      cacheDir.resolve(backCoeff).resolve(orthorectifyFlag).resolve(demInstance).resolve(date).resolve(tileId)
    }

    // create all subdirectories with a minimum of effort
    for {
      (tileId, date) <- keyParts.values.toSet
    } Files.createDirectories(subdirectory(date, tileId))

    for ((key, (tileId, date)) <- keyParts) {
      // TODO: avoid intermediary/temp geotiff
      val tempMultibandFile = Files.createTempFile(subfolder, null)

      try {
        S3.download(s3Client, bucketName, key, outputFile = tempMultibandFile)

        val multibandGeoTiff = GeoTiffReader.readMultiband(tempMultibandFile.toString)

        for ((bandName, singleBandTile) <- bandNames zip multibandGeoTiff.tile.bands) {
          val outputFile = subdirectory(date, tileId).resolve(s"$bandName.tif")

          SinglebandGeoTiff(singleBandTile, multibandGeoTiff.extent, multibandGeoTiff.crs)
            .write(outputFile.toString)

          onDownloaded(tileId, LocalDate.parse(date, BASIC_ISO_DATE).atStartOfDay(ZoneId.of("UTC")), bandName)
        }
      } finally Files.delete(tempMultibandFile)
    }
  }

  // assembles single band tiles in collecting_folder to multiband tiles, uploads these to
  // s3://$bucketName/assembled_xyz and returns "assembled_xyz"
  @deprecated("call assemble_multiband_tiles instead")
  def upload_multiband_tiles(subfolder: String, collecting_folder: String, bucket_name: String): String = {
    val assembledFolder = Paths.get(URI.create(assemble_multiband_tiles(subfolder, collecting_folder, bucket_name)))

    try {
      val prefix = new S3Service().uploadRecursively(assembledFolder, bucket_name)
      logger.debug(s"uploaded $assembledFolder to s3://$bucket_name/$prefix")
      prefix
    } finally deleteDirectory(assembledFolder.toFile)
  }

  // assembles single band tiles in collecting_folder to multiband tiles, saves these to
  // /tmp_epod/openeo_assembled/assembled_xyz and returns "file:///tmp_epod/openeo_assembled/assembled_xyz/"
  def assemble_multiband_tiles(subfolder: String, collecting_folder: String, bucket_name: String): String = {
    val collectingFolder = Paths.get(collecting_folder)
    val assembledFolder = Files.createTempDirectory(Paths.get("/tmp_epod/openeo_assembled"), "assembled_")

    val s3BatchProcessContextRepository = new S3BatchProcessContextRepository(bucket_name)

    val bandNames = s3BatchProcessContextRepository.loadFrom(subfolder) match {
      case context: Sentinel2L2aBatchProcessContext => context.bandNames
      case context: Sentinel1GrdBatchProcessContext => context.bandNames
    }

    assembleMultibandTiles(collectingFolder, assembledFolder, bandNames)

    assembledFolder.toUri.toString
  }

  // = read single band tiles from collectingDir (flat) and write them as multiband tiles to assembleDir (flat)
  private def assembleMultibandTiles(collectingDir: Path, assembledDir: Path, bandNames: Seq[String]): Unit = {
    import scala.sys.process._

    val isTiffFile: Path => Boolean =
      path => Files.isRegularFile(path) && path.getFileName.toString.endsWith(".tif")

    val singleBandTiffs = Files.list(collectingDir)
      .filter(isTiffFile.asJava)
      .collect(toList[Path]).asScala

    val bandsGrouped = singleBandTiffs.groupBy { singleBandTiff =>
      // TODO: this assumes the format of org.openeo.geotrellissentinelhub.AbstractInitialCacheOperation#flatFileName
      val Array(date, tileId, _) = singleBandTiff.getFileName.toString.split("-")
      (date, tileId)
    }.mapValues(bands => bands.sortBy { singleBandTiff =>
      val bandName = singleBandTiff.getFileName.toString.split("-")(2).takeWhile(_ != '.')
      bandNames.indexOf(bandName)
    })

    bandsGrouped foreach { case ((date, tileId), bandTiffs) =>
      val vrtFile = collectingDir.resolve("combined.vrt")
      val outputFile = assembledDir.resolve(s"$date-$tileId.tif")

      logger.debug(s"combining $bandTiffs to $outputFile")

      val gdalbuildvrt = Seq("gdalbuildvrt", "-q", "-separate", vrtFile.toString) ++ bandTiffs.map(_.toString)
      if (gdalbuildvrt.! != 0) {
        throw new IllegalStateException(s"${gdalbuildvrt mkString " "} returned non-zero exit status") // TODO: better error handling; also: gdalbuildvrt silently skips nonexistent files
      }

      try {
        val gdal_translate = Seq("gdal_translate", vrtFile.toString, outputFile.toString)
        if (gdal_translate.! != 0) {
          throw new IllegalStateException(s"${gdal_translate mkString " "} returned non-zero exit status") // TODO: better error handling
        }
      } finally Files.delete(vrtFile)
    }
  }
}
