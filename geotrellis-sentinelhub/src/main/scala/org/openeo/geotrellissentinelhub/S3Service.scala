package org.openeo.geotrellissentinelhub

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.FileOutputStream
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

object S3Service {
  private val logger = LoggerFactory.getLogger(classOf[S3Service])

  class StacMetadataUnavailableException extends IllegalStateException
  class UnknownFolderException extends IllegalArgumentException
}

class S3Service {
  import S3Service._

  private def withS3Client[R](f: S3Client => R): R = {
    val s3Client = S3Client.builder()
      .build()

    try f(s3Client)
    finally s3Client.close()
  }

  def delete_batch_process_results(bucket_name: String, subfolder: String): Unit = withS3Client { s3Client =>
    val objectIdentifiers = listObjectIdentifiers(s3Client, bucket_name, prefix = subfolder)

    if (objectIdentifiers.isEmpty)
      throw new UnknownFolderException

    @tailrec
    def deleteBatches(offset: Int): Unit = {
      val maxBatchSize = 1000 // as specified in the docs

      if (offset < objectIdentifiers.size()) {
        val batch = objectIdentifiers.subList(offset, (offset + maxBatchSize) min objectIdentifiers.size())

        val deleteObjectsRequest = DeleteObjectsRequest.builder()
          .bucket(bucket_name)
          .delete(Delete.builder().objects(batch).build())
          .build()

        s3Client.deleteObjects(deleteObjectsRequest)

        deleteBatches(offset + maxBatchSize)
      }
    }

    deleteBatches(offset = 0)
  }

  // previously batch processes wrote to s3://<bucket_name>/<batch_request_id> while the new ones write to
  // s3://<bucket_name>/<request_group_id> because they comprise multiple batch process requests
  def download_stac_data(bucket_name: String, request_group_id: String, target_dir: String,
                         metadata_poll_interval_secs: Int = 10,
                         max_metadata_delay_secs: Int = 600): Unit = withS3Client { s3Client =>
    def keys: Seq[String] = listObjectIdentifiers(s3Client, bucket_name, prefix = request_group_id)
      .asScala
      .map(_.key())

    def download(key: String): Unit = {
      val fileName = key.split("/").last
      val outputFile = Paths.get(target_dir, fileName)

      this.download(s3Client, bucket_name, key, outputFile)
    }

    val tiffKeys = keys
      .filter(_.endsWith(".tif"))

    tiffKeys foreach download

    val endMillis = System.currentTimeMillis() + max_metadata_delay_secs * 1000

    while (System.currentTimeMillis() < endMillis) {
      val stacMetadataKeys = keys
        .filter(_.endsWith("_metadata.json"))

      val allStacMetadataAvailable = stacMetadataKeys.size == tiffKeys.size

      if (allStacMetadataAvailable) {
        stacMetadataKeys foreach download
        return
      }

      TimeUnit.SECONDS.sleep(metadata_poll_interval_secs)
    }

    throw new StacMetadataUnavailableException
  }

  private def listObjectIdentifiers(s3Client: S3Client, bucketName: String,
                                    prefix: String): util.List[ObjectIdentifier] = {
    val listObjectsResponse = s3Client.listObjectsV2Paginator(
      ListObjectsV2Request.builder()
        .bucket(bucketName)
        .prefix(prefix)
        .build()
    )

    val toObjectIdentifier: S3Object => ObjectIdentifier =
      obj => ObjectIdentifier.builder().key(obj.key()).build()

    listObjectsResponse.contents().stream()
      .map[ObjectIdentifier](toObjectIdentifier.asJava)
      .collect(util.stream.Collectors.toList[ObjectIdentifier])
  }

  // = download multiband tiles and write them as single band tiles to the cache directory (a tree)
  def downloadBatchProcessResults(bucketName: String, subfolder: String, targetDir: Path, bandNames: Seq[String],
                                  onDownloaded: (String, ZonedDateTime, String) => Unit): Unit = withS3Client { s3Client =>
    import java.time.format.DateTimeFormatter.BASIC_ISO_DATE

    // TODO: move details elsewhere?
    val tiffKeys = listObjectIdentifiers(s3Client, bucketName, prefix = subfolder)
      .asScala
      .map(_.key())
      .filter(_.endsWith(".tif"))

    val parts = tiffKeys.map { key => // TODO: find a better name
      val Array(_, tileId, fileName) = key.split("/")
      val date = fileName.split(raw"\.").head.drop(1)

      key -> (tileId, date)
    }.toMap

    def subdirectory(date: String, tileId: String): Path = targetDir.resolve(date).resolve(tileId)

    // create all subdirectories with a minimum of effort
    for {
      (tileId, date) <- parts.values.toSet
    } Files.createDirectories(subdirectory(date, tileId))

    for ((key, (tileId, date)) <- parts) {
      // TODO: avoid intermediary/temp geotiff
      val tempMultibandFile = Files.createTempFile(subfolder, null)

      try {
        download(s3Client, bucketName, key, outputFile = tempMultibandFile)

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

  private def download(s3Client: S3Client, bucketName: String, key: String, outputFile: Path): Unit = {
    val getObjectRequest = GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    val out = new FileOutputStream(outputFile.toFile)

    try s3Client.getObject(getObjectRequest, ResponseTransformer.toOutputStream[GetObjectResponse](out))
    finally out.close()
  }

  def uploadRecursively(root: Path, bucket_name: String): String = withS3Client { s3Client =>
    val prefix = root.getFileName.toString

    val uploadFile: Path => Unit =
      path =>
        if (Files.isRegularFile(path)) {
          val key = root.getParent.relativize(path).toString

          val putObjectRequest = PutObjectRequest.builder()
            .bucket(bucket_name)
            .key(key)
            .build()

          s3Client.putObject(putObjectRequest, path)

          logger.debug(s"uploaded $path to s3://$bucket_name/$key")
        }

    Files.walk(root).forEach(uploadFile.asJava)

    prefix
  }

  // TODO: do these two belong here?
  def saveBatchProcessContext(batchProcessContext: BatchProcessContext, bucketName: String, subfolder: String): Unit = {
    val json = batchProcessContext.toJson
    uploadText(json, bucketName, batchProcessContextKey(subfolder))
  }

  def loadBatchProcessContext(bucketName: String, subfolder: String): BatchProcessContext = {
    val json = downloadText(bucketName, batchProcessContextKey(subfolder))
    BatchProcessContext.fromJson(json)
  }

  private def batchProcessContextKey(subfolder: String): String = s"$subfolder/request_context.json"

  private def uploadText(text: String, bucketName: String, key: String): Unit = withS3Client { s3Client =>
    val putObjectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.putObject(putObjectRequest, RequestBody.fromString(text))
  }

  private def downloadText(bucketName: String, key: String): String = withS3Client { s3Client =>
    val getObjectRequest = GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes[GetObjectResponse]).asUtf8String()
  }
}
