package org.openeo.geotrellissentinelhub

import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.FileOutputStream
import java.time.{LocalDate, ZonedDateTime, ZoneId}
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

object S3Service {
  class StacMetadataUnavailableException extends IllegalStateException
  class UnknownFolderException extends IllegalArgumentException

  private implicit class RichPath(path: Path) {
    def /(other: String): Path = path.resolve(other)
  }
}

class S3Service {
  import S3Service._

  def delete_batch_process_results(bucket_name: String, subfolder: String): Unit = {
    val s3Client = S3Client.builder()
      .build()

    val objectIdentifiers = listObjectIdentifiers(s3Client, bucket_name, prefix = subfolder)

    if (objectIdentifiers.isEmpty)
      throw new UnknownFolderException

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
                         metadata_poll_interval_secs: Int = 10, max_metadata_delay_secs: Int = 600): Unit = {
    val s3Client = S3Client.builder()
      .build()

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

  def downloadBatchProcessResults(bucketName: String, subfolder: String, targetDir: Path, bandNames: Seq[String],
                                  onDownloaded: (String, ZonedDateTime, String) => Unit): Unit = {
    import java.time.format.DateTimeFormatter.BASIC_ISO_DATE

    // FIXME: write bands to separate files (avoid intermediary/temp geotiffs?)
    // TODO: move details elsewhere?
    require(bandNames.size == 1, "currently only single band is implemented")

    val s3Client = S3Client.builder()
      .build()

    val tiffKeys = listObjectIdentifiers(s3Client, bucketName, prefix = subfolder)
      .asScala
      .map(_.key())
      .filter(_.endsWith(".tif"))

    tiffKeys foreach { key =>
      val Array(_, tileId, fileName) = key.split("/")
      val date = fileName.split(raw"\.").head.drop(1)
      val bandName = bandNames.head

      val outputFile = targetDir / date / tileId / (bandName + ".tif")

      Files.createDirectories(outputFile.getParent)
      download(s3Client, bucketName, key, outputFile)

      onDownloaded(tileId, LocalDate.parse(date, BASIC_ISO_DATE).atStartOfDay(ZoneId.of("UTC")), bandName)
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
}
