package org.openeo.geotrellissentinelhub

import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.services.s3.model._

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

object S3Service {
  private val logger = LoggerFactory.getLogger(classOf[S3Service])

  class StacMetadataUnavailableException extends IllegalStateException
  class UnknownFolderException extends IllegalArgumentException
}

class S3Service {
  import S3Service._

  def delete_batch_process_results(bucket_name: String, subfolder: String): Unit = S3.withClient { s3Client =>
    val objectIdentifiers = S3.listObjectIdentifiers(s3Client, bucket_name, prefix = subfolder).iterator

    if (objectIdentifiers.isEmpty)
      throw new UnknownFolderException

    val maxBatchSize = 1000 // as specified in the docs
    val batches = objectIdentifiers.sliding(size = maxBatchSize, step = maxBatchSize)

    for (batch <- batches) {
      val deleteObjectsRequest = DeleteObjectsRequest.builder()
        .bucket(bucket_name)
        .delete(Delete.builder().objects(batch.asJava).build())
        .build()

      s3Client.deleteObjects(deleteObjectsRequest)
    }
  }

  // previously batch processes wrote to s3://<bucket_name>/<batch_request_id> while the new ones write to
  // s3://<bucket_name>/<request_group_id> because they comprise multiple batch process requests
  def download_stac_data(bucket_name: String, request_group_id: String, target_dir: String,
                         metadata_poll_interval_secs: Int = 10,
                         max_metadata_delay_secs: Int = 600): Unit = S3.withClient { s3Client =>
    def keys: Seq[String] = S3.listObjectIdentifiers(s3Client, bucket_name, prefix = request_group_id).iterator
      .map(_.key())
      .toSeq

    def download(key: String): Unit = {
      val fileName = key.split("/").last
      val outputFile = Paths.get(target_dir, fileName)

      S3.download(s3Client, bucket_name, key, outputFile)
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

  def uploadRecursively(root: Path, bucket_name: String): String = S3.withClient { s3Client =>
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

  private def uploadText(text: String, bucketName: String, key: String): Unit = S3.withClient { s3Client =>
    val putObjectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.putObject(putObjectRequest, RequestBody.fromString(text))
  }

  private def downloadText(bucketName: String, key: String): String = S3.withClient { s3Client =>
    val getObjectRequest = GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes[GetObjectResponse]).asUtf8String()
  }
}
