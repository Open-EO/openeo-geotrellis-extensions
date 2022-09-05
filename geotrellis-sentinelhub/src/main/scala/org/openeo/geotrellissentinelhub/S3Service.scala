package org.openeo.geotrellissentinelhub

import org.slf4j.LoggerFactory
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

object S3Service {
  private val logger = LoggerFactory.getLogger(classOf[S3Service])

  class StacMetadataUnavailableException extends IllegalStateException
  class UnknownFolderException extends IllegalArgumentException

  private val bucketRegionCache = new ConcurrentHashMap[String, Region]
}

class S3Service {
  import S3Service._

  def delete_batch_process_results(bucket_name: String, subfolder: String): Unit = {
    val bucketRegion = this.bucketRegion(bucket_name)

    S3.withClient(bucketRegion) { s3Client =>
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
  }

  // previously batch processes wrote to s3://<bucket_name>/<batch_request_id> while the new ones write to
  // s3://<bucket_name>/<request_group_uuid> because they comprise multiple batch process requests
  def download_stac_data(bucket_name: String, request_group_uuid: String, target_dir: String,
                         metadata_poll_interval_secs: Int = 10,
                         max_metadata_delay_secs: Int = 600): Unit = S3.withClient { s3Client =>
    def keys: Seq[String] = S3.listObjectIdentifiers(s3Client, bucket_name, prefix = request_group_uuid).iterator
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

      logger.debug(s"STAC metadata in s3://$bucket_name with prefix $request_group_uuid: found ${stacMetadataKeys.size} of ${tiffKeys.size}")

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

  // TODO: reduce code duplication with openeo-geotrellis
  private def bucketRegion(bucketName: String): Region =
    bucketRegionCache.computeIfAbsent(bucketName, fetchBucketRegion.asJava)

  private val fetchBucketRegion: String => Region = bucketName => {
    val getBucketLocationRequest = GetBucketLocationRequest.builder()
      .bucket(bucketName)
      .build()

    val regionName = S3.withClient { s3Client =>
      s3Client
        .getBucketLocation(getBucketLocationRequest)
        .locationConstraint()
        .toString
    }

    Region.of(regionName)
  }
}
