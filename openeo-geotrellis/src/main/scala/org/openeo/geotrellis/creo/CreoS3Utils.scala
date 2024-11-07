package org.openeo.geotrellis.creo

import geotrellis.store.s3.AmazonS3URI
import org.apache.commons.io.FileUtils
import org.openeo.geotrelliss3.S3Utils
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client, S3Configuration}

import java.net.URI
import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.time.Duration
import scala.collection.JavaConverters._
import scala.collection.immutable.Iterable
import scala.util.control.Breaks.{break, breakable}

object CreoS3Utils {
  private val logger = LoggerFactory.getLogger(getClass)

  private val cloudFerroRegion: Region = Region.of("RegionOne")

  def getAsyncClient(): S3AsyncClient = {
    S3AsyncClient.crtBuilder()
      .credentialsProvider(credentialsProvider)
      //.serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .region(cloudFerroRegion).forcePathStyle(true).endpointOverride(URI.create(sys.env("SWIFT_URL")))
      .build();
  }

  def getCreoS3Client(): S3Client = {

    val clientBuilder = S3Client.builder()
      .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .overrideConfiguration(overrideConfig).forcePathStyle(true)
      .region(cloudFerroRegion)

    clientBuilder.endpointOverride(URI.create(sys.env("SWIFT_URL"))).credentialsProvider(credentialsProvider).build()
  }

  private def credentialsProvider = {
    val swiftAccess = sys.env.getOrElse("SWIFT_ACCESS_KEY_ID", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    val swiftSecretAccess = sys.env.getOrElse("SWIFT_SECRET_ACCESS_KEY", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(swiftAccess, swiftSecretAccess))
    credentialsProvider
  }

  private def overrideConfig = {
    val retryCondition =
      OrRetryCondition.create(
        RetryCondition.defaultRetryCondition(),
        RetryOnErrorCodeCondition.create("RequestTimeout")
      )
    val backoffStrategy =
      FullJitterBackoffStrategy.builder()
        .baseDelay(Duration.ofMillis(50))
        .maxBackoffTime(Duration.ofMillis(15))
        .build()
    val retryPolicy =
      RetryPolicy.defaultRetryPolicy()
        .toBuilder()
        .retryCondition(retryCondition)
        .backoffStrategy(backoffStrategy)
        .build()
    val overrideConfig =
      ClientOverrideConfiguration.builder()
        .retryPolicy(retryPolicy)
        .build()
    overrideConfig
  }

  //noinspection ScalaWeakerAccess
  def deleteCreoSubFolder(bucket_name: String, subfolder: String): Unit = {
    val s3Client = getCreoS3Client()
    S3Utils.deleteSubFolder(s3Client, bucket_name, subfolder)
  }

  def isS3(path: String): Boolean = {
    path.toLowerCase.startsWith("s3:/")
  }

  private def toAmazonS3URI(path: String): AmazonS3URI = {
    val correctS3Path = path.replaceFirst("(?i)s3:/(?!/)", "s3://")
    new AmazonS3URI(correctS3Path)
  }

  // In the following functions an asset path could be a local path or an S3 path.

  /**
   * S3 does not have folders, so we interpret the path as a prefix.
   */
  def assetDeleteFolders(paths: Iterable[String]): Unit = {
    for (path <- paths) {
      if (isS3(path)) {
        val s3Uri = toAmazonS3URI(path)
        deleteCreoSubFolder(s3Uri.getBucket, s3Uri.getKey)
      } else {
        val p = Path.of(path)
        if (Files.exists(p)) {
          if (Files.isDirectory(p)) {
            FileUtils.deleteDirectory(p.toFile)
          } else {
            throw new IllegalArgumentException(f"Can only delete directory here: $path")
          }
        }
      }
    }
  }

  def assetDelete(path: String): Unit = {
    if (isS3(path)) {
      val s3Uri = toAmazonS3URI(path)
      val keys = Seq(path)
      val deleteObjectsRequest = DeleteObjectsRequest.builder
        .bucket(s3Uri.getBucket)
        .delete(Delete.builder.objects(keys.map(key => ObjectIdentifier.builder.key(key).build).asJavaCollection).build)
        .build
      getCreoS3Client().deleteObjects(deleteObjectsRequest)
    } else {
      val p = Path.of(path)
      if (Files.isDirectory(p)) {
        throw new IllegalArgumentException(f"Cannot delete directory like this: $path")
      } else {
        Files.deleteIfExists(p)
      }
    }
  }

  def asseetPathListDirectChildren(path: String): Set[String] = {
    if (isS3(path)) {
      val s3Uri = toAmazonS3URI(path)
      val listObjectsRequest = ListObjectsRequest.builder
        .bucket(s3Uri.getBucket)
        .prefix(s3Uri.getKey)
        .build
      val listObjectsResponse = getCreoS3Client().listObjects(listObjectsRequest)
      listObjectsResponse.contents.asScala.map(o => f"s3://${s3Uri.getBucket}/${o.key}").toSet
    } else {
      Files.list(Path.of(path)).toArray.map(_.toString).toSet
    }
  }

  def assetExists(path: String): Boolean = {
    if (isS3(path)) {
      try {
        // https://stackoverflow.com/a/56038360/1448736
        val s3Uri = toAmazonS3URI(path)
        val objectRequest = HeadObjectRequest.builder
          .bucket(s3Uri.getBucket)
          .key(s3Uri.getKey)
          .build
        getCreoS3Client().headObject(objectRequest)
        true
      } catch {
        case _: NoSuchKeyException => false
      }
    } else {
      Files.exists(Path.of(path))
    }
  }

  def copyAsset(pathOrigin: String, pathDestination: String): Unit = {
    if (isS3(pathOrigin) && isS3(pathDestination)) {
      val s3UriOrigin = toAmazonS3URI(pathOrigin)
      val s3UriDestination = toAmazonS3URI(pathDestination)
      val copyRequest = CopyObjectRequest.builder
        .sourceBucket(s3UriOrigin.getBucket)
        .sourceKey(s3UriOrigin.getKey)
        .destinationBucket(s3UriDestination.getBucket)
        .destinationKey(s3UriDestination.getKey)
        .build
      getCreoS3Client().copyObject(copyRequest)
    } else if (!isS3(pathOrigin) && !isS3(pathDestination)) {
      Files.copy(Path.of(pathOrigin), Path.of(pathDestination))
    } else if (!isS3(pathOrigin) && isS3(pathDestination)) {
      uploadToS3(Path.of(pathOrigin), pathDestination)
    } else if (isS3(pathOrigin) && !isS3(pathDestination)) {
      // TODO: Download
      throw new IllegalArgumentException(f"S3->local not supported here yet ($pathOrigin, $pathDestination)")
    } else {
      throw new IllegalArgumentException(f"Should be impossible to get here ($pathOrigin, $pathDestination)")
    }
  }

  def moveAsset(pathOrigin: String, pathDestination: String): Unit = {
    // This could be optimized using move when on file system.
    copyAsset(pathOrigin, pathDestination)
    assetDelete(pathOrigin)
  }

  def waitTillPathAvailable(path: Path): Unit = {
    var retry = 0
    val maxTries = 20
    while (!assetExists(path.toString)) {
      if (retry < maxTries) {
        retry += 1
        val seconds = 5
        logger.info(f"Waiting for path to be available. Try $retry/$maxTries (sleep:$seconds seconds): $path")
        Thread.sleep(seconds * 1000)
      } else {
        logger.warn(f"Path is not available after $maxTries tries: $path")
        // Throw error instead?
        return
      }
    }
  }

  def moveOverwriteWithRetries(oldPath: String, newPath: String): Unit = {
    var try_count = 1
    breakable {
      while (true) {
        try {
          if (assetExists(newPath)) {
            // It might be a partial result of a previous failing task.
            logger.info(f"Will replace $newPath. (try $try_count)")
            assetDelete(newPath)
          }
          moveAsset(oldPath, newPath)
          break
        } catch {
          case e: Exception =>
            // Here if another executor wrote the file between the delete and the move statement.
            try_count += 1
            if (try_count > 5) {
              throw e
            }
        }
      }
    }
  }

  def uploadToS3(localFile: Path, s3Path: String) = {
    val s3Uri = toAmazonS3URI(s3Path)
    val objectRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build

    getCreoS3Client().putObject(objectRequest, RequestBody.fromFile(localFile))
    s3Path
  }
}
