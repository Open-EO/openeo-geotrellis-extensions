package org.openeo.geotrellis.creo

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.store.s3.AmazonS3URI
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.openeo.geotrelliss3.S3Utils
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, AwsCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client, S3Configuration}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest

import java.io.InputStream
import java.net.URI
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._
import scala.collection.immutable.Iterable
import scala.util.control.Breaks.{break, breakable}

object CreoS3Utils {
  private val logger = LoggerFactory.getLogger(getClass)
  private val objectMapper = new ObjectMapper()
  private val proxyS3ClientCache = new ConcurrentHashMap[String, S3Client]()
  private val proxyS3AsyncClientCache = new ConcurrentHashMap[String, S3AsyncClient]()
  private val proxySettingsCache = new ConcurrentHashMap[String, ProxySettings]()

  private val cloudFerroRegion: Region = Region.of("RegionOne")
  private val eodataRegion: Region = Region.of("eodata")

  // AWS_S3_ENDPOINT may be given without scheme (GDAL convention); AWS_HTTPS then decides the scheme.
  private def eodataEndpoint: URI = {
    val endpoint = sys.env.getOrElse("AWS_S3_ENDPOINT", "https://eodata.dataspace.copernicus.eu")
    if (endpoint.contains("://")) URI.create(endpoint)
    else {
      val https = sys.env.getOrElse("AWS_HTTPS", "YES").equalsIgnoreCase("YES")
      URI.create((if (https) "https://" else "http://") + endpoint)
    }
  }

  // Async client for large (multipart) uploads. Chooses the proxy client or a region-specific client like getS3Client.
  def getAsyncClient(uri: AmazonS3URI): S3AsyncClient = {
    val proxy = getProxyS3AsyncClient(uri.getBucket)
    if (proxy != null) proxy
    else getCreoS3AsyncClient(if (uri.getBucket == "eodata") eodataRegion else cloudFerroRegion)
  }

  private val creoS3AsyncClientCache = new ConcurrentHashMap[Region, S3AsyncClient]()

  private def getCreoS3AsyncClient(region: Region): S3AsyncClient =
    creoS3AsyncClientCache.computeIfAbsent(region, (buildCreoS3AsyncClient _).asJava)

  private def buildCreoS3AsyncClient(region: Region): S3AsyncClient = {
    val (endpointURI, credProvider) = creoEndpointAndCredentials(region)
    // Might log this warning:
    // "
    // The provided DefaultS3AsyncClient is not an instance of S3CrtAsyncClient,
    // and thus multipart upload/download feature is not enabled and resumable file upload is not supported.
    // To benefit from maximum throughput, consider using S3AsyncClient.crtBuilder().build() instead.
    // "
    S3AsyncClient.builder() // used to be crtBuilder, but then gave error
      .credentialsProvider(credProvider)
      .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .overrideConfiguration(overrideConfig)
      .forcePathStyle(true)
      .region(region)
      .endpointOverride(endpointURI)
      .build()
  }

  // Return a Client that goes through the S3 proxy if S3 proxy is available for the execution environment and
  // if the job received a bucket configuration.
  // Results are cached per bucket name; failures (null) are not cached and will be retried on the next call.
  def getProxyS3Client(bucketName: String): S3Client = {
    if (bucketName == null || bucketName.isEmpty) return null
    proxyS3ClientCache.computeIfAbsent(bucketName, (buildProxyS3Client _).asJava)
  }

  // Async variant of getProxyS3Client, same caching behavior.
  def getProxyS3AsyncClient(bucketName: String): S3AsyncClient = {
    if (bucketName == null || bucketName.isEmpty) return null
    proxyS3AsyncClientCache.computeIfAbsent(bucketName, (buildProxyS3AsyncClient _).asJava)
  }

  private def buildProxyS3Client(bucketName: String): S3Client = {
    val settings = getProxySettings(bucketName)
    if (settings == null) return null
    try {
      S3Client.builder()
        .credentialsProvider(settings.credentials)
        .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
        .overrideConfiguration(overrideConfig)
        .forcePathStyle(true)
        .region(settings.region)
        .endpointOverride(settings.s3Endpoint)
        .build()
    } catch {
      case e: Exception =>
        logger.warn(s"Cannot build proxy S3 client for bucket $bucketName: ${e.getMessage}", e)
        null
    }
  }

  // Reads an integer from an environment variable; warns and returns the default if missing/invalid/below min.
  private[creo] def envInt(name: String, default: Int, min: Int): Int = {
    sys.env.get(name).map(_.trim).filter(_.nonEmpty) match {
      case None => default
      case Some(value) =>
        value.toIntOption match {
          case Some(i) if i >= min => i
          case _ =>
            logger.warn(s"Ignoring invalid value '$value' for $name (expected integer >= $min), using $default")
            default
        }
    }
  }

  // Same as envInt, but the value is in MiB and the result is in bytes.
  private[creo] def envMiB(name: String, defaultMiB: Int, minMiB: Int): Long =
    envInt(name, defaultMiB, minMiB) * 1024L * 1024L

  private def buildProxyS3AsyncClient(bucketName: String): S3AsyncClient = {
    val settings = getProxySettings(bucketName)
    if (settings == null) return null
    try {
      val multipartConfig = MultipartConfiguration.builder()
        .thresholdInBytes(envMiB("S3PROXY_MULTIPART_THRESHOLD_MB", 100, 1))
        .minimumPartSizeInBytes(envMiB("S3PROXY_MULTIPART_PART_SIZE_MB", 32, 5)) // S3 minimum part size is 5 MiB
        .build()
      val maxConnections = envInt("S3PROXY_MAX_CONNECTIONS", 10, 1)
      S3AsyncClient.builder()
        .credentialsProvider(settings.credentials)
        .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
        .overrideConfiguration(overrideConfig)
        .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(maxConnections))
        .multipartEnabled(true)
        .multipartConfiguration(multipartConfig)
        .forcePathStyle(true)
        .region(settings.region)
        .endpointOverride(settings.s3Endpoint)
        .build()
    } catch {
      case e: Exception =>
        logger.warn(s"Cannot build proxy S3 async client for bucket $bucketName: ${e.getMessage}", e)
        null
    }
  }

  private case class ProxySettings(region: Region, s3Endpoint: URI, credentials: AwsCredentialsProvider)

  // Shared by the sync and async proxy clients, so both use the same STS credentials provider per bucket.
  // Failures (null) are not cached.
  private def getProxySettings(bucketName: String): ProxySettings =
    proxySettingsCache.computeIfAbsent(bucketName, (resolveProxySettings _).asJava)

  private def resolveProxySettings(bucketName: String): ProxySettings = {
    val tokenFile = Path.of(sys.env.getOrElse("OPENEO_WEB_IDENTITY_TOKEN_FILE", "/opt/job_config/token"))
    if (!Files.isRegularFile(tokenFile) || !Files.isReadable(tokenFile)) {
      logger.info(s"Skip proxy S3 client for bucket $bucketName: web identity token file is not readable: $tokenFile")
      return null
    }

    val bucketConfigFile = Path.of(sys.env.getOrElse("OPENEO_BUCKET_CONFIG_FILE", "/opt/job_config/bucket_config.json"))
    if (!Files.isRegularFile(bucketConfigFile) || !Files.isReadable(bucketConfigFile)) {
      logger.info(s"Skip proxy S3 client for bucket $bucketName: bucket config file is not readable: $bucketConfigFile")
      return null
    }

    val bucketConfig = readProxyBucketConfig(bucketName, bucketConfigFile)
    if (bucketConfig == null) {
      return null
    }

    val stsEndpoint = getRequiredUri("S3PROXY_STS_ENDPOINT_URL")
    if (stsEndpoint == null) {
      return null
    }

    val s3Endpoint = getRequiredUri("S3PROXY_S3_ENDPOINT_URL")
    if (s3Endpoint == null) {
      return null
    }

    try {
      val region = Region.of(bucketConfig.region)
      val stsClient = StsClient.builder()
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .region(region)
        .endpointOverride(stsEndpoint)
        .build()

      // Use the STS-backed variant so the web identity exchange can target a custom endpoint.
      val credentialsProvider = StsWebIdentityTokenFileCredentialsProvider.builder()
        .stsClient(stsClient)
        .roleArn(bucketConfig.roleArn)
        .roleSessionName(proxyRoleSessionName(bucketName))
        .webIdentityTokenFile(tokenFile)
        .build()

      ProxySettings(region, s3Endpoint, credentialsProvider)
    } catch {
      case e: Exception =>
        logger.warn(s"Cannot build proxy S3 client for bucket $bucketName: ${e.getMessage}", e)
        null
    }
  }

  def getS3Client(uri: AmazonS3URI): S3Client = {
    val proxy = getProxyS3Client(uri.getBucket)
    if (proxy != null) proxy
    else getCreoS3Client(if (uri.getBucket == "eodata") eodataRegion else cloudFerroRegion)
  }

  /** Opens the S3 object. Returns null if the key does not exist. Caller must close the stream. */
  def readFromS3(s3Uri: AmazonS3URI): InputStream = {
    val bucket = s3Uri.getBucket
    val key = Option(s3Uri.getKey).map(_.stripPrefix("/")).orNull
    try {
      getS3Client(s3Uri).getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
    } catch {
      case _: NoSuchKeyException =>
        logger.error(s"Error reading from S3: bucket: $bucket, NoSuchKeyException, key: $key")
        null
      case e: Throwable =>
        logger.error(s"Error reading from S3: bucket: $bucket, key: $key", e)
        throw e
    }
  }

  private def creoEndpointAndCredentials(region: Region): (URI, AwsCredentialsProvider) = {
    val endpointURI =
      if (region == eodataRegion) eodataEndpoint
      else if (region != cloudFerroRegion) this.getCFEndpoin(region)
      else URI.create(sys.env("SWIFT_URL"))
    val credProvider =
      if (region == eodataRegion) credentialsProviderEodata
      else if (region.toString.contains("waw")) credentialsProviderWAW
      else credentialsProvider
    (endpointURI, credProvider)
  }

  //Prefer using getS3Client with an S3 URI
  def getCreoS3Client(region: Region = cloudFerroRegion): S3Client = {
    val (endpointURI, credProvider) = creoEndpointAndCredentials(region)
    S3Client.builder()
      .credentialsProvider(credProvider)
      .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .overrideConfiguration(overrideConfig)
      .forcePathStyle(true)
      .region(region)
      .endpointOverride(endpointURI)
      .build()
  }

  //CloudFerro endpoints follow a structure based on region names.
  def getCFEndpoin(region: Region): URI = {
    URI.create(s"https://s3.${region}.cloudferro.com")
  }

  private def credentialsProvider = {
    val swiftAccess = sys.env.getOrElse("SWIFT_ACCESS_KEY_ID", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    val swiftSecretAccess = sys.env.getOrElse("SWIFT_SECRET_ACCESS_KEY", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(swiftAccess, swiftSecretAccess))
    credentialsProvider
  }

  private def credentialsProviderWAW = {
    //EC2 credentials in CFC are usable across regions
    val s3AccessKeyId = sys.env.getOrElse("CF_ACCESS_KEY_ID", "")
    val s3SecretKey = sys.env.getOrElse("CF_SECRET_ACCESS_KEY", "")
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKeyId, s3SecretKey))
    credentialsProvider
  }

  private def credentialsProviderEodata = {
    val accessKeyId = sys.env.getOrElse("AWS_ACCESS_KEY_ID", "")
    val secretKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "")
    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretKey))
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

  private case class ProxyBucketConfig(region: String, roleArn: String)

  private def readProxyBucketConfig(bucketName: String, bucketConfigFile: Path): ProxyBucketConfig = {
    try {
      val bucketConfigRoot = objectMapper.readTree(bucketConfigFile.toFile)
      if (bucketConfigRoot == null || !bucketConfigRoot.isObject) {
        logger.info(s"Skip proxy S3 client for bucket $bucketName: bucket config file does not contain a JSON object: $bucketConfigFile")
        return null
      }

      val bucketNode = bucketConfigRoot.path(bucketName)
      if (bucketNode.isMissingNode || !bucketNode.isObject) {
        logger.info(s"Skip proxy S3 client for bucket $bucketName: bucket config file has no object entry for this bucket: $bucketConfigFile")
        return null
      }

      val region = Option(bucketNode.path("region").asText(null)).map(_.trim).filter(_.nonEmpty).orNull
      if (region == null) {
        //Warn because this should not happen
        logger.warn(s"Cannot build proxy S3 client for bucket $bucketName: bucket config entry has no region: $bucketConfigFile")
        return null
      }

      val roleArn = Option(bucketNode.path("role_arn").asText(null)).map(_.trim).filter(_.nonEmpty).orNull
      if (roleArn == null) {
        //Warn because this should not happen
        logger.warn(s"Cannot build proxy S3 client for bucket $bucketName: bucket config entry has no role_arn: $bucketConfigFile")
        return null
      }

      ProxyBucketConfig(region, roleArn)
    } catch {
      case e: Exception =>
        logger.warn(s"Cannot build proxy S3 client for bucket $bucketName: failed to read bucket config file $bucketConfigFile: ${e.getMessage}", e)
        null
    }
  }

  private def getRequiredUri(envName: String): URI = {
    val endpointValue = sys.env.get(envName).map(_.trim).filter(_.nonEmpty).orNull
    if (endpointValue == null) {
      logger.warn(s"Cannot build proxy S3 client: environment variable $envName is not set.")
      return null
    }

    try {
      URI.create(endpointValue)
    } catch {
      case e: IllegalArgumentException =>
        logger.warn(s"Cannot build proxy S3 client: environment variable $envName does not contain a valid URI: $endpointValue", e)
        null
    }
  }

  private def proxyRoleSessionName(bucketName: String): String = {
    val podName = sys.env.getOrElse("SPARK_EXECUTOR_POD_NAME", "unknown-sparkappid")
    val sparkAppId = podName.split("-").slice(0, 2).mkString("-")
    val executorId = sys.env.getOrElse("SPARK_EXECUTOR_ID", "00")

    s"$sparkAppId-$executorId-$bucketName".take(64)
  }

  //noinspection ScalaWeakerAccess
  def deleteCreoSubFolder(bucket_name: String, subfolder: String): Unit = {
    val s3Client = getS3Client(new AmazonS3URI(s"s3://$bucket_name"))
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
            val files_in_directory = FileUtils
              .listFilesAndDirs(p.toFile, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
              .asScala
              .filter(_.isFile)
            // Ideally, the directory should be empty.
            if (files_in_directory.nonEmpty) logger.warn(f"Deleting files_in_directory: $files_in_directory")
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
      val keys = Seq(s3Uri.getKey)
      val deleteObjectsRequest = DeleteObjectsRequest.builder
        .bucket(s3Uri.getBucket)
        .delete(Delete.builder.objects(keys.map(key => ObjectIdentifier.builder.key(key).build).asJavaCollection).build)
        .build
      getS3Client(s3Uri).deleteObjects(deleteObjectsRequest)
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
      val listObjectsResponse = getS3Client(s3Uri).listObjects(listObjectsRequest)
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
        getS3Client(s3Uri).headObject(objectRequest)
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
      val originClient = getS3Client(s3UriOrigin)
      val destClient = getS3Client(s3UriDestination)
      if (originClient.serviceClientConfiguration().region() == destClient.serviceClientConfiguration().region() ) {
        throw new IllegalArgumentException(f"S3->S3 cross region not supported yet ($pathOrigin, $pathDestination)")
      }
      originClient.copyObject(copyRequest)
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

  def waitTillPathAvailable(path: String): Unit = {
    var retry = 0
    val maxTries = 20
    while (!assetExists(path)) {
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
            logger.info("moveOverwriteWithRetries exception: " + e + f" (try $try_count)")
            try_count += 1
            if (try_count > 5) {
              throw e
            }
            Thread.sleep(1000)
        }
      }
    }
  }

  def uploadToS3(localFile: Path, s3Path: String): String = {
    val s3Uri = toAmazonS3URI(s3Path)
    val objectRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build

    getS3Client(s3Uri).putObject(objectRequest, RequestBody.fromFile(localFile))
    s3Path
  }


  def uploadToS3LargeFile(localPath: Path, s3Path: String): String = {
    val s3Uri = toAmazonS3URI(s3Path)

    val putRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build
    val uploadFileRequest = UploadFileRequest.builder
      .putObjectRequest(putRequest)
      .source(localPath)
      .build

    // Closing the transfer manager releases its internal thread pool; the (cached) async client stays open
    // because the SDK only closes clients it created itself.
    val transferManager = S3TransferManager.builder
      .s3Client(CreoS3Utils.getAsyncClient(s3Uri))
      .build
    try {
      transferManager.uploadFile(uploadFileRequest).completionFuture.join
    } finally {
      transferManager.close()
    }
    s3Path
  }

  def uploadToS3TryFirstWithStreaming(localPath: Path, s3Path: String): String = {
    // Streaming to s3 still has issues. This function often runs on executors, su SparkContext is not accessible.
    val try_streaming = sys.env.getOrElse("TRY_SWIFT_STREAMING", "true").toBoolean
    if (try_streaming) {
      // TODO: Streaming to s3 could cause error, so disable on prod for the moment
      // py4j.protocol.Py4JJavaError: An error occurred while calling z:org.openeo.geotrellis.netcdf.NetCDFRDDWriter.writeRasters.
      //: java.util.concurrent.CompletionException: software.amazon.awssdk.services.s3.model.S3Exception: null (Service: S3, Status Code: 400, Request ID: tx0000000000000613fcf8d-00655f6998-84eddc61-default)
      try {
        logger.info(f"uploadToS3TryFirstWithStreaming: Try to upload with streaming")
        uploadToS3LargeFile(localPath, s3Path)
      } catch {
        case e: Throwable =>
          logger.warn(f"uploadToS3TryFirstWithStreaming: Failed to upload with streaming, trying with regular upload: $e")
          uploadToS3(localPath, s3Path)
      }
    } else {
      uploadToS3(localPath, s3Path)
    }
  }

  def readFileAsString(path: String): String = {
    if (isS3(path)) {
      val s3Uri = toAmazonS3URI(path)
      val objectRequest = GetObjectRequest.builder
        .bucket(s3Uri.getBucket)
        .key(s3Uri.getKey)
        .build
      val response = getS3Client(s3Uri).getObject(objectRequest)
      val content = response.readAllBytes()
      new String(content)
    } else {
      Files.readString(Path.of(path))
    }
  }

  def writeStringToFile(path: String, content: String): Unit = {
    if (isS3(path)) {
      val s3Uri = toAmazonS3URI(path)
      val objectRequest = PutObjectRequest.builder
        .bucket(s3Uri.getBucket)
        .key(s3Uri.getKey)
        .build
      val tempFile = Files.createTempFile("tmp_writeStringToFile", ".txt")
      Files.writeString(tempFile, content)
      getS3Client(s3Uri).putObject(objectRequest, tempFile)
      Files.delete(tempFile)
    } else {
      Files.writeString(Path.of(path), content)
    }
  }
}
