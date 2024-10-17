package org.openeo

import _root_.geotrellis.raster._
import net.jodah.failsafe.event.{ExecutionAttemptedEvent, ExecutionCompletedEvent, ExecutionScheduledEvent}
import net.jodah.failsafe.{ExecutionContext, Failsafe, RetryPolicy => FailsafeRetryPolicy}
import org.slf4j.Logger
import scalaj.http.{HttpResponse, HttpStatusException}
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.core.retry.{RetryPolicy => AwsRetryPolicy}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.{SocketException, SocketTimeoutException, URI}
import java.nio.file.{Path, Paths}
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.concurrent.ConcurrentHashMap
import scala.compat.java8.FunctionConverters._


package object geotrellis {
  def logTiming[R](context: String)(action: => R)(implicit logger: Logger): R = {
    if (logger.isDebugEnabled()) {
      val start = Instant.now()
      logger.debug(s"$context: start")

      try
        action
      finally {
        val end = Instant.now()
        val elapsed = Duration.between(start, end)

        logger.debug(s"$context: end, elapsed $elapsed")
      }
    } else action
  }

  private val s3ClientCache = new ConcurrentHashMap[(Region, URI), S3Client]

  private[geotrellis] def s3Client(region: Region = null, endpoint: URI = null): S3Client =
    s3ClientCache.computeIfAbsent((region, endpoint), s3Client.asJava)

  private val s3Client: ((Region, URI)) => S3Client = { case (region, endpoint) =>
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
      AwsRetryPolicy.defaultRetryPolicy()
        .toBuilder()
        .retryCondition(retryCondition)
        .backoffStrategy(backoffStrategy)
        .build()
    val overrideConfig =
      ClientOverrideConfiguration.builder()
        //.putHeader("x-amz-request-payer", "requester")
        .retryPolicy(retryPolicy)
        .build()

    val clientBuilder = S3Client.builder()
      .overrideConfiguration(overrideConfig)
      .region(if(region != null) region else Region.EU_CENTRAL_1)

    val theClient = if(endpoint != null) {
      clientBuilder.serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build()).forcePathStyle(true).endpointOverride(endpoint).build()
    }else{
      clientBuilder.build()
    }

    theClient
  }

  private val bucketRegionCache = new ConcurrentHashMap[String, Region]

  private[geotrellis] def bucketRegion(bucketName: String): Region =
    bucketRegionCache.computeIfAbsent(bucketName, fetchBucketRegion.asJava)

  private val fetchBucketRegion: String => Region = bucketName => {
    // might not be allowed unless owner of the bucket (403)
    val getBucketLocationRequest = GetBucketLocationRequest.builder()
      .bucket(bucketName)
      .build()

    val regionName = s3Client()
      .getBucketLocation(getBucketLocationRequest)
      .locationConstraint()
      .toString

    Region.of(regionName)
  }

  def toSigned(cellType: CellType): CellType = {
    cellType match {
      case UByteCellType => ByteCellType
      case UByteConstantNoDataCellType => ByteConstantNoDataCellType
      case UByteUserDefinedNoDataCellType(noDataValue) => ByteUserDefinedNoDataCellType(noDataValue)
      case UShortCellType => ShortCellType
      case UShortConstantNoDataCellType => ShortConstantNoDataCellType
      case UShortUserDefinedNoDataCellType(noDataValue) => ShortUserDefinedNoDataCellType(noDataValue)
      case FloatConstantNoDataCellType => cellType
      case ShortConstantNoDataCellType => cellType
      case BitCellType => cellType
      case ByteConstantNoDataCellType => cellType
      case ByteCellType => cellType
      case ByteUserDefinedNoDataCellType(_) => cellType
      case ShortCellType => cellType
      case ShortUserDefinedNoDataCellType(_) => cellType
      case IntConstantNoDataCellType => cellType
      case IntCellType => cellType
      case IntUserDefinedNoDataCellType(_) => cellType
      case FloatCellType => cellType
      case FloatUserDefinedNoDataCellType(_) => cellType
      case DoubleConstantNoDataCellType => cellType
      case DoubleCellType => cellType
      case DoubleUserDefinedNoDataCellType(_) => cellType
      case _ => throw new IllegalArgumentException("Cannot convert to unsigned equivalent: '" + cellType.getClass.getName + "'.")
    }
  }

  /**
   * Inspired on 'Files.createTempFile', but does not create an empty file.
   * The default permissions of 'createTempFile' are a bit too strict too: 600, which is not accessible by other users.
   * This function could have default 664 for example.
   */
  def getTempFile(prefix: String, suffix: String): Path = {
    val prefixNonNull = if (prefix == null) "" else suffix
    val suffixNonNull = if (suffix == null) ".tmp" else suffix
    val tmpdirProp = sun.security.action.GetPropertyAction.privilegedGetProperty("java.io.tmpdir")
    val tmpdir = Paths.get(tmpdirProp)
    val random = new java.security.SecureRandom()

    def generatePath(prefix: String, suffix: String, dir: Path) = {
      val n = random.nextLong
      val s = prefix + java.lang.Long.toUnsignedString(n) + suffix
      val name = dir.getFileSystem.getPath(s)
      if (name.getParent != null) throw new IllegalArgumentException("Invalid prefix or suffix")
      else dir.resolve(name)
    }

    generatePath(prefixNonNull, suffixNonNull, tmpdir)
  }

  def sortableSourceName(sourceName: SourceName): String = sourceName match {
    case s: SourcePath => s.value
    case s: StringName => s.value
    case s => s.toString // ex: EmptyName
  }

  object TemporalResolution extends Enumeration {
    val seconds, days, undefined = Value
  }


  /**
   * taken from DefaultProcessApi
   * Specific to retry HttpResponse, without throwError logic.
   */
  def withRetryAfterRetries[R](context: String)(httpResponseCallback: => HttpResponse[R])(implicit logger: Logger): HttpResponse[R] = {
    val retryableResult: HttpResponse[R] => Boolean = r => r.is5xx
    val retryableException: Throwable => Boolean = {
      case _: HttpStatusException =>
        logger.warn("withRetryAfterRetries does not support .throwError logic.")
        false
      case _: SocketTimeoutException => true
      case _: SocketException => true // Got this when terminating test server in the middle of a request
      case e => logger.error(s"Not attempting to retry unrecoverable error in context: '$context' " + e.getMessage); false
    }

    val shakyConnectionRetryPolicy = new FailsafeRetryPolicy[HttpResponse[R]]()
      .handleResultIf(retryableResult.asJava)
      .handleIf(retryableException.asJava)
      .withBackoff(1, 1000, ChronoUnit.SECONDS) // should not reach maxDelay because of maxAttempts 5
      .withJitter(0.5)
      .withMaxAttempts(5)
      .onFailedAttempt((attempt: ExecutionAttemptedEvent[HttpResponse[R]]) => {
        val msg = if (attempt.getLastFailure != null) {
          attempt.getLastFailure
        } else if (attempt.getLastResult != null) {
          "result code: " + attempt.getLastResult.code
        } else ""
        logger.warn(s"Attempt ${attempt.getAttemptCount} failed in context: '$context' " + msg)
      })
      .onFailure((execution: ExecutionCompletedEvent[HttpResponse[R]]) => {
        val e = execution.getFailure
        logger.error(s"Failed after ${execution.getAttemptCount} attempt(s) in context: '$context'" + e.getMessage)
      })

    val isRateLimitingResponse: (HttpResponse[R], Throwable) => Boolean =
      (response, _ /* ignore exceptions, those are handled in shakyConnectionRetryPolicy */) => response.code == 429
    val rateLimitingRetryPolicy = new FailsafeRetryPolicy[HttpResponse[R]]()
      .handleIf(isRateLimitingResponse.asJava)
      .withMaxAttempts(5)
      .withDelay((lastResponse: HttpResponse[R], _: Throwable, executionContext: ExecutionContext) => {
        val retryAfterSecondsCalculated = (20 * math.pow(1.6, executionContext.getAttemptCount - 1)).toLong
        val retryAfterSeconds = lastResponse.headers
          .find { case (header, _) => header equalsIgnoreCase "retry-after" }
          .map { case (_, values) => values.head.toLong }
          .getOrElse(retryAfterSecondsCalculated)

        val duration = Duration.ofSeconds(retryAfterSeconds)
        logger.warn(s"Attempt ${executionContext.getAttemptCount} failed in context: '$context' Scheduled retry in $duration")
        duration
      })

    Failsafe
      .`with`(java.util.Arrays.asList(shakyConnectionRetryPolicy, rateLimitingRetryPolicy))
      .get(() => {
        httpResponseCallback
      })
  }
}
