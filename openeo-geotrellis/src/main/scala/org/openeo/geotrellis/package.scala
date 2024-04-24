package org.openeo

import _root_.geotrellis.raster._
import org.slf4j.Logger
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}

import java.net.URI
import java.nio.file.{Path, Paths}
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
      RetryPolicy.defaultRetryPolicy()
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
   * Inspired on 'Files.createTempFile'
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
}
