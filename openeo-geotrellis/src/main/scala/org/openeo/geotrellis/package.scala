package org.openeo

import java.time.{Duration, Instant}
import org.slf4j.Logger
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest
import _root_.geotrellis.raster._

import java.net.URI
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
        .retryPolicy(retryPolicy)
        .build()

    val clientBuilder = S3Client.builder()
      .overrideConfiguration(overrideConfig)
      .region(if(region != null) region else Region.EU_CENTRAL_1)

    val theClient = if(endpoint != null) {
      clientBuilder.endpointOverride(endpoint).build()
    }else{
      clientBuilder.build()
    }

    theClient
  }

  private val bucketRegionCache = new ConcurrentHashMap[String, Region]

  private[geotrellis] def bucketRegion(bucketName: String): Region =
    bucketRegionCache.computeIfAbsent(bucketName, fetchBucketRegion.asJava)

  private val fetchBucketRegion: String => Region = bucketName => {
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
      case c: UByteUserDefinedNoDataCellType => ByteUserDefinedNoDataCellType(c.noDataValue)
      case UShortCellType => ShortCellType
      case UShortConstantNoDataCellType => ShortConstantNoDataCellType
      case c: UShortUserDefinedNoDataCellType => ShortUserDefinedNoDataCellType(c.noDataValue)
      case FloatConstantNoDataCellType => cellType
      case ShortConstantNoDataCellType => cellType
      case BitCellType => cellType
      case ByteConstantNoDataCellType => cellType
      case ByteCellType => cellType
      case _: ByteUserDefinedNoDataCellType => cellType
      case ShortCellType => cellType
      case _: ShortUserDefinedNoDataCellType => cellType
      case IntConstantNoDataCellType => cellType
      case IntCellType => cellType
      case _: IntUserDefinedNoDataCellType => cellType
      case FloatCellType => cellType
      case _: FloatUserDefinedNoDataCellType => cellType
      case DoubleConstantNoDataCellType => cellType
      case DoubleCellType => cellType
      case _: DoubleUserDefinedNoDataCellType => cellType
      case _ => throw new IllegalArgumentException("Can convert to unsigned equivalent: '" + cellType.getClass.getName + "'.")
    }
  }
}
