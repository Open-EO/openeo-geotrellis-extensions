package org.openeo

import _root_.io.circe.{Decoder, Encoder, HCursor, Json}
import _root_.io.circe.Decoder.Result
import cats.syntax.either._
import net.jodah.failsafe.event.ExecutionAttemptedEvent
import net.jodah.failsafe.function.{CheckedConsumer, CheckedSupplier}
import net.jodah.failsafe.{Failsafe, FailsafeExecutor, RetryPolicy}
import org.slf4j.Logger
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, ListObjectsV2Request, ObjectIdentifier}

import java.io.FileOutputStream
import java.net.SocketTimeoutException
import java.nio.file.Path
import java.time.temporal.ChronoUnit.SECONDS
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

package object geotrellissentinelhub {
  def withRetries[R](context: String)(fn: => R)(implicit logger: Logger): R = {
    val retryPolicy = {
      val tooManyRequests: SentinelHubException => Boolean = e => e.statusCode == 429
      val serverError: SentinelHubException => Boolean = e => e.statusCode >= 500
      val emptyRequestBody: SentinelHubException => Boolean =
        e => e.statusCode == 400 && e.responseBody.contains("Request body should be non-empty.")

      new RetryPolicy[R]
        .handle(classOf[SocketTimeoutException], classOf[CirceException])
        .handleIf(tooManyRequests.asJava)
        .handleIf(serverError.asJava)
        .handleIf(emptyRequestBody.asJava)
        .withBackoff(1, 1000, SECONDS) // should not reach maxDelay because of maxAttempts 5
        .withJitter(0.5)
        .withMaxAttempts(5)
        .onFailedAttempt(new CheckedConsumer[ExecutionAttemptedEvent[R]] {
          override def accept(e: ExecutionAttemptedEvent[R]): Unit = {
            logger.warn(s"Attempt ${e.getAttemptCount} failed: $context -> ${e.getLastFailure.getMessage}")
          }
        })
    }

    val failsafe: FailsafeExecutor[R] = Failsafe
      .`with`(retryPolicy)

    failsafe.get(new CheckedSupplier[R] {
      override def get(): R = fn
    })
  }

  /**
   * Maps dataFilters (Process/Batch Processing API) to equivalent query properties (Catalog API).
   */
  private[geotrellissentinelhub] def toQueryProperties(dataFilters: util.Map[String, Any]):
  collection.Map[String, String] =
    dataFilters.asScala
      .map {
        case ("orbitDirection", value: String) => "sat:orbit_state" -> value
        case ("acquisitionMode", value: String) => "sar:instrument_mode" -> value
        case (property, value: String) => property -> value
        case (property, _) => throw new IllegalArgumentException(s"unsupported metadata property $property")
      }

  // TODO: put it in a central place
  implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }

  implicit object zonedDateTimeEncoder extends Encoder[ZonedDateTime] {
    override def apply(date: ZonedDateTime): Json = Json.fromLong(date.toInstant.toEpochMilli)
  }

  implicit object zonedDateTimeDecoder extends Decoder[ZonedDateTime] {
    override def apply(c: HCursor): Result[ZonedDateTime] = c.as[Long]
      .map(millis => ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
  }

  object S3 {
    def withClient[R](f: S3Client => R): R = {
      val s3Client = S3Client.builder()
        .build()

      try f(s3Client)
      finally s3Client.close()
    }

    def download(s3Client: S3Client, bucketName: String, key: String, outputFile: Path): Unit = {
      val getObjectRequest = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()

      val out = new FileOutputStream(outputFile.toFile)

      try s3Client.getObject(getObjectRequest, ResponseTransformer.toOutputStream[GetObjectResponse](out))
      finally out.close()
    }

    def listObjectIdentifiers(s3Client: S3Client, bucketName: String, prefix: String): Iterable[ObjectIdentifier] = {
      val listObjectsResponse = s3Client.listObjectsV2Paginator(
        ListObjectsV2Request.builder()
          .bucket(bucketName)
          .prefix(prefix)
          .build()
      )

      listObjectsResponse.contents().asScala
        .map(obj => ObjectIdentifier.builder().key(obj.key()).build())
    }
  }
}
