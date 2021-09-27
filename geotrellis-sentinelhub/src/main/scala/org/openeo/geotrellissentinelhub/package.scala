package org.openeo

import _root_.io.circe.{Decoder, Encoder, HCursor, Json}
import _root_.io.circe.Decoder.Result
import cats.syntax.either._
import org.slf4j.Logger
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, ListObjectsV2Request, ObjectIdentifier}

import java.io.FileOutputStream
import java.lang.Math.{pow, random}
import java.net.SocketTimeoutException
import java.nio.file.Path
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util
import scala.annotation.tailrec
import scala.collection.JavaConverters._

package object geotrellissentinelhub {
  def withRetries[T](attempts: Int, context: String)(fn: => T)(implicit logger: Logger): T = {
    @tailrec
    def attempt(i: Int): T = {
      def retryable(e: Exception): Boolean = {
        logger.warn(s"Attempt $i failed: $context -> ${e.getMessage}")

        i < attempts && (e match {
          case s: SentinelHubException if s.statusCode == 429 || s.statusCode >= 500
            || (s.statusCode == 400 && s.responseBody.contains("Request body should be non-empty.")) => true
          case _: SocketTimeoutException => true
          case _ => false
        })
      }

      try
        return fn
      catch {
        case e: Exception if retryable(e) => () // won't recognize tail-recursive calls in a catch block as such
      }

      val exponentialRetryAfter = 1000 * pow(2, i - 1)
      val retryAfterWithJitter = (exponentialRetryAfter * (0.5 + random)).toInt
      Thread.sleep(retryAfterWithJitter)
      logger.debug(s"Retry $i after ${retryAfterWithJitter}ms: $context")
      attempt(i + 1)
    }

    attempt(i = 1)
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
