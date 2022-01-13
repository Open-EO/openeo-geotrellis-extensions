package org.openeo

import _root_.io.circe.{Decoder, Encoder, HCursor, Json}
import _root_.io.circe.Decoder.Result
import cats.syntax.either._
import geotrellis.vector._
import net.jodah.failsafe.event.ExecutionAttemptedEvent
import net.jodah.failsafe.function.{CheckedConsumer, CheckedSupplier}
import net.jodah.failsafe.{Failsafe, FailsafeExecutor, RetryPolicy}
import org.slf4j.Logger
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.regions.Region
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
      val retryable: Throwable => Boolean = {
        // exceptions like ClassCastException, MatchError etc. thrown from this predicate are swallowed by Failsafe :/
        case SentinelHubException(_, 429, _) => true
        case SentinelHubException(_, 400, body) if body contains "Request body should be non-empty." => true
        case SentinelHubException(_, statusCode, _) if statusCode >= 500 => true
        case _: SocketTimeoutException => true
        case _: CirceException => true
        case _ => false
      }

      new RetryPolicy[R]
        .handleIf(retryable.asJava)
        .withBackoff(1, 1000, SECONDS) // should not reach maxDelay because of maxAttempts 5
        .withJitter(0.5)
        .withMaxAttempts(5)
        .onFailedAttempt(new CheckedConsumer[ExecutionAttemptedEvent[R]] {
          override def accept(t: ExecutionAttemptedEvent[R]): Unit = {
            logger.warn(s"Attempt ${t.getAttemptCount} failed: $context -> ${t.getLastFailure.getMessage}")
          }
        })
    }

    val failsafe: FailsafeExecutor[R] = Failsafe
      .`with`(retryPolicy)

    failsafe.get(new CheckedSupplier[R] {
      override def get(): R = fn
    })
  }

  // TODO: merge this "one time retry policy" with withRetries() and move the retries from the APIs to the services?
  def authorized[R](clientId: String, clientSecret: String)(fn: String => R): R = {
    def accessToken: String = AccessTokenCache.get(clientId, clientSecret)

    try fn(accessToken)
    catch {
      case SentinelHubException(_, 401, _) =>
        AccessTokenCache.invalidate(clientId, clientSecret)
        fn(accessToken)
    }
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
    def withClient[R](region: Region)(f: S3Client => R): R = {
      val s3Client = Option(region)
        .foldLeft(S3Client.builder()) { case (builder, region) => builder.region(region) }
        .build()

      // TODO: cache and reuse the S3Client instead?
      try f(s3Client)
      finally s3Client.close()
    }

    def withClient[R](f: S3Client => R): R = withClient(region = null)(f)

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

  // to compensate for the removal of "units": "DN"
  private[geotrellissentinelhub] def dnScaleFactor(datasetId: String, bandName: String): Option[Int] =
    if (Set("sentinel-2-l2a", "S2L2A") contains datasetId) {
      if (Set("B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12") contains bandName)
        Some(10000)
      else if (bandName == "AOT")
        Some(1000)
      else None
    } else if (Set("sentinel-2-l1c", "S2L1C").contains(datasetId) &&
      Set("B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B10", "B11", "B12") .contains(bandName))
      Some(10000)
    else None

  // flattens n MultiPolygons into their polygon exteriors
  private def polygonExteriors(multiPolygons: Array[MultiPolygon]): Seq[Polygon] =
    for {
      multiPolygon <- multiPolygons
      polygon <- multiPolygon.polygons
    } yield Polygon(polygon.getExteriorRing)

  private def dissolve(polygons: Seq[Polygon]): MultiPolygon =
    GeometryCollection(polygons).union().asInstanceOf[MultiPolygon]

  private[geotrellissentinelhub] def simplify(multiPolygons: Array[MultiPolygon]): MultiPolygon =
    dissolve(polygonExteriors(multiPolygons))
}
