package org.openeo

import org.slf4j.Logger

import java.lang.Math.{pow, random}
import java.net.SocketTimeoutException
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
          case s: SentinelHubException if s.statusCode == 429 || s.statusCode >= 500 => true
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

  private[geotrellissentinelhub] def mapDataFilters(metadataProperties: util.Map[String, Any]): collection.Map[String, String] =
    metadataProperties.asScala
      .map {
        case ("orbitDirection", value: String) => "sat:orbit_state" -> value
        case (property, _) => throw new IllegalArgumentException(s"unsupported metadata property $property")
      }
}
