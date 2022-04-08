package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.{Duration, ZonedDateTime}
import scala.sys.process._

trait RateLimitingGuard {
  def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
            outputFormat: String, nDataSamples: Int, s1Orthorectification: Boolean): Duration
}

object NoRateLimitingGuard extends RateLimitingGuard with Serializable {
  override def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
                     outputFormat: String, nDataSamples: Int,
                     s1Orthorectification: Boolean): Duration = Duration.ZERO
}

object RlGuardAdapter {
  private val logger = LoggerFactory.getLogger(classOf[RlGuardAdapter])

  case class AccessToken(token: String, expires_at: ZonedDateTime) {
    def isValid(when: ZonedDateTime): Boolean = when isBefore expires_at
  }
}

class RlGuardAdapter extends RateLimitingGuard with Serializable {
  import RlGuardAdapter._

  private val newline = System.lineSeparator()

  override def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
                     outputFormat: String, nDataSamples: Int, s1Orthorectification: Boolean): Duration = {
    val request = Map(
      "request_id" -> "delay".asJson,
      "arguments" -> Map(
        "batch_processing" -> batchProcessing.asJson,
        "width" -> width.asJson,
        "height" -> height.asJson,
        "n_input_bands_without_datamask" -> nInputBandsWithoutDatamask.asJson,
        "output_format" -> outputFormat.asJson,
        "n_data_samples" -> nDataSamples.asJson,
        "s1_orthorectification" -> s1Orthorectification.asJson
      ).asJson
    )

    call[Duration](
      request,
      onSuccess = result => {
        val delayInSeconds = result("delay_s").asNumber.map(_.toDouble).get
        Duration.ofMillis((delayInSeconds * 1000).toLong)
      },
      onError = Duration.ZERO // retry with exponential backoff
    )
  }

  def accessToken: Option[AccessToken] = {
    val request = Map("request_id" -> "access_token".asJson)

    call[Option[AccessToken]](
      request,
      onSuccess = result => result("access_token").as[Option[AccessToken]].valueOr(throw _),
      onError = None // retry with Auth API token
    )
  }

  private def call[R](request: Map[String, Json], onSuccess: Map[String, Json] => R, onError: => R): R = {
    val python = System.getenv("PYSPARK_PYTHON")

    if (python == null) {
      logger.warn("Cannot invoke rate-limiting guard process because PYSPARK_PYTHON is not set")
      throw new IllegalStateException("Cannot invoke rate-limiting guard process because PYSPARK_PYTHON is not set")
    }

    val rlGuardAdapterInvocation = Seq(python, "-m", "openeogeotrellis.sentinel_hub.rlguard_adapter",
      request.asJson.noSpaces)

    val stdErrBuffer = new StringBuilder

    try {
      val rlGuardAdapterOutput = rlGuardAdapterInvocation
        .!!(ProcessLogger(fout = _ => (), ferr = s => {
          logger.debug(s) // assumes rlguard_adapter logs to stderr (= Python's logging default destination)
          stdErrBuffer.append(s).append(newline)
        }))

      decode[Map[String, Json]](rlGuardAdapterOutput.trim)
        .map { result =>
          val error = result.get("error").flatMap(_.asString)

          error match {
            case Some(message) => logger.error(message, new IOException(message)); onError
            case _ => onSuccess(result)
          }
        }
        .valueOr(throw _)
    } catch {
      case e: RuntimeException if e.getMessage startsWith "Nonzero exit value: " => // actual exception is undocumented
        // propagate to driver with more context
        val stderr = stdErrBuffer.result().trim
        val message = s"""Failed to invoke rate-limiting guard process "$rlGuardAdapterInvocation":$newline$stderr"""
        throw new IOException(message, e)
    }
  }
}
