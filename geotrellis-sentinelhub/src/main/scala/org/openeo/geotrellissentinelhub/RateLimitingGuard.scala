package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import io.circe.parser.decode
import io.circe.syntax._

import java.io.IOException
import java.time.Duration
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

class RlGuardAdapter extends RateLimitingGuard with Serializable {
  private val newline = System.lineSeparator()

  override def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
                     outputFormat: String, nDataSamples: Int, s1Orthorectification: Boolean): Duration = {
    val requestParams = Map(
      "batch_processing" -> batchProcessing.asJson,
      "width" -> width.asJson,
      "height" -> height.asJson,
      "n_input_bands_without_datamask" -> nInputBandsWithoutDatamask.asJson,
      "output_format" -> outputFormat.asJson,
      "n_data_samples" -> nDataSamples.asJson,
      "s1_orthorectification" -> s1Orthorectification.asJson
    )

    val python = System.getenv("PYSPARK_PYTHON")

    if (python == null)
      throw new IllegalStateException("Cannot invoke rate-limiting guard process because PYSPARK_PYTHON is not set")

    val rlGuardAdapterInvocation = Seq(python, "-m", "openeogeotrellis.sentinel_hub.rlguard_adapter",
      requestParams.asJson.noSpaces)

    val stdErrBuffer = new StringBuilder

    try {
      val rlGuardAdapterOutput = rlGuardAdapterInvocation
        .!!(ProcessLogger(fout = _ => (), ferr = s => stdErrBuffer.append(s).append(newline)))

      val delayInSeconds = decode[Map[String, Double]](rlGuardAdapterOutput.trim)
        .map(result => result("delay_s"))
        .valueOr(throw _)

      Duration.ofMillis((delayInSeconds * 1000).toLong)
    } catch {
      case e: RuntimeException if e.getMessage startsWith "Nonzero exit value: " => // actual exception is undocumented
        // propagate to driver with more context
        val stderr = stdErrBuffer.result().trim
        val message = s"""Failed to invoke rate-limiting guard process "$rlGuardAdapterInvocation":$newline$stderr"""
        throw new IOException(message, e)
    }
  }
}
