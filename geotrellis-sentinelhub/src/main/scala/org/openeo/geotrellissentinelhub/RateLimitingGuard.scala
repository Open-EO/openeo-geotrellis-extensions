package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import io.circe.parser.decode
import io.circe.syntax._

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

object RlGuardAdapter extends RateLimitingGuard with Serializable {
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

    val rlGuardAdapterOutput =
      Seq("python", "-m", "openeogeotrellis.sentinel_hub.rlguard_adapter", requestParams.asJson.noSpaces).!!.trim

    val delayInSeconds = decode[Map[String, Double]](rlGuardAdapterOutput)
      .map(result => result("delay_s"))
      .valueOr(throw _)

    Duration.ofMillis((delayInSeconds * 1000).toLong)
  }
}
