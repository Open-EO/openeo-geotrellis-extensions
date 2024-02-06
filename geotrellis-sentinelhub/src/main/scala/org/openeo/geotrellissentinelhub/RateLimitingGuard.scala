package org.openeo.geotrellissentinelhub

import java.time.Duration

trait RateLimitingGuard {
  def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
            outputFormat: String, nDataSamples: Int, s1Orthorectification: Boolean): Duration
}

object NoRateLimitingGuard extends RateLimitingGuard with Serializable {
  override def delay(batchProcessing: Boolean, width: Int, height: Int, nInputBandsWithoutDatamask: Int,
                     outputFormat: String, nDataSamples: Int,
                     s1Orthorectification: Boolean): Duration = Duration.ZERO
}
