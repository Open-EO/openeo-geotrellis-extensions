package org.openeo.geotrellis.creo

import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import org.openeo.geotrelliss3.S3Utils

import java.net.URI
import java.time.Duration

object CreoS3Utils {

  def getCreoS3Client(): S3Client = {
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
      .region(Region.of("RegionOne"))

    clientBuilder.endpointOverride(URI.create(System.getenv("SWIFT_URL"))).build()
  }

  def deleteCreoSubFolder(bucket_name: String, subfolder: String) = {
    val s3Client = getCreoS3Client()
    S3Utils.deleteSubFolder(s3Client, bucket_name, subfolder)
  }
}
