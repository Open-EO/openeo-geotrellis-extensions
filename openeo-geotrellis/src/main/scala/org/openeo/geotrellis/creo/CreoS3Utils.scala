package org.openeo.geotrellis.creo

import org.openeo.geotrelliss3.S3Utils
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Client, S3Configuration}

import java.net.URI
import java.time.Duration

object CreoS3Utils {

  private val cloudFerroRegion: Region = Region.of("RegionOne")

  def getAsyncClient(): S3AsyncClient = {
    S3AsyncClient.crtBuilder()
      .credentialsProvider(credentialsProvider)
      //.serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .region(cloudFerroRegion).forcePathStyle(true).endpointOverride(URI.create(sys.env("SWIFT_URL")))
      .build();
  }

  def getCreoS3Client(): S3Client = {

    val clientBuilder = S3Client.builder()
      .serviceConfiguration(S3Configuration.builder().checksumValidationEnabled(false).build())
      .overrideConfiguration(overrideConfig).forcePathStyle(true)
      .region(cloudFerroRegion)

    clientBuilder.endpointOverride(URI.create(sys.env("SWIFT_URL"))).credentialsProvider(credentialsProvider).build()
  }

  private def credentialsProvider = {
    val swiftAccess = sys.env.getOrElse("SWIFT_ACCESS_KEY_ID", sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
    val swiftSecretAccess = sys.env.getOrElse("SWIFT_SECRET_ACCESS_KEY", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(swiftAccess, swiftSecretAccess))
    credentialsProvider
  }

  private def overrideConfig = {
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
    overrideConfig
  }

  def deleteCreoSubFolder(bucket_name: String, subfolder: String) = {
    val s3Client = getCreoS3Client()
    S3Utils.deleteSubFolder(s3Client, bucket_name, subfolder)
  }
}
