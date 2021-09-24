package org.openeo.geotrelliss3

import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import org.openeo.geotrelliss3.MultiClientRangeReaderProvider.client
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.core.retry.conditions.{OrRetryCondition, RetryCondition}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import java.net.URI
import java.time.Duration

/**
 * A range reader provider for openEO that tries to support situations where there is not a single S3 endpoint.
 * On CreoDIAS this is the case, or the backend could be reading from a remote S3 for certain layers.
 *
 * Rangereaders or instantiated lazily on the executors, so we can not assume that configuration on the driver is available there.
 * What we can do, is to parse a mapping of bucket -> endpoint (or S3 config) based on a config file, in each executor.
 *
 * We start out with a naive hard coded implementation.
 */
object MultiClientRangeReaderProvider {
  def client(endpoint:String, region:String):S3Client = {
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
      .region(if(region!="") Region.of(region) else Region.EU_CENTRAL_1)
    val theClient = if(endpoint!="") {
      clientBuilder.endpointOverride(URI.create(endpoint)).build()
    }else{
      clientBuilder.build()
    }
    return theClient
  }
}

class MultiClientRangeReaderProvider extends S3RangeReaderProvider{

  @transient lazy val swiftEndpoint = System.getenv().getOrDefault("SWIFT_URL","https://s3.waw2-1.cloudferro.com")
  @transient lazy val s3Endpoint = System.getenv().getOrDefault("AWS_S3_ENDPOINT","")


  override def rangeReader(uri: URI): S3RangeReader = {
    val s3Uri = new AmazonS3URI(uri)
    val isCloudFerro = s3Endpoint.toLowerCase.contains("cloudferro")
    val theClient: S3Client =
    if(isCloudFerro ) {
      if(s3Uri.getBucket.toLowerCase().equals("eodata")){
        client(s3Endpoint,"RegionOne")
      }else{
        client(swiftEndpoint,"RegionOne")
      }
    }else{
      S3ClientProducer.get()
    }
    rangeReader(uri, theClient)
  }

}
