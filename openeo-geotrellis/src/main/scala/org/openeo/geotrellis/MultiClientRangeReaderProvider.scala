package org.openeo.geotrellis

import geotrellis.store.s3.AmazonS3URI
import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import java.net.URI

/**
 * A range reader provider for openEO that tries to support situations where there is not a single S3 endpoint.
 * On CreoDIAS this is the case, or the backend could be reading from a remote S3 for certain layers.
 *
 * Rangereaders or instantiated lazily on the executors, so we can not assume that configuration on the driver is available there.
 * What we can do, is to parse a mapping of bucket -> endpoint (or S3 config) based on a config file, in each executor.
 *
 * We start out with a naive hard coded implementation.
 */

class MultiClientRangeReaderProvider extends S3RangeReaderProvider {
  @transient lazy val swiftEndpoint = new URI(System.getenv().getOrDefault("SWIFT_URL", "https://s3.waw2-1.cloudferro.com"))
  @transient lazy val s3Endpoint = System.getenv().get("AWS_S3_ENDPOINT")

  override def rangeReader(uri: URI): S3RangeReader = {
    val s3Uri = new AmazonS3URI(uri)
    val isCloudFerro = s3Endpoint != null && s3Endpoint.toLowerCase.contains("cloudferro")

    val theClient: S3Client =
      if (isCloudFerro)
        if (s3Uri.getBucket.toLowerCase().equals("eodata")) s3Client(Region.of("RegionOne"), new URI(s3Endpoint))
        else s3Client(Region.of("RegionOne"), swiftEndpoint)
      else s3Client(bucketRegion(s3Uri.getBucket))

    rangeReader(uri, theClient)
  }
}
