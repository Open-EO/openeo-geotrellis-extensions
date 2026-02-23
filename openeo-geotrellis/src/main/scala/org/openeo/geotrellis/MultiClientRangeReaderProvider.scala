package org.openeo.geotrellis

import geotrellis.store.s3.AmazonS3URI
import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import org.openeo.geotrellis.creo.CreoS3Utils
import org.slf4j.LoggerFactory
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
object MultiClientRangeReaderProvider {
  private val logger = LoggerFactory.getLogger(classOf[MultiClientRangeReaderProvider])

}
class MultiClientRangeReaderProvider extends S3RangeReaderProvider {
  import MultiClientRangeReaderProvider._
  @transient lazy val swiftEndpoint = new URI(sys.env.getOrElse("SWIFT_URL", "https://s3.waw3-1.cloudferro.com"))
  @transient lazy val s3Endpoint = sys.env.getOrElse("AWS_S3_ENDPOINT", null)
  @transient lazy val s3Https = sys.env.getOrElse("AWS_HTTPS","NO").toUpperCase.equals("YES")

  override def rangeReader(uriArgument: URI): S3RangeReader = {
    var effectiveUri = uriArgument // Can't make an method parameter 'var'
    var s3Uri = new AmazonS3URI(effectiveUri)
    val isCloudFerro = s3Endpoint != null &&
      (s3Endpoint.toLowerCase.contains("cloudferro") || s3Endpoint.toLowerCase.endsWith(".dataspace.copernicus.eu"))

    val theClient: S3Client =
      if (isCloudFerro) {

        val deprecatedBuckets = List("EOCLOUD", "eocloud", "DIAS", "dias")
        if (deprecatedBuckets.contains(s3Uri.getBucket)) {
          // https://dataspace.copernicus.eu/news/2025-12-11-upcoming-changes-earth-observation-data-eodata-repository-bucket-names
          logger.warn(s"Bucket ${s3Uri.getBucket} is deprecated, it probably needs to be eodata (lower-case).")
        }

        if (s3Uri.getBucket.toLowerCase().equals("eodata") || s3Uri.getBucket.toLowerCase().equals("hrvpp")) {
          // if the bucket is EODATA, rename it to eodata
          if (s3Uri.getBucket == "EODATA") {
            logger.warn("Bucket is EODATA, but should be lower-case: eodata.")
            effectiveUri = URI.create(effectiveUri.toString.replaceFirst("EODATA", "eodata"))
            s3Uri = new AmazonS3URI(effectiveUri)
          }

          var uri = new URI(s3Endpoint)
          if(uri.getScheme == null) {
            if(s3Https) {
              uri = URI.create("https://" + uri.toString)
            }else{
              uri = URI.create("http://" + uri.toString)
            }
          }
          s3Client(Region.of("RegionOne"), uri)
        } else if (s3Uri.getBucket.toLowerCase().startsWith("lcfm") || s3Uri.getBucket.toLowerCase().startsWith("eugw")) {
          CreoS3Utils.getCreoS3Client(Region.of("waw3-1"))
        } else if (s3Uri.getBucket.toLowerCase().startsWith("hr-vpp-products-") || s3Uri.getBucket.toLowerCase() == "topography" || s3Uri.getBucket.toLowerCase() == "hr-vpp-auxdata") {
          CreoS3Utils.getCreoS3Client(Region.of("waw3-1"))
        } else if (s3Uri.getBucket.toLowerCase().contains("waw4-1") || s3Uri.getBucket.toLowerCase().startsWith("landsat-bimonthly-mosaics-")) {
          //For moved buckets we should still go to waw4-1
          CreoS3Utils.getCreoS3Client(Region.of("waw4-1"))
        } else if (swiftEndpoint.toString.contains("waw4-1") || swiftEndpoint.toString.contains("otc")) {
          //Hack while transitioning from single region to multi region setup
          CreoS3Utils.getCreoS3Client(Region.of("waw3-1"))
        }
        else s3Client(Region.of("RegionOne"), swiftEndpoint)
      } else s3Client(bucketRegion(s3Uri.getBucket))

    rangeReader(effectiveUri, theClient)
  }
}
