package org.openeo.geotrellis.geotiff

import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import org.jboss.netty.util.internal.ConcurrentHashMap
import org.openeo.geotrellis.geotiff.RegionAwareS3RangeReaderProvider.{regionAwareS3ClientCache, getBucketRegion, requestBucketRegion}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest

import scala.compat.java8.FunctionConverters._
import java.net.URI

object RegionAwareS3RangeReaderProvider {
  private val regionAwareS3ClientCache = new ConcurrentHashMap[String, Region] // TODO: cache the region-aware S3Client instead

  private val requestBucketRegion: String => Region = bucketName => {
    val getBucketLocationRequest = GetBucketLocationRequest.builder()
      .bucket(bucketName)
      .build()

    val bucketRegion = S3ClientProducer.get()
      .getBucketLocation(getBucketLocationRequest)
      .locationConstraint()
      .toString

    Region.of(bucketRegion)
  }

  private def getBucketRegion(bucketName: String): Region =
    regionAwareS3ClientCache.computeIfAbsent(bucketName, requestBucketRegion.asJava)
}

class RegionAwareS3RangeReaderProvider extends S3RangeReaderProvider {
  override def rangeReader(uri: URI): S3RangeReader = rangeReader(uri, regionAwareS3Client(uri))

  private def regionAwareS3Client(uri: URI): S3Client = {
    val s3Uri = new AmazonS3URI(uri)

    S3Client.builder()
      .region(getBucketRegion(s3Uri.getBucket))
      .build()
  }
}
