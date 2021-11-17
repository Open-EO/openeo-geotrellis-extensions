package org.openeo.geotrellis.geotiff

import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import org.jboss.netty.util.internal.ConcurrentHashMap
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest

import scala.compat.java8.FunctionConverters._
import java.net.URI

object RegionAwareS3RangeReaderProvider {
  private type BucketName = String

  private val regionAwareS3ClientCache = new ConcurrentHashMap[BucketName, S3Client]

  private val instantiateRegionAwareS3ClientFromBucketName: BucketName => S3Client = bucketName => {
    val getBucketLocationRequest = GetBucketLocationRequest.builder()
      .bucket(bucketName)
      .build()

    val bucketRegion = S3ClientProducer.get()
      .getBucketLocation(getBucketLocationRequest)
      .locationConstraint()
      .toString

    S3Client.builder()
      .region(Region.of(bucketRegion))
      .build()
  }

  private def getRegionAwareS3Client(bucketName: BucketName): S3Client =
    regionAwareS3ClientCache.computeIfAbsent(bucketName, instantiateRegionAwareS3ClientFromBucketName.asJava)
}

class RegionAwareS3RangeReaderProvider extends S3RangeReaderProvider {
  import RegionAwareS3RangeReaderProvider._

  override def rangeReader(uri: URI): S3RangeReader = rangeReader(uri, regionAwareS3Client(uri))

  private def regionAwareS3Client(uri: URI): S3Client = {
    val s3Uri = new AmazonS3URI(uri)
    getRegionAwareS3Client(s3Uri.getBucket)
  }
}
