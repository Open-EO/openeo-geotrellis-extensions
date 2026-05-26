package org.openeo.geotrellis

import geotrellis.store.s3.AmazonS3URI
import org.junit.jupiter.api.{Disabled, Test}
import org.openeo.geotrellis.creo.CreoS3Utils
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.HeadObjectRequest

class TestS3 {
  @Disabled("This snippet is just for debugging.")
  @Test def testUsgsLandsatS3(): Unit = {
    val s3Client = CreoS3Utils.getCreoS3Client(Region.of("us-west-2"))
    val url = "s3:/usgs-landsat/collection02/level-1/standard/etm/2021/198/024/LE07_L1TP_198024_20210608_20210704_02_T1/LE07_L1TP_198024_20210608_20210704_02_T1_B1.TIF"
    val correctUrl = url.replaceFirst("(?i)s3:/(?!/)", "s3://")
    val s3Uri = new AmazonS3URI(correctUrl)
    val objectRequest = HeadObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build
    val response = s3Client.headObject(objectRequest)
    println("HeadObject response: " + response)
  }
}
