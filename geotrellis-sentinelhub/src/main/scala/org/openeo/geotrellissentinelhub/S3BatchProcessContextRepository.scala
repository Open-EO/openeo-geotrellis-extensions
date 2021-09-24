package org.openeo.geotrellissentinelhub

import _root_.io.circe.generic.auto._
import geotrellis.vector._ // to be able to decode the Geometry in *BatchProcessContext
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, PutObjectRequest}

class S3BatchProcessContextRepository(bucketName: String) {
  def saveTo(batchProcessContext: BatchProcessContext, subfolder: String): Unit = S3.withClient { s3Client =>
    val contextType = batchProcessContext.getClass.getName
    uploadText(s3Client, batchProcessContext.toJson, key = s"$subfolder/$contextType.json")
  }

  def loadFrom(subfolder: String): BatchProcessContext = S3.withClient { s3Client =>
      val batchProcessContextKeys = S3.listObjectIdentifiers(s3Client, bucketName, prefix = subfolder)
        .map(_.key())
        .filter(key => key.contains("org.openeo.geotrellissentinelhub") && key.endsWith("BatchProcessContext.json"))
        .iterator

    if (batchProcessContextKeys.hasNext) {
      val key = batchProcessContextKeys.next()

      if (batchProcessContextKeys.hasNext)
        throw new IllegalStateException(s"multiple batch process contexts found at s3://$bucketName/$subfolder")

      val contextType = key.split("/").last.replace(".json", "")
      val json = downloadText(s3Client, key)

      // TODO: is there a more general way?
      if (contextType == classOf[Sentinel2L2aBatchProcessContext].getName) {
        BatchProcessContext.fromJson[Sentinel2L2aBatchProcessContext](json)
      } else if (contextType == classOf[Sentinel1GrdBatchProcessContext].getName) {
        BatchProcessContext.fromJson[Sentinel1GrdBatchProcessContext](json)
      } else
        throw new IllegalStateException(s"unknown batch process context $contextType")
    } else throw new IllegalStateException(s"no batch process contexts found at s3://$bucketName/$subfolder")
  }

  private def uploadText(s3Client: S3Client, text: String, key: String): Unit = {
    val putObjectRequest = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.putObject(putObjectRequest, RequestBody.fromString(text))
  }

  private def downloadText(s3Client: S3Client, key: String): String = {
    val getObjectRequest = GetObjectRequest.builder()
      .bucket(bucketName)
      .key(key)
      .build()

    s3Client.getObject(getObjectRequest, ResponseTransformer.toBytes[GetObjectResponse]).asUtf8String()
  }
}
