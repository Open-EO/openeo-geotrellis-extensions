package org.openeo.geotrelliss3

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectsRequest, ListObjectsRequest, ObjectIdentifier}
import scala.collection.JavaConverters._

object S3Utils {

  val logger = LoggerFactory.getLogger(S3Utils.getClass)

  def deleteSubFolder(client: S3Client, bucketName: String, subfolder: String) = {
    val listObjectsRequest = ListObjectsRequest.builder
      .bucket(bucketName)
      .prefix(subfolder)
      .build
    val listObjectsResponse = client.listObjects(listObjectsRequest)
    val keys = listObjectsResponse.contents.asScala.map(_.key)
    val deleteObjectsRequest = DeleteObjectsRequest.builder
      .bucket(bucketName)
      .delete(Delete.builder.objects(keys.map(key => ObjectIdentifier.builder.key(key).build).asJavaCollection).build)
      .build
    logger.info("Deleting objects from S3.")
    logger.info(s"Bucket: $bucketName, Subfolder: $subfolder")
    logger.info(s"Objects: ${keys.mkString(", ")}")
    client.deleteObjects(deleteObjectsRequest)
  }

}
