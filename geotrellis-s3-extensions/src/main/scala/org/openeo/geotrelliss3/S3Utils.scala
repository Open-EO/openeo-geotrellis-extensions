package org.openeo.geotrelliss3

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectsRequest, ListObjectsRequest, ObjectIdentifier}
import scala.collection.JavaConverters._

object S3Utils {

  val logger = LoggerFactory.getLogger(S3Utils.getClass)

  def deleteSubFolder(client: S3Client, bucketName: String, subfolder: String): Unit = {
    val listObjectsRequest = ListObjectsRequest.builder
      .bucket(bucketName)
      .prefix(subfolder)
      .build
    val listObjectsResponse = client.listObjects(listObjectsRequest)
    val keys = listObjectsResponse.contents.asScala.map(_.key)
    if (keys.isEmpty) {
      logger.info(s"No objects to delete in $bucketName/$subfolder")
      // Avoid S3Exception: The XML you provided was not well-formed or did not validate against our published schema
      return
    }
    val deleteObjectsRequest = DeleteObjectsRequest.builder
      .bucket(bucketName)
      .delete(Delete.builder.objects(keys.map(key => ObjectIdentifier.builder.key(key).build).asJavaCollection).build)
      .build
    logger.info(s"Deleting objects from $bucketName/$subfolder: ${keys.mkString(", ")}")
    client.deleteObjects(deleteObjectsRequest)
  }

}
