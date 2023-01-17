package org.openeo.geotrelliss3

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectsRequest, ListObjectsRequest, ObjectIdentifier}
import scala.collection.JavaConverters._

object S3Utils {

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
    client.deleteObjects(deleteObjectsRequest)
  }

}
