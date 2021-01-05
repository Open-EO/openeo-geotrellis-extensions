package org.openeo.geotrellissentinelhub

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.util
import scala.compat.java8.FunctionConverters._

class S3Service {
  def delete_batch_process_results(bucketName: String, batchRequestId: String): Unit = {
    val s3Client = S3Client.builder()
      .build()

    val listObjectsResponse = s3Client.listObjectsV2Paginator(
      ListObjectsV2Request.builder()
        .bucket(bucketName)
        .prefix(batchRequestId)
        .build()
    )

    val objectIdentifiers = {
      val toObjectIdentifier: S3Object => ObjectIdentifier =
        obj => ObjectIdentifier.builder().key(obj.key()).build()

      listObjectsResponse.contents().stream()
        .map[ObjectIdentifier](toObjectIdentifier.asJava)
        .collect(util.stream.Collectors.toList[ObjectIdentifier])
    }

    val deleteObjectsRequest = DeleteObjectsRequest.builder()
      .bucket(bucketName)
      .delete(Delete.builder().objects(objectIdentifiers).build())
      .build()

    s3Client.deleteObjects(deleteObjectsRequest)
  }
}
