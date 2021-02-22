package org.openeo.geotrellissentinelhub

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.nio.file.Paths
import java.util
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

class S3Service {
  def delete_batch_process_results(bucket_name: String, batch_requestId: String): Unit = {
    val s3Client = S3Client.builder()
      .build()

    val objectIdentifiers = listObjectIdentifiers(s3Client, bucket_name, batch_requestId)

    val deleteObjectsRequest = DeleteObjectsRequest.builder()
      .bucket(bucket_name)
      .delete(Delete.builder().objects(objectIdentifiers).build())
      .build()

    s3Client.deleteObjects(deleteObjectsRequest)
  }

  // previously batch processes wrote to s3://<bucket_name>/<batch_request_id> while the new ones write to
  // s3://<bucket_name>/<request_group_id> because they comprise multiple batch process requests
  def download_stac_metadata(bucket_name: String, request_group_id: String, target_dir: String): Unit = {
    val s3Client = S3Client.builder()
      .build()

    val stacMetadataKeys = listObjectIdentifiers(s3Client, bucket_name, request_group_id)
      .asScala
      .map(_.key())
      .filter(_.endsWith("_metadata.json"))

    for (key <- stacMetadataKeys) {
      val getObjectRequest = GetObjectRequest.builder()
        .bucket(bucket_name)
        .key(key)
        .build()

      val fileName = key.split("/").last
      val outputFile = Paths.get(target_dir).resolve(fileName)

      s3Client.getObject(getObjectRequest, outputFile)
    }
  }

  private def listObjectIdentifiers(s3Client: S3Client, bucketName: String,
                                    prefix: String): util.List[ObjectIdentifier] = {
    val listObjectsResponse = s3Client.listObjectsV2Paginator(
      ListObjectsV2Request.builder()
        .bucket(bucketName)
        .prefix(prefix)
        .build()
    )

    val toObjectIdentifier: S3Object => ObjectIdentifier =
      obj => ObjectIdentifier.builder().key(obj.key()).build()

    listObjectsResponse.contents().stream()
      .map[ObjectIdentifier](toObjectIdentifier.asJava)
      .collect(util.stream.Collectors.toList[ObjectIdentifier])
  }
}
