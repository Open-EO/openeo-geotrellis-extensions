package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.rules.TemporaryFolder
import org.junit.{BeforeClass, Ignore, Rule, Test}
import org.openeo.geotrellissentinelhub.S3Service.{StacMetadataUnavailableException, UnknownFolderException}

import java.nio.file.Paths
import scala.annotation.meta.getter

object S3ServiceTest {
  @BeforeClass
  def checkAwsSettings(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")
  }
}

class S3ServiceTest {
  private val s3Service = new S3Service
  private val bucketName = "openeo-sentinelhub"

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def download_stac_data(): Unit = {
    val tempDir = temporaryFolder.getRoot

    s3Service.download_stac_data(
      bucketName,
      request_group_uuid = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
      target_dir = tempDir.getAbsolutePath
    )

    val outputFiles = tempDir.list()
    assertEquals(outputFiles mkString ", ", 6, outputFiles.size)

    assertEquals(3, outputFiles.count(_.endsWith(".tif")))
    assertEquals(3, outputFiles.count(_.endsWith("_metadata.json")))
  }

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test(expected = classOf[StacMetadataUnavailableException], timeout = 60 * 1000)
  def download_stac_dataThrowsIfMetadataTakesTooLong(): Unit = {
    val tempDir = temporaryFolder.getRoot

    s3Service.download_stac_data(
      bucketName,
      request_group_uuid = "a6b90672-495a-4e6c-8729-fcbd8e6ff82f",
      target_dir = tempDir.getAbsolutePath,
      max_metadata_delay_secs = 30
    )
  }

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def download_stac_dataCanHandleBatchJobRetries(): Unit = {
    val tempDir = temporaryFolder.getRoot

    def download(): Unit = {
      s3Service.download_stac_data(
        bucketName,
        request_group_uuid = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
        target_dir = tempDir.getAbsolutePath
      )
    }

    download()
    download()

    val outputFiles = tempDir.list()
    assertEquals(outputFiles mkString ", ", 6, outputFiles.size)
  }

  @Ignore
  @Test
  def delete_batch_process_results(): Unit = {
    s3Service.delete_batch_process_results(bucketName, subfolder = "d4737bbc-77b2-4ecb-8a5c-e1919b7eb23c")
  }

  @Test(expected = classOf[UnknownFolderException])
  def delete_batch_process_resultsThrowsForUnknownSubfolder(): Unit = {
    s3Service.delete_batch_process_results(bucketName, subfolder = "retteketet")
  }

  @Ignore
  @Test
  def delete_batch_process_resultsInBucketInDifferentRegion(): Unit = {
    val bucketName = "openeo-sentinelhub-uswest2"
    s3Service.delete_batch_process_results(bucketName, subfolder = "5e2c5280-b900-4350-9e3a-fb25048bc207")
  }

  @Ignore
  @Test
  def uploadRecursively(): Unit = {
    s3Service.uploadRecursively(Paths.get("/tmp/1"), bucketName)
  }
}
