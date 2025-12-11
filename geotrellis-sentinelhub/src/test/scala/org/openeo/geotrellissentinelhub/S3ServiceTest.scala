package org.openeo.geotrellissentinelhub

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeAll, Disabled, Test, Timeout}
import org.openeo.geotrellissentinelhub.S3Service.{StacMetadataUnavailableException, UnknownFolderException}

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.ListHasAsScala

object S3ServiceTest {
  @BeforeAll
  def checkAwsSettings(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")
  }
}

class S3ServiceTest {
  private val s3Service = new S3Service
  private val bucketName = "openeo-sentinelhub"

  @Disabled("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def download_stac_data(@TempDir temporaryFolder: Path): Unit = {
    val tempDir = temporaryFolder.getRoot

    s3Service.download_stac_data(
      bucketName,
      request_group_uuid = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
      target_dir = tempDir.toAbsolutePath.toString
    )

    val outputFiles = Files.list(tempDir).toList.asScala
    assertEquals(6, outputFiles.size, outputFiles mkString ", ")

    assertEquals(3, outputFiles.count(_.endsWith(".tif")))
    assertEquals(3, outputFiles.count(_.endsWith("_metadata.json")))
  }

  @Disabled("the bucket is being emptied because S3 costs are through the roof")
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  def download_stac_dataThrowsIfMetadataTakesTooLong(@TempDir temporaryFolder: Path): Unit = {
    val tempDir = temporaryFolder.getRoot

    assertThrows(
      classOf[StacMetadataUnavailableException], () =>
        s3Service.download_stac_data(
          bucketName,
          request_group_uuid = "a6b90672-495a-4e6c-8729-fcbd8e6ff82f",
          target_dir = tempDir.toAbsolutePath.toString,
          max_metadata_delay_secs = 30
        ))
  }

  @Disabled("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def download_stac_dataCanHandleBatchJobRetries(@TempDir temporaryFolder: Path): Unit = {
    val tempDir = temporaryFolder.getRoot

    def download(): Unit = {
      s3Service.download_stac_data(
        bucketName,
        request_group_uuid = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
        target_dir = tempDir.toAbsolutePath.toString
      )
    }

    download()
    download()

    val outputFiles = Files.list(tempDir).toList.asScala
    assertEquals(6, outputFiles.size, outputFiles mkString ", ")
  }

  @Disabled
  @Test
  def delete_batch_process_results(): Unit = {
    s3Service.delete_batch_process_results(bucketName, subfolder = "d4737bbc-77b2-4ecb-8a5c-e1919b7eb23c")
  }

  @Test
  def delete_batch_process_resultsThrowsForUnknownSubfolder(): Unit = {
    assertThrows(
      classOf[UnknownFolderException],
      () => s3Service.delete_batch_process_results(bucketName, subfolder = "retteketet"))
  }

  @Disabled
  @Test
  def delete_batch_process_resultsInBucketInDifferentRegion(): Unit = {
    val bucketName = "openeo-sentinelhub-uswest2"
    s3Service.delete_batch_process_results(bucketName, subfolder = "5e2c5280-b900-4350-9e3a-fb25048bc207")
  }

  @Disabled
  @Test
  def uploadRecursively(): Unit = {
    s3Service.uploadRecursively(Paths.get("/tmp/1"), bucketName)
  }
}
