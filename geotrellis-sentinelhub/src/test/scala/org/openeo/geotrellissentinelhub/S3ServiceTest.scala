package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.{BeforeClass, Ignore, Rule, Test}

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

  @Test
  def download_stac_metadata(): Unit = {
    val tempDir = temporaryFolder.getRoot

    s3Service.download_stac_metadata(
      bucketName,
      request_group_id = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
      target_dir = tempDir.getAbsolutePath
    )

    val outputFiles = tempDir.list()
    assertEquals(outputFiles mkString ", ", 3, outputFiles.size)
    assertTrue(outputFiles.forall(_.endsWith("_metadata.json")))
  }

  @Test(timeout = 60 * 1000)
  def download_stac_metadataBailsIfTakesTooLong(): Unit = {
    s3Service.download_stac_metadata(
      bucketName,
      request_group_id = "a6b90672-495a-4e6c-8729-fcbd8e6ff82f",
      target_dir = "/does/not/matter",
      max_delay_secs = 30
    )
  }

  @Ignore
  @Test
  def delete_batch_process_results(): Unit = {
    s3Service.delete_batch_process_results(bucketName, subfolder = "d4737bbc-77b2-4ecb-8a5c-e1919b7eb23c")
  }

  @Test
  def delete_batch_process_resultsSucceedsForUnknownSubfolder(): Unit = {
    s3Service.delete_batch_process_results(bucketName, subfolder = "retteketet")
  }
}
