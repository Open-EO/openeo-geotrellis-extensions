package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.{BeforeClass, Rule, Test}

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

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder

  @Test
  def downloadStacMetadata(): Unit = {
    val tempDir = temporaryFolder.getRoot

    s3Service.download_stac_metadata(
      bucket_name = "openeo-sentinelhub",
      batch_job_id = "e89517fe-390d-4109-b3cc-4e4d514ebe2b",
      target_dir = tempDir.getAbsolutePath
    )

    val outputFiles = tempDir.list()
    assertEquals(outputFiles.toString, 3, outputFiles.size)
    assertTrue(outputFiles.forall(_.endsWith("_metadata.json")))
  }
}
