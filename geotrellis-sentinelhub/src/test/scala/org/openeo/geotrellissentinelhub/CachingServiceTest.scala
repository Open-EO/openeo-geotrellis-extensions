package org.openeo.geotrellissentinelhub

import org.junit.{Ignore, Test}

import java.nio.file.Files

@Ignore
class CachingServiceTest {

  private val cachingService = new CachingService

  @Test
  def download_and_cache_results(): Unit = {
    val bucketName = "openeo-sentinelhub"
    val subfolder = "d100d49b-7c4d-4cf3-bd11-3e7997f3a916"
    val collectingFolder = "/tmp/collecting3"

    // downloads batch process results from s3:///subfolder, caches them and symlinks them in collectingFolder
    cachingService.download_and_cache_results(
      bucketName,
      subfolder,
      collectingFolder
    )

    // assembles single band files from collectingFolder and uploads them recursively to S3
    val assembledFolder = cachingService.upload_multiband_tiles(
      subfolder,
      collectingFolder,
      bucketName
    )

    println(s"check out results at s3://$bucketName/$assembledFolder")
  }

  @Test
  def assemble_multiband_tiles(): Unit = {
    val assembledFolder = Files.createTempDirectory("assembled_")
    val collectingFolder = "/home/bossie/Documents/VITO/EP-4019: improve OpenEO batch job tracking/openeo_collecting/a747c8c4-b289-4c7e-9512-0f883529975e"
    val bucketName = "openeo-sentinelhub"
    val subfolder = "dummy"

    cachingService.assemble_multiband_tiles(
      collectingFolder,
      assembledFolder.toString,
      bucketName,
      subfolder
    )

    println(assembledFolder.toUri)
  }
}
