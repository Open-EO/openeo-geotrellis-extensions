package org.openeo.geotrellissentinelhub

import org.junit.{Ignore, Test}

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
}
