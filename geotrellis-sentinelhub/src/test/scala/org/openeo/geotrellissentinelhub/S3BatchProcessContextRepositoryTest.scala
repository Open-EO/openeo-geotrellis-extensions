package org.openeo.geotrellissentinelhub

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Disabled, Test}

class S3BatchProcessContextRepositoryTest {
  private val s3BatchProcessContextRepository = new S3BatchProcessContextRepository(bucketName = "openeo-sentinelhub")

  @Disabled
  @Test
  def saveTo(): Unit = {
    val s2BatchProcessContext = Sentinel2L2aBatchProcessContext(Seq("B04", "B03", "B02"), None, None, None, None)
    s3BatchProcessContextRepository.saveTo(s2BatchProcessContext, subfolder = "dummy")
  }

  @Disabled
  @Test
  def loadFrom(): Unit = {
    val s2BatchProcessContext = s3BatchProcessContextRepository.loadFrom(subfolder = "dummy")
      .asInstanceOf[Sentinel2L2aBatchProcessContext]

    println(s2BatchProcessContext)

    assertEquals(Seq("B04", "B03", "B02"), s2BatchProcessContext.bandNames)
  }
}
