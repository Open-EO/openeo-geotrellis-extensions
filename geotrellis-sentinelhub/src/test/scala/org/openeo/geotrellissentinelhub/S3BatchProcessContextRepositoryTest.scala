package org.openeo.geotrellissentinelhub

import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

class S3BatchProcessContextRepositoryTest {
  private val s3BatchProcessContextRepository = new S3BatchProcessContextRepository(bucketName = "openeo-sentinelhub")

  @Ignore
  @Test
  def saveTo(): Unit = {
    val s2BatchProcessContext = Sentinel2L2aBatchProcessContext(Seq("B04", "B03", "B02"), None, None, None, None)
    s3BatchProcessContextRepository.saveTo(s2BatchProcessContext, subfolder = "dummy")
  }

  @Ignore
  @Test
  def loadFrom(): Unit = {
    val s2BatchProcessContext = s3BatchProcessContextRepository.loadFrom(subfolder = "dummy")
      .asInstanceOf[Sentinel2L2aBatchProcessContext]

    println(s2BatchProcessContext)

    assertEquals(Seq("B04", "B03", "B02"), s2BatchProcessContext.bandNames)
  }
}
