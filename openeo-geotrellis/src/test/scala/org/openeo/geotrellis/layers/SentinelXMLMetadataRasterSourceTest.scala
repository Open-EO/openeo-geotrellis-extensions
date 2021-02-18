package org.openeo.geotrellis.layers

import org.junit.Assert.assertEquals
import org.junit.Test


class SentinelXMLMetadataRasterSourceTest {


  @Test
  def testReadAngles(): Unit = {
    val source = new SentinelXMLMetadataRasterSource(getClass.getResource("/org/openeo/geotrellis/layers/MTD_TL.xml"))
    assertEquals( 171.800, source.mSAA,0.001)
    assertEquals( 65.707, source.mSZA,0.001)
    assertEquals( 251.333, source.mVAA,0.001)
  }
}
