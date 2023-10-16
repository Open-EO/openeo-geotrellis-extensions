package org.openeo.geotrellis.layers

import org.junit.Assert.assertEquals
import org.junit.Test


class SentinelXMLMetadataRasterSourceTest {

  @Test
  def testReadAngles(): Unit = {
    val path = getClass.getResource("/org/openeo/geotrellis/layers/MTD_TL.xml").getPath
    val source: Seq[SentinelXMLMetadataRasterSource] = SentinelXMLMetadataRasterSource.forAngleBands(path)
    assertEquals( 171.800, source(0).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals(  65.707, source(1).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals( 251.333, source(2).read().get.tile.band(0).getDouble(0,0),0.001)
    assertEquals(source.head.name.toString, path)
  }
}
