package org.openeo.geotrellis.geotiff

import org.junit.Assert.assertEquals
import org.junit.Test

class GTiffOptionsTest {

  @Test
  def testTags(): Unit = {
    val options = new GTiffOptions
    options.addHeadTag("PROCESSING_SOFTWARE", "0.6.1a1")

    val bandNames = Seq("VV", "VH", "mask", "local_incidence_angle")

    for ((bandName, index) <- bandNames.zipWithIndex) {
      options.addBandTag(index, "DESCRIPTION", bandName)
    }

    assertEquals(Map("PROCESSING_SOFTWARE" -> "0.6.1a1"), options.tags.headTags)
    assertEquals(List(
      Map("DESCRIPTION" -> "VV"),
      Map("DESCRIPTION" -> "VH"),
      Map("DESCRIPTION" -> "mask"),
      Map("DESCRIPTION" -> "local_incidence_angle")
    ), options.tags.bandTags)
  }
}
