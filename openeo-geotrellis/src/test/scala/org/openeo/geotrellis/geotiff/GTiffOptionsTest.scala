package org.openeo.geotrellis.geotiff

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.xml.Utility.trim

class GTiffOptionsTest {

  @Test
  def testTags(): Unit = {
    val options = new GTiffOptions
    options.addHeadTag("PROCESSING_SOFTWARE", "0.6.1a1")
    options.addHeadTag("license", "CC-BY 4.0 - https://creativecommons.org/licenses/by/4.0/")
    options.addHeadTag("version", "v010")
    options.addHeadTag("references", "https://land.copernicus.eu/")

    val bandNames = Seq("VV", "VH", "mask", "local_incidence_angle")

    for ((bandName, index) <- bandNames.zipWithIndex) {
      options.addBandTag(index, "DESCRIPTION", bandName)
    }

    assertEquals(Seq(
      "license" -> "CC-BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
      "PROCESSING_SOFTWARE" -> "0.6.1a1",
      "references" -> "https://land.copernicus.eu/",
      "version" -> "v010"
    ), options.tags.headTags.toSeq)

    assertEquals(List(
      Map("DESCRIPTION" -> "VV"),
      Map("DESCRIPTION" -> "VH"),
      Map("DESCRIPTION" -> "mask"),
      Map("DESCRIPTION" -> "local_incidence_angle")
    ), options.tags.bandTags)

    val expectedGdalMetadataXml =
      <GDALMetadata>
        <Item name="license">CC-BY 4.0 - https://creativecommons.org/licenses/by/4.0/</Item>
        <Item name="PROCESSING_SOFTWARE">0.6.1a1</Item>
        <Item name="references">https://land.copernicus.eu/</Item>
        <Item name="version">v010</Item>
        <Item name="DESCRIPTION" sample="0" role="description">VV</Item>
        <Item name="DESCRIPTION" sample="1" role="description">VH</Item>
        <Item name="DESCRIPTION" sample="2" role="description">mask</Item>
        <Item name="DESCRIPTION" sample="3" role="description">local_incidence_angle</Item>
      </GDALMetadata>

    assertEquals(trim(expectedGdalMetadataXml), trim(options.tagsAsGdalMetadataXml))
  }
}
