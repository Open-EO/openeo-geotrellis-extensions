package org.openeo.geotrellis.geotiff


import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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

    for ((bandName, bandIndex) <- bandNames.zipWithIndex) {
      options.addBandTag(bandIndex, "DESCRIPTION", bandName)
      options.addBandTag(bandIndex, "SCALE", "1.0")
      options.addBandTag(bandIndex, "OFFSET", "0.0")
    }

    assertEquals(Seq(
      "license" -> "CC-BY 4.0 - https://creativecommons.org/licenses/by/4.0/",
      "PROCESSING_SOFTWARE" -> "0.6.1a1",
      "references" -> "https://land.copernicus.eu/",
      "version" -> "v010"
    ), options.tags.headTags.toSeq)

    assertEquals(List(
      Seq("DESCRIPTION" -> "VV", "OFFSET" -> "0.0", "SCALE" -> "1.0"),
      Seq("DESCRIPTION" -> "VH", "OFFSET" -> "0.0", "SCALE" -> "1.0"),
      Seq("DESCRIPTION" -> "mask", "OFFSET" -> "0.0", "SCALE" -> "1.0"),
      Seq("DESCRIPTION" -> "local_incidence_angle", "OFFSET" -> "0.0", "SCALE" -> "1.0"),
    ), options.tags.bandTags.map(_.toSeq))

    val expectedGdalMetadataXml =
      <GDALMetadata>
        <Item name="license">CC-BY 4.0 - https://creativecommons.org/licenses/by/4.0/</Item>
        <Item name="PROCESSING_SOFTWARE">0.6.1a1</Item>
        <Item name="references">https://land.copernicus.eu/</Item>
        <Item name="version">v010</Item>
        <Item name="DESCRIPTION" sample="0" role="description">VV</Item>
        <Item name="OFFSET" sample="0" role="offset">0.0</Item>
        <Item name="SCALE" sample="0" role="scale">1.0</Item>
        <Item name="DESCRIPTION" sample="1" role="description">VH</Item>
        <Item name="OFFSET" sample="1" role="offset">0.0</Item>
        <Item name="SCALE" sample="1" role="scale">1.0</Item>
        <Item name="DESCRIPTION" sample="2" role="description">mask</Item>
        <Item name="OFFSET" sample="2" role="offset">0.0</Item>
        <Item name="SCALE" sample="2" role="scale">1.0</Item>
        <Item name="DESCRIPTION" sample="3" role="description">local_incidence_angle</Item>
        <Item name="OFFSET" sample="3" role="offset">0.0</Item>
        <Item name="SCALE" sample="3" role="scale">1.0</Item>
      </GDALMetadata>

    assertEquals(trim(expectedGdalMetadataXml), trim(options.tagsAsGdalMetadataXml))
  }
}
