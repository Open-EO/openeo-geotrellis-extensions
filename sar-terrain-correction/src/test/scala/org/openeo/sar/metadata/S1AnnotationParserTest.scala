package org.openeo.sar.metadata

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.net.URI
import java.nio.file.Paths

class S1AnnotationParserTest {

  private def noiseUri: URI = {
    val url = getClass.getResource("/noise.xml")
    assertTrue(url != null, "noise.xml missing from test resources")
    Paths.get(url.toURI).toUri
  }

  @Test
  def parsesLegacyNoiseVectorList(): Unit = {
    val lut = S1AnnotationParser.parseNoiseLut(noiseUri)

    // The fixture is an IPF < 2.9 product: 26 <noiseVector> entries of 642 (or 641) samples.
    assertEquals(26, lut.lines.length)
    assertEquals(0, lut.lines.head)
    assertEquals(16717, lut.lines.last)
    assertTrue(lut.lines.sliding(2).forall(p => p(0) < p(1)), "lines must be strictly increasing")

    assertEquals(26, lut.values.length)
    assertEquals(lut.pixels.length, lut.values.head.length)
    assertEquals(0, lut.pixels.head)
    assertTrue(lut.pixels.last > 25000, s"unexpected last pixel ${lut.pixels.last}")
    assertTrue(lut.pixels.sliding(2).forall(p => p(0) < p(1)), "pixels must be strictly increasing")
  }

  @Test
  def noiseLutSamplesMatchTheAnnotationValues(): Unit = {
    val lut = S1AnnotationParser.parseNoiseLut(noiseUri)

    // First and last sample of the first noiseLut row.
    assertEquals(1604.051, lut(0, 0), 1e-2)
    assertEquals(0.0, lut(0, lut.pixels.last), 1e-6)

    // First sample of the last noiseLut row.
    assertEquals(1587.373, lut(16717, 0), 1e-2)

    // Interpolated values stay within the range of the surrounding samples.
    val mid = lut(8000, 12000)
    assertTrue(mid >= 0.0, s"noise must be non-negative, got $mid")
  }

  @Test
  def noiseLutClampsOutsideTheAnnotatedGrid(): Unit = {
    val lut = S1AnnotationParser.parseNoiseLut(noiseUri)

    assertEquals(lut(0, 0), lut(-100, -100), 1e-9)
    assertEquals(lut(lut.lines.last, lut.pixels.last), lut(999999, 999999), 1e-9)
  }
}
