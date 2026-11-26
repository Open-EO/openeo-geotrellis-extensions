package org.openeo.geotrellis

import geotrellis.raster.io.geotiff.compression.DeflateCompression
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Random
import java.util.zip.Inflater

class DeflateCompressionTest {
  @Test
  def incompressibleTileProducesCompleteDeflateStream(): Unit = {
    val input = new Array[Byte](96 * 96)
    new Random(1234).nextBytes(input)
    val compressed = DeflateCompression(6).createCompressor(1).compress(input, 0)
    val inflater = new Inflater()
    try {
      inflater.setInput(compressed)
      val decoded = new Array[Byte](input.length + 1)
      val length = inflater.inflate(decoded)
      assertEquals(input.length, length)
      assertTrue(inflater.finished(), "Deflate stream must include its end marker and checksum")
      assertArrayEquals(input, java.util.Arrays.copyOf(decoded, length))
    } finally {
      inflater.end()
    }
  }
}
