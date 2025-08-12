package org.openeo.geotrellis.geotiff

import geotrellis.raster.io.geotiff.compression.{Compression, Compressor, Decompressor}
import org.apache.commons.compress.compressors.zstandard.{ZstdCompressorInputStream, ZstdCompressorOutputStream}
import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

case class ZStdCompression(level: Int = 3) extends Compression {
  def createCompressor(segmentCount: Int): Compressor =
    new ZStdCompressor(segmentCount, level)
}

object ZStdCompression extends ZStdCompression(3)

class ZStdCompressor(segmentCount: Int, level: Int) extends Compressor {
  private val segmentSizes = Array.ofDim[Int](segmentCount)
  def code = 50000

  def compress(segment: Array[Byte], segmentIndex: Int): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val compressorOutputStream = new ZstdCompressorOutputStream(outputStream, level)
    IOUtils.copyLarge(new ByteArrayInputStream(segment), compressorOutputStream)
    compressorOutputStream.close()
    outputStream.toByteArray
  }

  def createDecompressor(): Decompressor =
    new ZStdDecompressor(segmentSizes)
}

class ZStdDecompressor(segmentSizes: Array[Int]) extends Decompressor {
  def code = 50000

  def decompress(segment: Array[Byte], segmentIndex: Int): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val stream = new ByteArrayInputStream(segment)
    val compressorInputStream = new ZstdCompressorInputStream(stream)
    IOUtils.copyLarge(compressorInputStream, outputStream)
    compressorInputStream.close()
    outputStream.toByteArray
  }
}

