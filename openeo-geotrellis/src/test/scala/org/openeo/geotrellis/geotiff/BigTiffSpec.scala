package org.openeo.geotrellis.geotiff

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.{Decompressor, NoCompressor}
import geotrellis.raster.{CellType, IntConstantNoDataCellType, TileLayout}
import geotrellis.raster.io.geotiff.{BandInterleave, BandType, BigTiff, FullResolutionImage, GeoTiffData, GeoTiffImageData, GeoTiffOptions, GeoTiffSegmentLayout, NewSubfileType, ReducedImage, SegmentBytes, Tags, Tiled}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.vector.Extent
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class BigTiffSpec extends AnyFunSpec with Matchers {

  it("should handle offsets greater than 2^32 without overflowing") {
    val tempFile = File.createTempFile("bigtiff_", ".tif")

    try {
      // the overview having a finer resolution than the full res image does not make sense but the point is that
      // the overview already tips the file size over 2^32 and the full res image's offsets should go beyond that
      // without overflowing [in the case of a cloud-optimized file layout]
      val overview = constantBigTiff(cols = 32768, rows = 32768, subfileType = ReducedImage) // over 2^32
      val fullRes = constantBigTiff(cols = 256, rows = 256, subfileType = FullResolutionImage, overviews = List(overview))
      GeoTiffWriter.write(fullRes, path = tempFile.toString, optimizedOrder = true)

      val bigTiffSizeThreshold = math.pow(2, 32).toLong

      assert(tempFile.length() > bigTiffSizeThreshold)

      val Array(firstFullResTileOffset, _*) = firstTileOffsets(tempFile)
      firstFullResTileOffset should be > bigTiffSizeThreshold
    } finally tempFile.delete()
  }

  private def firstTileOffsets(tiff: File): Array[Long] = {
    import sys.process._

    val cmd = Seq("tiffdump", tiff.toString)
    val output = cmd.!!

    val tagValue = raw"<(.+)>".r.unanchored

    output.split("\n")
      .filter(_ startsWith "TileOffsets")
      .map { line =>
        val firstTileOffset = line match {
          case tagValue(values) => values.split(" ").head
        }
        firstTileOffset.toLong
      }
  }

  private def constantBigTiff(cols: Int, rows: Int, subfileType: NewSubfileType, overviews: List[GeoTiffData] = Nil)
  : GeoTiffData = {
    val (_cols, _rows) = (cols, rows)
    val _overviews = overviews
    val _cellType = IntConstantNoDataCellType

    new GeoTiffData {
      override val cellType: CellType = _cellType

      override val extent: Extent = Extent(-180, -90, 180, 90)

      override val crs: CRS = LatLng

      override val tags: Tags = Tags.empty

      override val options: GeoTiffOptions = GeoTiffOptions.DEFAULT
        .copy(tiffType = BigTiff, subfileType = Some(subfileType))

      override val overviews: List[GeoTiffData] = _overviews

      override val imageData: GeoTiffImageData = new GeoTiffImageData {
        private val (blockCols, blockRows) = (256, 256)

        override val cols: Int = _cols

        override val rows: Int = _rows

        override val bandType: BandType = BandType.forCellType(_cellType)

        override val bandCount: Int = 1

        override val segmentBytes: SegmentBytes = new SegmentBytes {
          private lazy val segment = (for {
            _ <- 0 until (blockCols * blockRows)
            byte <- Array[Byte](0, 0, 0, 123)
          } yield byte).toArray

          override def getSegment(i: Int): Array[Byte] = segment

          override def getSegments(indices: Iterable[Int]): Iterator[(Int, Array[Byte])] =
            indices.iterator.map(i => i -> getSegment(i))

          override def getSegmentByteCount(i: Int): Int = blockCols * blockRows * cellType.bytes

          override def length: Int = (cols / blockCols) * (rows / blockRows) // # of blocks
        }

        override val decompressor: Decompressor = NoCompressor

        override val segmentLayout: GeoTiffSegmentLayout = {
          GeoTiffSegmentLayout(
            totalCols = cols,
            totalRows = rows,
            tileLayout = TileLayout(layoutCols = cols / blockCols, layoutRows = rows / blockRows, tileCols = blockCols, tileRows = blockRows),
            storageMethod = Tiled(blockCols = blockCols, blockRows = blockRows),
            interleaveMethod = BandInterleave,
          )
        }
      }
    }
  }
}
