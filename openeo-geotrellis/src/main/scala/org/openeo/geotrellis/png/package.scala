package org.openeo.geotrellis

import java.io.File

import ar.com.hjg.pngj.{ImageInfo, ImageLineHelper, ImageLineInt, PngWriter}
import geotrellis.layer.SpatialKey
import geotrellis.raster.{MultibandTile, UByteCellType}
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.geotiff.SRDD

package object png {
  def saveStitched(srdd: SRDD, path: String, cropBounds: Extent): Unit = {
    val tilesByRow = Option(cropBounds).foldLeft(srdd)(_ crop _)
        .groupBy { case (SpatialKey(_, row), _) => row }

    val scanLinesByRow = tilesByRow
      .mapValues(toScanLines)

    val scanLines = scanLinesByRow
      .sortByKey()
      .values
      .cache()
      .toLocalIterator.flatten

    val materialized = scanLines.toArray

    val combinedImageInfo = {
      val src = materialized.head.imgInfo
      new ImageInfo(src.cols, materialized.length, src.bitDepth, src.alpha, src.greyscale, src.indexed)
    }

    val pngWriter = new PngWriter(new File(path), combinedImageInfo)

    try {
      materialized foreach pngWriter.writeRow
      pngWriter.end()
    } finally pngWriter.close()
  }

  def saveStitched(srdd: SRDD, path: String): Unit = saveStitched(srdd, path, cropBounds = null)

  private def toScanLines(horizontalTiles: Iterable[(SpatialKey, MultibandTile)]): Iterable[ImageLineInt] = {
    // TODO: doesn't support missing SpatialKeys (think sparse polygons), use
    //  geotrellis.layer.stitch.SpatialTileLayoutCollectionStitchMethods.sparseStitch instead
    val stitched = horizontalTiles
      .toSeq
      .stitch()
      .convert(UByteCellType)

    val bitsPerChannel = 8
    val alpha = false
    val grayscale = stitched.bandCount < 3
    val indexed = false
    val imageInfo = new ImageInfo(stitched.cols, stitched.rows, bitsPerChannel, alpha, grayscale, indexed)

    def toImageLine(row: Int): ImageLineInt = {
      val line = new ImageLineInt(imageInfo)

      for (col <- 0 until stitched.cols) {
        if (grayscale) {
          val v = stitched.band(0).get(col, row)
          line.getScanline()(col) = v
        } else {
          val r = stitched.band(0).get(col, row)
          val g = stitched.band(1).get(col, row)
          val b = stitched.band(2).get(col, row)

          ImageLineHelper.setPixelRGB8(line, col, r, g, b)
        }
      }

      line
    }

    (0 until stitched.rows) map toImageLine
  }

  implicit class MultibandTilePngOutputMethods(spatialLayer: MultibandTileLayerRDD[SpatialKey]) {
    def writePng(path: String, bbox: ProjectedExtent = null): Unit =
      saveStitched(spatialLayer, path, if (bbox == null) null else bbox.reproject(spatialLayer.metadata.crs))
  }
}
