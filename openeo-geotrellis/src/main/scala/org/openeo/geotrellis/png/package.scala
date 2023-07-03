package org.openeo.geotrellis

import ar.com.hjg.pngj.{ImageInfo, ImageLineHelper, ImageLineInt, PngWriter}
import geotrellis.layer.SpatialKey
import geotrellis.raster.render.RGBA
import geotrellis.raster.{MultibandTile, UByteCellType}
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.geotiff.{SRDD, uploadToS3}

import java.io.File
import java.nio.file.{Files, Paths}

package object png {
  def saveStitched(srdd: SRDD, path: String, cropBounds: Extent, options: PngOptions): String = {
    val tilesByRow = Option(cropBounds).foldLeft(srdd)(_ crop _)
        .groupBy { case (SpatialKey(_, row), _) => row }

    val scanLinesByRow = tilesByRow
      .mapValues(toScanLines)

    val cached = scanLinesByRow
      .sortByKey()
      .values
      .cache
    cached.name = s"PNG-RDD ${path}"
    val scanLines = cached
      .toLocalIterator.flatten

    val materialized = scanLines.toArray

    cached.unpersist(blocking = false)

    val isIndexed = options != null && options.colorMap.isDefined
    val combinedImageInfo = {
      val src = materialized.head.imgInfo
      new ImageInfo(src.cols, materialized.length, src.bitDepth, src.alpha, src.greyscale && !isIndexed, src.greyscale && isIndexed)
    }

    val s3Path = path.toLowerCase.startsWith("s3:")

    val localPath = if (s3Path) Files.createTempFile(null, null) else Paths.get(path)

    try {
      val pngWriter = new PngWriter(localPath.toFile, combinedImageInfo)
      try {
        if (combinedImageInfo.indexed) {
          val colorMap = options.colorMap.get
          val paletteChunk = pngWriter.getMetadata.createPLTEChunk
          paletteChunk.setNentries(colorMap.colors.size)

          for ((color, entry) <- colorMap.colors.zipWithIndex) {
            val rgb = RGBA(color)
            paletteChunk.setEntry(entry, rgb.red, rgb.green, rgb.blue)
          }
        }

        materialized foreach pngWriter.writeRow
        pngWriter.end()

        if (s3Path) {
          val correctS3Path = path.replaceFirst("(?i)s3:/(?!/)", "s3://")
          uploadToS3(localPath, correctS3Path)
        } else path
      } finally pngWriter.close()
    } finally if (s3Path) Files.delete(localPath)
  }

  def saveStitched(srdd: SRDD, path: String, options: PngOptions): Unit = saveStitched(srdd, path, cropBounds = null, options)
  def saveStitched(srdd: SRDD, path: String, cropBounds: Extent): Unit = saveStitched(srdd, path, cropBounds, options=null)
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
