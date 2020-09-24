package org.openeo.geotrellis

import java.util.{ArrayList, Map}

import geotrellis.layer._
import geotrellis.raster
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.Compression
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.{ArrayTile, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import spire.syntax.cfor.cfor

import scala.collection.JavaConverters._

package object geotiff {
  type SRDD = RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]

  private def toExtent(extent: Map[String, Double]) = Extent(
    extent.get("xmin"),
    extent.get("ymin"),
    extent.get("xmax"),
    extent.get("ymax")
  )

  // ~ SpatialTiledRasterLayer in GeoPySpark but supports compression
  def saveStitched(rdd: SRDD, path: String, compression: Compression): Unit =
    saveStitched(rdd, path, None, None, compression)

  def saveStitched(rdd: SRDD, path: String, cropBounds: Map[String, Double], compression: Compression): Unit =
    saveStitched(rdd, path, Some(cropBounds), None, compression)

  def saveRDD(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]):Unit = {
    val compression = Deflate(zLevel)

    val re = rdd.metadata.toRasterExtent()
    val gridBounds = re.gridBoundsFor(cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = true)

    val tileLayout = rdd.metadata.tileLayout

    val keyBounds: Bounds[SpatialKey] = rdd.metadata.bounds
    val maxKey = keyBounds.get.maxKey
    val minKey = keyBounds.get.minKey

    val totalCols = maxKey.col - minKey.col +1
    val totalRows = maxKey.row - minKey.row + 1

    val segmentLayout = GeoTiffSegmentLayout(
      totalCols = gridBounds.width,
      totalRows = gridBounds.height,
      Tiled(tileLayout.tileCols,tileLayout.tileRows),
      BandInterleave,
      BandType.forCellType(rdd.metadata.cellType))

    val bandSegmentCount = totalCols * totalRows
    //val bandCount = 1
    val segmentCount = bandSegmentCount * bandCount
    println("Saving geotiff with "+ segmentCount + " segments.")
    val compressor = compression.createCompressor(segmentCount)

    val tiffs: collection.Map[Int, Array[Byte]] = rdd.flatMap{ case (key:SpatialKey,multibandTile:MultibandTile) => {
      var bandIndex = -1
      multibandTile.bands.map{
      tile => {
        bandIndex+=1
        val layoutCol = key._1 - minKey._1
        val layoutRow = key._2 - minKey._2
        val bandSegmentOffset = bandSegmentCount * bandIndex
        val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
        //tiff format seems to require that we provide 'full' tiles
        val bytes = raster.CroppedTile(tile,raster.GridBounds(0,0,tileLayout.tileCols-1,tileLayout.tileRows-1)).toBytes()
        val compressedBytes = compressor.compress(bytes, index)
        (index,compressedBytes)
      }

    }}}.collectAsMap()

    lazy val emptySegment =
      ArrayTile.empty(rdd.metadata.cellType, tileLayout.tileCols, tileLayout.tileRows).toBytes

    val segments: Array[Array[Byte]] = Array.ofDim(segmentCount)
    cfor (0)(_ < segmentCount, _ + 1){ index => {
      val maybeBytes = tiffs.get(index)
      if (maybeBytes.isEmpty) {
        segments(index) = compressor.compress(emptySegment, index)
      } else {
        segments(index) = maybeBytes.get
      }
    }
    }

    val tiffTile: GeoTiffMultibandTile = GeoTiffMultibandTile(
      new ArraySegmentBytes(segments),
      compressor.createDecompressor(),
      segmentLayout,
      compression,
      bandCount,
      rdd.metadata.cellType)
    val thegeotiff = MultibandGeoTiff(tiffTile,croppedExtent,rdd.metadata.crs)

    GeoTiffWriter.write(thegeotiff,path)

  }

  def saveStitched(
                    rdd: SRDD,
                    path: String,
                    cropBounds: Option[Map[String, Double]],
                    cropDimensions: Option[ArrayList[Int]],
                    compression: Compression)
  : Unit = {
    val contextRDD = ContextRDD(rdd, rdd.metadata)

    val stitched: Raster[MultibandTile] = contextRDD.stitch()

    val adjusted = {
      val cropped =
        cropBounds match {
          case Some(extent) => stitched.crop(toExtent(extent))
          case None => stitched
        }

      val resampled =
        cropDimensions.map(_.asScala.toArray) match {
          case Some(dimensions) =>
            cropped.resample(dimensions(0), dimensions(1))
          case None =>
            cropped
        }

      resampled
    }

    MultibandGeoTiff(adjusted, contextRDD.metadata.crs, GeoTiffOptions(compression)).write(path)
  }
}
