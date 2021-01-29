package org.openeo.geotrellis

import java.util.{ArrayList, Map}

import geotrellis.layer._
import geotrellis.raster
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.Compression
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, RasterExtent}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import spire.syntax.cfor.cfor

import scala.collection.JavaConverters._

package object geotiff {

  class SetAccumulator[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
    def this() = this(Set.empty[T])
    override def isZero: Boolean = value.isEmpty
    override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)
    override def reset(): Unit = value = Set.empty[T]
    override def add(v: T): Unit = value = value + v
    override def merge(other: AccumulatorV2[T, Set[T]]): Unit = value = value ++ other.value
  }


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
    var gridBounds = re.gridBoundsFor(cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = true)
    val preprocessedRdd =
    if (gridBounds.colMin != 0 || gridBounds.rowMin != 0) {
      val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), 256)
      val retiledRDD = rdd.reproject(rdd.metadata.crs,geotiffLayout)._2.crop(croppedExtent,Options(force=false))

      gridBounds = retiledRDD.metadata.toRasterExtent().gridBoundsFor(cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
      retiledRDD
    }else{
      rdd.crop(croppedExtent,Options(force=false))
    }

    val keyBounds: Bounds[SpatialKey] = preprocessedRdd.metadata.bounds
    val maxKey = keyBounds.get.maxKey
    val minKey = keyBounds.get.minKey

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = maxKey.col - minKey.col +1
    val totalRows = maxKey.row - minKey.row + 1

    val segmentLayout = GeoTiffSegmentLayout(
      totalCols = gridBounds.width,
      totalRows = gridBounds.height,
      Tiled(tileLayout.tileCols, tileLayout.tileRows),
      BandInterleave,
      BandType.forCellType(rdd.metadata.cellType))

    val bandSegmentCount = totalCols * totalRows

    val totalBandCount = rdd.sparkContext.longAccumulator("TotalBandCount")
    val typeAccumulator = new SetAccumulator[raster.CellType]()
    rdd.sparkContext.register(typeAccumulator,"CellType")
    val tiffs: collection.Map[Int, Array[Byte]] = preprocessedRdd.flatMap { case (key: SpatialKey, multibandTile: MultibandTile) => {
      var bandIndex = -1
      if(multibandTile.bandCount>0) {
        totalBandCount.add(multibandTile.bandCount)
      }
      typeAccumulator.add(multibandTile.cellType)
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!
      val theCompressor = compression.createCompressor( multibandTile.bandCount)
      multibandTile.bands.map {
        tile => {
          bandIndex += 1
          val layoutCol = key._1 - minKey._1
          val layoutRow = key._2 - minKey._2
          val bandSegmentOffset = bandSegmentCount * bandIndex
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
          //tiff format seems to require that we provide 'full' tiles
          val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
          val compressedBytes = theCompressor.compress(bytes, 0)
          (index, compressedBytes)
        }

      }
    }
    }.collectAsMap()

    val cellType = {
      if(typeAccumulator.value.isEmpty) {
        rdd.metadata.cellType
      }else{
        typeAccumulator.value.head
      }
    }
    println("Saving geotiff with Celltype: " + cellType)
    val detectedBandCount =  if(totalBandCount.avg >0) totalBandCount.avg else 1
    val segmentCount = (bandSegmentCount*detectedBandCount).toInt
    val compressor = compression.createCompressor(segmentCount)
    lazy val emptySegment =
      ArrayTile.empty(cellType, tileLayout.tileCols, tileLayout.tileRows).toBytes

    val segments: Array[Array[Byte]] = Array.ofDim(segmentCount)
    val emptySegmentCompressed = compressor.compress(emptySegment, 0)
    cfor(0)(_ < segmentCount, _ + 1) { index => {
      val maybeBytes = tiffs.get(index)
      if (maybeBytes.isEmpty) {
        segments(index) = emptySegmentCompressed
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
      detectedBandCount.toInt,
      cellType)
    val thegeotiff = MultibandGeoTiff(tiffTile, croppedExtent, preprocessedRdd.metadata.crs)

    GeoTiffWriter.write(thegeotiff, path)

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
