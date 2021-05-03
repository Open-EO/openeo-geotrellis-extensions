package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.{Compression, DeflateCompression}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{ArrayTile, CellType, GridBounds, MultibandTile, Raster, RasterExtent, TileLayout}
import geotrellis.spark._
import geotrellis.util._
import geotrellis.vector.Extent
import mil.nga.geopackage.{GeoPackage, GeoPackageManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory
import spire.syntax.cfor.cfor

import java.io.File
import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.util.{ArrayList, Map}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect._

package object geotiff {

  private val logger = LoggerFactory.getLogger(classOf[OpenEOProcesses])

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

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, compression: Compression): List[String] =
    saveStitchedTileGrid(rdd, path, tileGrid, None, None, compression)

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, cropBounds: Map[String, Double], compression: Compression): List[String] =
    saveStitchedTileGrid(rdd, path, tileGrid, Some(cropBounds), None, compression)

  def saveRDDTiled(rdd:MultibandTileLayerRDD[SpaceTimeKey], path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]):Unit = {
    val layout = rdd.metadata.layout
    rdd.foreach(key_t => {
      val filename = s"X${key_t._1.col}Y${key_t._1.col}_${DateTimeFormatter.ISO_DATE.format(key_t._1.time)}.tif"
      GeoTiff(key_t._2,key_t._1.spatialKey.extent(layout),rdd.metadata.crs).write(path + filename,true)
    })
  }

  /**
   * Save temporal rdd, on the executors
   * @param rdd
   * @param path
   * @param zLevel
   * @param cropBounds
   */
  def saveRDDTemporal(rdd:MultibandTileLayerRDD[SpaceTimeKey], path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]):Unit = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) = preProcess(rdd,cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = preProcessResult._3

    val keyBounds = preprocessedRdd.metadata.bounds
    val maxKey = keyBounds.get.maxKey.getComponent[SpatialKey]
    val minKey = keyBounds.get.minKey.getComponent[SpatialKey]

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = maxKey.col - minKey.col +1
    val totalRows = maxKey.row - minKey.row + 1

    val compression = Deflate(zLevel)
    val bandSegmentCount = totalCols * totalRows

    preprocessedRdd.map { case (key: SpaceTimeKey, multibandTile: MultibandTile) => {
      var bandIndex = -1
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val theCompressor = compression.createCompressor( multibandTile.bandCount)
      (key,multibandTile.bands.map {
        tile => {
          bandIndex += 1
          val layoutCol = key.getComponent[SpatialKey]._1 - minKey._1
          val layoutRow = key.getComponent[SpatialKey]._2 - minKey._2
          val bandSegmentOffset = bandSegmentCount * bandIndex
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
          //tiff format seems to require that we provide 'full' tiles
          val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
          val compressedBytes = theCompressor.compress(bytes, 0)
          (index, (multibandTile.cellType,compressedBytes))
        }

      })
    }
    }.map(tuple => {
      val filename = s"openEO_${DateTimeFormatter.ISO_DATE.format(tuple._1.time)}.tif"
      (filename,tuple._2)
    }).groupByKey().foreach((tuple: (String, Iterable[Vector[(Int, (CellType, Array[Byte]))]])) => {
      val detectedBandCount = tuple._2.map(_.size).max
      val segments: Iterable[(Int, (CellType, Array[Byte]))] = tuple._2.flatten
      val cellTypes = segments.map(_._2._1).toSet
      val tiffs: Predef.Map[Int, Array[Byte]] = segments.map(tuple => (tuple._1,tuple._2._2)).toMap

      val segmentCount = (bandSegmentCount*detectedBandCount)
      writeTiff( Paths.get(path).resolve(tuple._1).toString  ,tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, tileLayout, compression, cellTypes.head, detectedBandCount, segmentCount)
    })

  }

  def saveRDD(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]):Unit = {
    saveRDDGeneric(rdd,bandCount, path, zLevel, cropBounds)
  }

  def preProcess[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K],cropBounds:Option[Extent]): (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = {
    val re = rdd.metadata.toRasterExtent()
    var gridBounds = re.gridBoundsFor(cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = true)
    val preprocessedRdd =
      if (gridBounds.colMin != 0 || gridBounds.rowMin != 0) {
        val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), 256)
        val retiledRDD = rdd.reproject(rdd.metadata.crs, geotiffLayout)._2.crop(croppedExtent, Options(force = false))

        gridBounds = retiledRDD.metadata.toRasterExtent().gridBoundsFor(cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
        retiledRDD
      } else {
        rdd.crop(croppedExtent, Options(force = false))
      }
    (gridBounds, croppedExtent, preprocessedRdd)
  }

  def saveRDDGeneric[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K], bandCount:Int, path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]):Unit = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd,cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3

    val keyBounds = preprocessedRdd.metadata.bounds
    val maxKey = keyBounds.get.maxKey.getComponent[SpatialKey]
    val minKey = keyBounds.get.minKey.getComponent[SpatialKey]

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = maxKey.col - minKey.col +1
    val totalRows = maxKey.row - minKey.row + 1

    val compression = Deflate(zLevel)
    val bandSegmentCount = totalCols * totalRows

    val totalBandCount = rdd.sparkContext.longAccumulator("TotalBandCount")
    val typeAccumulator = new SetAccumulator[CellType]()
    rdd.sparkContext.register(typeAccumulator,"CellType")
    val tiffs: collection.Map[Int, Array[Byte]] = preprocessedRdd.flatMap { case (key: K, multibandTile: MultibandTile) => {
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
          val layoutCol = key.getComponent[SpatialKey]._1 - minKey._1
          val layoutRow = key.getComponent[SpatialKey]._2 - minKey._2
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
    writeTiff( path,tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, tileLayout, compression, cellType, detectedBandCount, segmentCount)

  }

  private def writeTiff( path: String, tiffs:collection.Map[Int, Array[Byte]] , gridBounds: GridBounds[Int], croppedExtent: Extent,crs:CRS, tileLayout: TileLayout, compression: DeflateCompression, cellType: CellType, detectedBandCount: Double, segmentCount: Int) = {
    logger.info(s"Writing geotiff to $path with type ${cellType.toString()} and bands $detectedBandCount")
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

    val segmentLayout = GeoTiffSegmentLayout(
      totalCols = gridBounds.width,
      totalRows = gridBounds.height,
      Tiled(tileLayout.tileCols, tileLayout.tileRows),
      BandInterleave,
      BandType.forCellType(cellType))

    val tiffTile: GeoTiffMultibandTile = GeoTiffMultibandTile(
      new ArraySegmentBytes(segments),
      compressor.createDecompressor(),
      segmentLayout,
      compression,
      detectedBandCount.toInt,
      cellType)
    val thegeotiff = MultibandGeoTiff(tiffTile, croppedExtent, crs)

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

  def saveStitchedTileGrid(
                            rdd: SRDD,
                            path: String,
                            tileGrid: String,
                            cropBounds: Option[Map[String, Double]],
                            cropDimensions: Option[ArrayList[Int]],
                            compression: Compression)
  : List[String] = {
    val geoPackage = getGeoPackage(tileGrid)

    val features = ListBuffer[(String, Extent)]()

    try {
      val extent = rdd.metadata.extent.reproject(rdd.metadata.crs, LatLng)

      val resultSet = geoPackage.getFeatureDao(geoPackage.getFeatureTables.get(0))
        .query(s"ST_MaxX(geom)>=${extent.xmin} AND ST_MinX(geom)<=${extent.xmax} AND ST_MaxY(geom)>=${extent.ymin} AND ST_MinY(geom)<=${extent.ymax}")

      try while (resultSet.moveToNext) {
        val row = resultSet.getRow()
        val name = row.getValue("name").toString
        val envelope = row.getGeometryEnvelope
        val extent = Extent(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
        val reprojectedExtent = extent.reproject(LatLng, rdd.metadata.crs)
        features.append((name, reprojectedExtent))
      }
      finally resultSet.close()
    } finally geoPackage.close()

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      s"${path.substring(0, index)}-$tileId${path.substring(index)}"
    }

    val croppedExtent = cropBounds.map(toExtent)

    rdd.flatMap {
      case (key, tile) => features.filter { case (_, extent) =>
        val tileBounds = rdd.metadata.layout.mapTransform(extent)

        if (KeyBounds(tileBounds).includes(key)) true else false
      }.map { case (name, extent) =>
        ((name, extent), (key, tile))
      }
    }.groupByKey()
      .map { case ((name, extent), tiles) =>
        val stitched: Raster[MultibandTile] = ContextSeq(tiles, rdd.metadata).stitch().crop(extent)

        val adjusted = {
          val cropped =
            croppedExtent match {
              case Some(extent) => stitched.crop(extent)
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

        val filePath = newFilePath(path, name)
        MultibandGeoTiff(adjusted, rdd.metadata.crs, GeoTiffOptions(compression))
          .withOverviews(NearestNeighbor, List(4, 8, 16))
          .write(filePath)

        filePath
      }.collect()
      .toList
  }

  private def getGeoPackage(tileGrid: String): GeoPackage = {
    val basePath = "/data/users/Public/nielsh/tiling-grid"
    val path =
      if (tileGrid.contains("degree"))
        s"$basePath/wgs84-1degree.gpkg"
      else if (tileGrid.contains("100km"))
        s"$basePath/100km.gpkg"
      else if (tileGrid.contains("20km"))
        s"$basePath/20km.gpkg"
      else
        s"$basePath/10km.gpkg"

    GeoPackageManager.open(new File(path))
  }

  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: M) extends Seq[(K, V)] with Metadata[M] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }
}
