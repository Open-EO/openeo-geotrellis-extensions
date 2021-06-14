package org.openeo.geotrellis

import java.io.File
import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.util.{ArrayList, Map}

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
import geotrellis.vector.{ProjectedExtent, _}
import mil.nga.geopackage.{GeoPackage, GeoPackageManager}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.locationtech.jts.geom.Envelope
import org.openeo.geotrellis
import org.slf4j.LoggerFactory
import spire.syntax.cfor.cfor

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

  def saveRDDTileGrid(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String, tileGrid: String, zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]) = {
    saveRDDGenericTileGrid(rdd,bandCount, path, tileGrid, zLevel, cropBounds)
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

  // This implementation does not properly work, output tiffs are not properly aligned and colors are also incorrect
  def saveRDDGenericTileGrid[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K], bandCount:Int, path:String, tileGrid: String, zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]) = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd,cropBounds)
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val compression = Deflate(zLevel)

    val features = getOverlappingFeaturesFromTileGrid(tileGrid, ProjectedExtent(preprocessedRdd.metadata.extent,preprocessedRdd.metadata.crs))

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      s"${path.substring(0, index)}-$tileId${path.substring(index)}"
    }

    preprocessedRdd
      .flatMap {
        case (key, tile) => features.map { case (name, extent) =>
          val tileBounds = preprocessedRdd.metadata.layout.mapTransform(extent)

          (name, extent, tileBounds)
        }.filter { case (_, _, tileBounds) =>
          if (KeyBounds(tileBounds).includes(key.getComponent[SpatialKey]())) true else false
        }.map { case (name, extent, tileBounds) =>
          val re = preprocessedRdd.metadata.toRasterExtent()
          val gridBounds = re.gridBoundsFor(extent, clamp = true)
          val croppedExtent = re.extentFor(gridBounds, clamp = true)
          ((name, croppedExtent, tileBounds, gridBounds), (key, tile))
        }
      }.groupByKey()
      .map { case ((name, extent, tileBounds, gridBounds), tiles) =>
        //The part below is probably wrong: each tile in a fixed tilegrid, will have it's own 'tilelayout', while here
        //we use the global tilelayout of the RDD.
        val keyBounds = KeyBounds(tileBounds)
        val maxKey = keyBounds.get.maxKey.getComponent[SpatialKey]
        val minKey = keyBounds.get.minKey.getComponent[SpatialKey]

        val totalCols = maxKey.col - minKey.col +1
        val totalRows = maxKey.row - minKey.row + 1

        val bandSegmentCount = totalCols * totalRows
        val someTile = tiles.head._2
        val detectedBandCount = someTile.bandCount
        val cellType = someTile.cellType

        val tiffs = tiles.flatMap { case (key: K, multibandTile: MultibandTile) => {
          var bandIndex = -1

          //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
          //Not sure how this works out for other types of compression!!!

          val theCompressor = compression.createCompressor(multibandTile.bandCount)
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
        }.toMap

        println("Saving geotiff with Celltype: " + cellType)

        val segmentCount = (bandSegmentCount*detectedBandCount).toInt
        val newPath = newFilePath(path, name)
        writeTiff(newPath, tiffs, gridBounds, extent.intersection(croppedExtent).get, preprocessedRdd.metadata.crs, tileLayout, compression, cellType, detectedBandCount, segmentCount)

        newPath
      }.collect()
      .toList
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
    val thegeotiff = MultibandGeoTiff(tiffTile, croppedExtent, crs).withOverviews(NearestNeighbor, List(4, 8, 16))

    GeoTiffWriter.write(thegeotiff, path,true)
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

    MultibandGeoTiff(adjusted, contextRDD.metadata.crs, GeoTiffOptions(compression))
      .withOverviews(NearestNeighbor, List(4, 8, 16))
      .write(path)
  }

  def saveStitchedTileGrid(
                            rdd: SRDD,
                            path: String,
                            tileGrid: String,
                            cropBounds: Option[Map[String, Double]],
                            cropDimensions: Option[ArrayList[Int]],
                            compression: Compression)
  : List[String] = {
    val features = getOverlappingFeaturesFromTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent,rdd.metadata.crs))

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      s"${path.substring(0, index)}-$tileId${path.substring(index)}"
    }

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
    rdd.flatMap {
      case (key, tile) => features.filter { case (_, extent) =>
        val tileBounds = layout.mapTransform(extent)

        if (KeyBounds(tileBounds).includes(key)) true else false
      }.map { case (name, extent) =>
        ((name, extent), (key, tile))
      }
    }.groupByKey()
      .map { case ((name, extent), tiles) =>
        val filePath = newFilePath(path, name)

        stitchAndWriteToTiff(tiles, filePath, layout, crs, extent, croppedExtent, cropDimensions, compression)

        filePath
      }.collect()
      .toList
  }

  private def stitchAndWriteToTiff(tiles: Iterable[(SpatialKey, MultibandTile)], filePath: String, layout: LayoutDefinition, crs: CRS, extent: Extent, croppedExtent: Option[Extent], cropDimensions: Option[java.util.ArrayList[Int]], compression: Compression) = {
    val raster: Raster[MultibandTile] = ContextSeq(tiles, layout).stitch()

    val re = raster.rasterExtent
    val alignedExtent = re.createAlignedGridExtent(extent).extent

    val stitched: Raster[MultibandTile] = raster.crop(alignedExtent)

    //TODO this additional cropping + resampling might not be needed, as a tile grid already defines a clear cropping
    val adjusted = {
      val cropped =
        croppedExtent match {
          case Some(extraExtent) => stitched.crop(extraExtent)
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


    MultibandGeoTiff(adjusted, crs, GeoTiffOptions(compression))
      .withOverviews(NearestNeighbor, List(4, 8, 16))
      .write(filePath,optimizedOrder = true)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: List[String],
                                   compression: Compression): List[String] = {
    val features = sampleNames.zip(polygons.polygons.map(_.extent))
    groupByFeatureAndWriteToTiff(rdd, Option.empty, features,path, Option.empty,compression)

}

  def saveStitchedTileGridTemporal( rdd:MultibandTileLayerRDD[SpaceTimeKey],
                                    path:String,
                                    tileGrid: String,
                                    compression: Compression) : List[String] = geotrellis.geotiff.saveStitchedTileGridTemporal(rdd,path,tileGrid, Option.empty, Option.empty, compression)

  def saveStitchedTileGridTemporal(
                                    rdd:MultibandTileLayerRDD[SpaceTimeKey],
                                    path:String,
                            tileGrid: String,
                            cropBounds: Option[Map[String, Double]],
                            cropDimensions: Option[ArrayList[Int]],
                            compression: Compression)
  : List[String] = {
    val features = getOverlappingFeaturesFromTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent, rdd.metadata.crs))
    groupByFeatureAndWriteToTiff(rdd, cropBounds, features,path,cropDimensions, compression)
  }

  private def groupByFeatureAndWriteToTiff(rdd: MultibandTileLayerRDD[SpaceTimeKey], cropBounds: Option[java.util.Map[String, Double]], features: List[(String, Extent)],path:String,cropDimensions: Option[ArrayList[Int]],
                                           compression: Compression) = {
    val featuresBC: Broadcast[List[(String, Extent)]] = SparkContext.getOrCreate().broadcast(features)

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
    rdd.flatMap {
      case (key, tile) => featuresBC.value.filter { case (_, extent) =>
        val tileBounds = layout.mapTransform(extent)

        if (KeyBounds(tileBounds).includes(key.spatialKey)) true else false
      }.map { case (name, extent) =>
        ((name, TemporalProjectedExtent(extent, crs, key.time)), (key.spatialKey, tile))
      }
    }.groupByKey()
      .map { case ((name, extent), tiles) =>

        val filename = s"openEO_${DateTimeFormatter.ISO_DATE.format(extent.time)}_${name}.tif"
        val filePath = Paths.get(path).resolve(filename).toString
        stitchAndWriteToTiff(tiles, filePath, layout, crs, extent.extent, croppedExtent, cropDimensions, compression)

        filePath
      }.collect()
      .toList
  }

  private def getGeoPackage(tileGrid: String): GeoPackage = {
    //TODO: can we move to artifactory?
    val basePath = "/data/MEP/tilegrids"
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

  def getOverlappingFeaturesFromTileGrid[K](tileGrid: String, extent: ProjectedExtent) = {
    val geoPackage = getGeoPackage(tileGrid)

    val features = ListBuffer[(String, Extent)]()

    try {
      val extentLatLng = extent.reproject(LatLng)

      val resultSet = geoPackage.getFeatureDao(geoPackage.getFeatureTables.get(0))
        .query(s"ST_MaxX(geom)>=${extentLatLng.xmin} AND ST_MinX(geom)<=${extentLatLng.xmax} AND ST_MaxY(geom)>=${extentLatLng.ymin} AND ST_MinY(geom)<=${extentLatLng.ymax}")

      try while (resultSet.moveToNext) {
        val row = resultSet.getRow()
        val name = row.getValue("name").toString

        val points = row.getGeometry.getGeometry.asInstanceOf[mil.nga.sf.Polygon].getExteriorRing.getPoints
        val bbox = new Envelope()
        for (p <- points.asScala) {
          val reprojected = Point(p.getX(), p.getY).reproject(LatLng, extent.crs)
          bbox.expandToInclude(reprojected.getX,reprojected.getY)

        }
        val tileExtent = Extent(bbox)
        features.append((name, tileExtent))
      }
      finally resultSet.close()
    } finally geoPackage.close()

    features.toList
  }

  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }
}
