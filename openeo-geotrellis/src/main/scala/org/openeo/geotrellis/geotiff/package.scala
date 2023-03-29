package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.{Compression, DeflateCompression}
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.render.IndexedColorMap
import geotrellis.raster.resample._
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.s3._
import geotrellis.util._
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.openeo.geotrellis
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.stac.STACItem
import org.openeo.geotrellis.tile_grid.TileGrid
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import spire.syntax.cfor.cfor

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.util.{ArrayList, Collections, Map, List => JList}
import scala.collection.JavaConverters._
import scala.reflect._

package object geotiff {

  private val logger = LoggerFactory.getLogger(getClass)

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
  def saveStitched(rdd: SRDD, path: String, compression: Compression): Extent =
    saveStitched(rdd, path, None, None, compression)

  def saveStitched(rdd: SRDD, path: String, cropBounds: Map[String, Double], compression: Compression): Extent =
    saveStitched(rdd, path, Some(cropBounds), None, compression)

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, compression: Compression): java.util.List[(String, Extent)] =
    saveStitchedTileGrid(rdd, path, tileGrid, None, None, compression)

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, cropBounds: Map[String, Double], compression: Compression): java.util.List[(String, Extent)] =
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
   *
   * @param rdd
   * @param path
   * @param zLevel
   * @param cropBounds
   */
  def saveRDDTemporal(rdd:MultibandTileLayerRDD[SpaceTimeKey], path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent], formatOptions:GTiffOptions = new GTiffOptions): java.util.List[(String, String, Extent)] = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) = preProcess(rdd,cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = preProcessResult._3

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
    val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

    logger.info(s"Write Geotiff per date ${croppedExtent}, ${gridBounds}, ${tileLayout}")

    val compression = Deflate(zLevel)
    val bandSegmentCount = totalCols * totalRows

    preprocessedRdd.map { case (key: SpaceTimeKey, multibandTile: MultibandTile) =>
      var bandIndex = -1
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val theCompressor = compression.createCompressor(multibandTile.bandCount)
      (key, multibandTile.bands.map {
        tile =>
          bandIndex += 1
          val layoutCol = key.getComponent[SpatialKey]._1
          val layoutRow = key.getComponent[SpatialKey]._2
          val bandSegmentOffset = bandSegmentCount * bandIndex
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
          //tiff format seems to require that we provide 'full' tiles
          val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
          val compressedBytes = theCompressor.compress(bytes, 0)
          (index, (multibandTile.cellType, compressedBytes))
      })
    }.map(tuple => {
      val filename = s"${formatOptions.filenamePrefix}_${DateTimeFormatter.ISO_DATE.format(tuple._1.time)}.tif"
      val timestamp = tuple._1.time format DateTimeFormatter.ISO_ZONED_DATE_TIME
      ((filename, timestamp), tuple._2)
    }).groupByKey().map((tuple: ((String, String), Iterable[Vector[(Int, (CellType, Array[Byte]))]])) => {
      val detectedBandCount = tuple._2.map(_.size).max
      val segments: Iterable[(Int, (CellType, Array[Byte]))] = tuple._2.flatten
      val cellTypes = segments.map(_._2._1).toSet
      val tiffs: Predef.Map[Int, Array[Byte]] = segments.map(tuple => (tuple._1, tuple._2._2)).toMap

      val segmentCount = (bandSegmentCount*detectedBandCount)
      val thePath = Paths.get(path).resolve(tuple._1._1).toString
      writeTiff( thePath  ,tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, tileLayout, compression, cellTypes.head, detectedBandCount, segmentCount,formatOptions)
      val (_, timestamp) = tuple._1
      (thePath, timestamp, croppedExtent)
    }).collect().toList.asJava

  }

  def saveRDD(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent], formatOptions:GTiffOptions = new GTiffOptions):java.util.List[String] = {
    saveRDDGeneric(rdd,bandCount, path, zLevel, cropBounds,formatOptions)
  }

  def saveRDDTileGrid(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String, tileGrid: String, zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]) = {
    saveRDDGenericTileGrid(rdd,bandCount, path, tileGrid, zLevel, cropBounds)
  }

  def preProcess[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K],cropBounds:Option[Extent]): (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = {
    val re = rdd.metadata.toRasterExtent()
    /**
     * CLAMPING EP-4150
     * Gridbounds are clamped to the actually available rasterextent, this means that we don't add empty data if somehow a cropping bounds is provided that is larger than the actual datacube.
     * CroppedExtent needs to match exactly with whatever gridbounds that we got, so there we do not clamp. So even though we clamp when computing gridbounds, it can still have an extent that is larger than rasterextent!!!
     */
    var gridBounds = re.gridBoundsFor(cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = false)
    val filtered = new OpenEOProcesses().filterEmptyTile(rdd)
    val preprocessedRdd = {
      if (gridBounds.colMin != 0 || gridBounds.rowMin != 0) {
        logger.info(s"Gridbounds requires reprojection: ${gridBounds}")
        val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), rdd.metadata.tileCols,rdd.metadata.tileRows)
        val retiledRDD = filtered.reproject(rdd.metadata.crs, geotiffLayout)._2.crop(croppedExtent, Options(force = false))

        gridBounds = retiledRDD.metadata.toRasterExtent().gridBoundsFor(cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
        retiledRDD
      } else {
        // Buffering or not keeps the bottom line NaN.
        // However, buffering could make SentinelHub tiles to become almost empty.
        filtered.crop(croppedExtent, Options(force = false))
      }
    }
    val tileLayout = rdd.metadata.tileLayout
    val fullRDD = preprocessedRdd.withContext {
      _.mapValues[MultibandTile]((mbt: MultibandTile) => mbt.mapBands((i: Int,t: Tile) => raster.CroppedTile(t, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1))))
    }
    (gridBounds, croppedExtent, fullRDD)
  }

  class PowerOfTwoLocalLayoutScheme extends LayoutScheme {

    def zoomOut(level: LayoutLevel): LayoutLevel = {
      val LayoutLevel(zoom, LayoutDefinition(extent, tileLayout)) = level
      require(zoom > 0)
      // layouts may be uneven, don't let the short dimension go to 0
      val currentSize = level.layout.cellSize
      val outLayout = LayoutDefinition(RasterExtent(extent,CellSize(currentSize.width*2.0,currentSize.height*2.0)),level.layout.tileCols)

      LayoutLevel(zoom - 1, outLayout)
    }

    // not used in Pyramiding
    def zoomIn(level: LayoutLevel): LayoutLevel = ???
    def levelFor(extent: Extent, cellSize: CellSize): LayoutLevel = ???
  }

  def saveRDDGeneric[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K], bandCount:Int, path:String,zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent], formatOptions:GTiffOptions = new GTiffOptions):java.util.List[String] = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd,cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3.persist(StorageLevel.MEMORY_AND_DISK)

    try{
      val compression = Deflate(zLevel)
      val ( tiffs: _root_.scala.collection.Map[Int, _root_.scala.Array[Byte]], cellType: CellType, detectedBandCount: Double, segmentCount: Int) = getCompressedTiles(preprocessedRdd, gridBounds, compression)

      val overviews =
        if(formatOptions.overviews.toUpperCase == "ALL" || (formatOptions.overviews.toUpperCase == "AUTO" && (gridBounds.width>1024 || gridBounds.height>1024 )) ) {
          //create overviews
          val method = formatOptions.resampleMethod match {
            case "near" => NearestNeighbor
            case "mode" => Mode
            case "average" => Average
            case "bilinear" => Bilinear
            case "max" => Max
            case "min" => Min
            case "med" => Median
            case _ => NearestNeighbor
          }
          val levels = LocalLayoutScheme.inferLayoutLevel(preprocessedRdd.metadata)

          if(levels>1) {
            val scheme = new PowerOfTwoLocalLayoutScheme()

            var nextOverviewLevel: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preprocessedRdd
            var nextZoom = -1
            val overviews = (1 to levels).reverse.map( level=>{
              var zoom_rdd = Pyramid.up(nextOverviewLevel,scheme,level,Pyramid.Options(resampleMethod = method))
              nextOverviewLevel = zoom_rdd._2
              val overViewGridBounds = nextOverviewLevel.metadata.gridBoundsFor(croppedExtent, clamp = true).toGridType[Int]
              val ( overViewTiffs: _root_.scala.collection.Map[Int, _root_.scala.Array[Byte]], cellType: CellType, detectedBandCount: Double, overViewSegmentCount: Int) = getCompressedTiles(nextOverviewLevel,overViewGridBounds, compression)
              val overviewTiff = toTiff(overViewTiffs, overViewGridBounds, nextOverviewLevel.metadata.tileLayout, compression, cellType, detectedBandCount, overViewSegmentCount)
              overviewTiff
            })

            overviews.toList

            /*if(levels>2) {
              val (lowerZoom,lowerOverviewLevel) = Pyramid.up(nextOverviewLevel,scheme,nextZoom)
              val stitched: Option[Raster[MultibandTile]] = lowerOverviewLevel.withContext(_.map(t=>(t._1.getComponent[SpatialKey](),t._2))).sparseStitch()
              List(overviewTiff,GeoTiffMultibandTile(stitched.get.tile))
            }else{*/
            //List(overviewTiff)
            //}
          }else{
            Nil
          }

        }else{
          Nil
        }
      val stacItemPath = FilenameUtils.removeExtension(path) + "_item.json"
      val metadata = new STACItem()
      metadata.asset(path)
      metadata.write(stacItemPath)
      writeTiff( path,tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, preprocessedRdd.metadata.tileLayout, compression, cellType, detectedBandCount, segmentCount,formatOptions = formatOptions, overviews = overviews)
      return Collections.singletonList(path)
    }finally {
      preprocessedRdd.unpersist()
    }



  }

  private def getCompressedTiles[K: SpatialComponent : Boundable : ClassTag](preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]],gridBounds: GridBounds[Int], compression: Compression) = {
    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
    val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

    val cols = tileLayout.tileCols
    val rows = tileLayout.tileRows

    val bandSegmentCount = totalCols * totalRows

    preprocessedRdd.sparkContext.setJobDescription(s"Write geotiff ${preprocessedRdd.metadata.toRasterExtent()} of type ${preprocessedRdd.metadata.cellType}")
    val totalBandCount = preprocessedRdd.sparkContext.longAccumulator("TotalBandCount")
    val typeAccumulator = new SetAccumulator[CellType]()
    preprocessedRdd.sparkContext.register(typeAccumulator, "CellType")
    val tmp = preprocessedRdd.flatMap { case (key: K, multibandTile: MultibandTile) => {
      var bandIndex = -1
      if (multibandTile.bandCount > 0) {
        totalBandCount.add(multibandTile.bandCount)
      }
      typeAccumulator.add(multibandTile.cellType)
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val layoutCol = key.getComponent[SpatialKey]._1
      val layoutRow = key.getComponent[SpatialKey]._2
      if (layoutCol >= totalCols || layoutRow >= totalRows || layoutCol < 0 && layoutRow < 0) {
        // Might as well throw an error:
        logger.warn(f"Unexpected key: (c=$layoutCol, r=$layoutRow) should be between (0,0) and (c=$totalCols, r=$totalRows)")
      }

      val theCompressor = compression.createCompressor(multibandTile.bandCount)
      multibandTile.bands.map {
        tile => {
          bandIndex += 1
          val bandSegmentOffset = bandSegmentCount * bandIndex
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset

          val bytes =
          if(cols != tile.cols || rows != tile.rows) {
            logger.error(s"Incorrect tile size in geotiff: ${tile.cols}x${tile.rows} " )
            tile.crop(cols,rows,Options(clamp = false, force = true)).toBytes()
          }else{
            tile.toBytes()
          }
          //tiff format seems to require that we provide 'full' tiles
          val compressedBytes = theCompressor.compress(bytes, 0)
          (index, compressedBytes)
        }

      }
    }
    }.collect()
    val tiffs: collection.Map[Int, Array[Byte]] = tmp.groupBy(_._1).map(_._2.last)


    preprocessedRdd.sparkContext.clearJobGroup()

    val cellType = {
      if (typeAccumulator.value.isEmpty) {
        preprocessedRdd.metadata.cellType
      } else {
        typeAccumulator.value.head
      }
    }
    println("Saving geotiff with Celltype: " + cellType)
    val detectedBandCount = if (totalBandCount.avg > 0) totalBandCount.avg else 1
    val segmentCount = (bandSegmentCount * detectedBandCount).toInt
    ( tiffs, cellType, detectedBandCount, segmentCount)
  }

  // This implementation does not properly work, output tiffs are not properly aligned and colors are also incorrect
  def saveRDDGenericTileGrid[K: SpatialComponent : Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], bandCount: Int, path: String, tileGrid: String, zLevel: Int = 6, cropBounds: Option[Extent] = Option.empty[Extent]) = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd, cropBounds)
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val compression = Deflate(zLevel)

    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(preprocessedRdd.metadata.extent, preprocessedRdd.metadata.crs))

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
          if (KeyBounds(tileBounds).includes(key.getComponent[SpatialKey])) true else false
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
        val minKey = keyBounds.get.minKey.getComponent[SpatialKey]

        val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
        val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

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

        val segmentCount = bandSegmentCount * detectedBandCount
        val newPath = newFilePath(path, name)
        writeTiff(newPath, tiffs, gridBounds, extent.intersection(croppedExtent).get, preprocessedRdd.metadata.crs, tileLayout, compression, cellType, detectedBandCount, segmentCount)
      }.collect()
      .toList
  }

  private def writeTiff(path: String, tiffs: collection.Map[Int, Array[Byte]], gridBounds: GridBounds[Int], croppedExtent: Extent, crs: CRS, tileLayout: TileLayout, compression: DeflateCompression, cellType: CellType, detectedBandCount: Double, segmentCount: Int, formatOptions:GTiffOptions = new GTiffOptions,overviews: List[GeoTiffMultibandTile] = Nil) = {
    logger.info(s"Writing geotiff to $path with type ${cellType.toString()} and bands $detectedBandCount")
    val tiffTile: GeoTiffMultibandTile = toTiff(tiffs, gridBounds, tileLayout, compression, cellType, detectedBandCount, segmentCount)
    val options = if(formatOptions.colorMap.isDefined){
      new GeoTiffOptions(colorMap = formatOptions.colorMap.map(IndexedColorMap.fromColorMap),colorSpace = ColorSpace.Palette)
    }else{
      val theColorspace = if(detectedBandCount==3) {
        ColorSpace.RGB
      }else{
        ColorSpace.BlackIsZero
      }
      new GeoTiffOptions(colorSpace = theColorspace)
    }

    val thegeotiff = new MultibandGeoTiff(tiffTile, croppedExtent, crs,formatOptions.tags,options,overviews = overviews.map(o=>MultibandGeoTiff(o,croppedExtent,crs,options = options.copy(subfileType = Some(ReducedImage)))))//.withOverviews(NearestNeighbor, List(4, 8, 16))

    writeGeoTiff(thegeotiff, path)
  }

  private def toTiff(tiffs:collection.Map[Int, Array[Byte]] , gridBounds: GridBounds[Int], tileLayout: TileLayout, compression: DeflateCompression, cellType: CellType, detectedBandCount: Double, segmentCount: Int) = {
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
      cellType
    )
    tiffTile
  }

  def saveStitched(
                    rdd: SRDD,
                    path: String,
                    cropBounds: Option[Map[String, Double]],
                    cropDimensions: Option[ArrayList[Int]],
                    compression: Compression): Extent = {
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

    val geoTiff = MultibandGeoTiff(adjusted, contextRDD.metadata.crs, GeoTiffOptions(compression))
      .withOverviews(NearestNeighbor, List(4, 8, 16))

    writeGeoTiff(geoTiff, path)
    adjusted.extent
  }

  def saveStitchedTileGrid(
                            rdd: SRDD,
                            path: String,
                            tileGrid: String,
                            cropBounds: Option[Map[String, Double]],
                            cropDimensions: Option[ArrayList[Int]],
                            compression: Compression)
  : java.util.List[(String, Extent)] = {
    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent,rdd.metadata.crs))

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      val extension = if(index>=0) path.substring(index) else ".tiff"
      val prefix = if(index>=0 ) path.substring(0, index) else "openEO"
      s"$prefix-$tileId$extension"
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

        (stitchAndWriteToTiff(tiles, filePath, layout, crs, extent, croppedExtent, cropDimensions, compression), extent)
      }.collect()
      .toList.asJava
  }

  private def stitchAndWriteToTiff(tiles: Iterable[(SpatialKey, MultibandTile)], filePath: String, layout: LayoutDefinition, crs: CRS, geometry: Geometry, croppedExtent: Option[Extent], cropDimensions: Option[java.util.ArrayList[Int]], compression: Compression) = {
    val raster: Raster[MultibandTile] = ContextSeq(tiles, layout).stitch()

    val re = raster.rasterExtent
    val alignedExtent = re.createAlignedGridExtent(geometry.extent).extent

    val stitched: Raster[MultibandTile] = raster.mask(geometry).crop(alignedExtent)

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

    val geotiff = MultibandGeoTiff(adjusted, crs, GeoTiffOptions(compression))
      .withOverviews(NearestNeighbor, List(4, 8, 16))

    writeGeoTiff(geotiff, filePath)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                 ): JList[(String, String, Extent)] =
    saveSamples(rdd, path, polygons, sampleNames, compression, None)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                  filenamePrefix: Option[String],
                 ): JList[(String, String, Extent)] = {
    val reprojected = ProjectedPolygons.reproject(polygons, rdd.metadata.crs)
    val features = sampleNames.asScala.zip(reprojected.polygons)
    groupByFeatureAndWriteToTiff(rdd, Option.empty, features, path, Option.empty, compression, filenamePrefix)
  }

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   compression: Compression,
                                   filenamePrefix: Option[String],
                                  ): java.util.List[(String, String, Extent)] =
    geotrellis.geotiff.saveStitchedTileGridTemporal(rdd, path, tileGrid, Option.empty, Option.empty, compression, filenamePrefix)

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   compression: Compression,
                                  ): java.util.List[(String, String, Extent)] =
    geotrellis.geotiff.saveStitchedTileGridTemporal(rdd, path, tileGrid, Option.empty, Option.empty, compression)

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   cropBounds: Option[Map[String, Double]],
                                   cropDimensions: Option[ArrayList[Int]],
                                   compression: Compression,
                                   filenamePrefix: Option[String] = None,
                                  ): java.util.List[(String, String, Extent)] = {
    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent, rdd.metadata.crs))
      .map { case (s, extent) => (s, extent.toPolygon()) }
    groupByFeatureAndWriteToTiff(rdd, cropBounds, features, path, cropDimensions, compression, filenamePrefix)
  }

  private def groupByFeatureAndWriteToTiff(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                           cropBounds: Option[java.util.Map[String, Double]],
                                           features: Seq[(String, Geometry)],
                                           path: String,
                                           cropDimensions: Option[ArrayList[Int]],
                                           compression: Compression,
                                           filenamePrefix: Option[String] = None,
                                          ): java.util.List[(String, String, Extent)] = {
    val featuresBC: Broadcast[Seq[(String, Geometry)]] = SparkContext.getOrCreate().broadcast(features)

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs

    rdd
      .flatMap { case (key, tile) => featuresBC.value
        .filter { case (_, geometry) => layout.mapTransform.keysForGeometry(geometry) contains key.spatialKey }
        .map { case (name, geometry) => ((name, (geometry, key.time)), (key.spatialKey, tile)) }
      }
      .groupByKey()
      .map { case ((name, (geometry, time)), tiles) =>
        val filename = s"${filenamePrefix.getOrElse("openEO")}_${DateTimeFormatter.ISO_DATE.format(time)}_$name.tif"
        val filePath = Paths.get(path).resolve(filename).toString
        val timestamp = time format DateTimeFormatter.ISO_ZONED_DATE_TIME
        (stitchAndWriteToTiff(tiles, filePath, layout, crs, geometry, croppedExtent, cropDimensions, compression),
          timestamp, geometry.extent)
      }
      .collect()
      .toList.asJava
  }

  private def writeGeoTiff(geoTiff: MultibandGeoTiff, path: String): String = {
    if (path.startsWith("s3:/")) {
      val correctS3Path = path.replaceFirst("s3:/(?!/)", "s3://")
      val s3Uri = new AmazonS3URI(correctS3Path)

      import java.nio.file.Files

      val tempFile = Files.createTempFile(null, null)
      geoTiff.write(tempFile.toString, optimizedOrder = true)
      val objectRequest = PutObjectRequest.builder
        .bucket(s3Uri.getBucket)
        .key(s3Uri.getKey)
        .build

      CreoS3Utils.getCreoS3Client().putObject(objectRequest, RequestBody.fromFile(tempFile))
      correctS3Path

    } else {
      geoTiff.write(path, optimizedOrder = true)
      path
    }

  }


  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }
}
