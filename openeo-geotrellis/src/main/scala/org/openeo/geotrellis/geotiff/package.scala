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
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.s3._
import geotrellis.util._
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.openeo.geotrellis
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.netcdf.NetCDFRDDWriter.fixedTimeOffset
import org.openeo.geotrellis.stac.STACItem
import org.openeo.geotrellis.tile_grid.TileGrid
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import spire.math.Integral
import spire.syntax.cfor.cfor

import java.nio.channels.FileChannel
import java.nio.file.{FileAlreadyExistsException, Files, NoSuchFileException, Path, Paths}
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util.{ArrayList, Collections, Map, List => JList}
import scala.collection.JavaConverters._
import scala.reflect._
import scala.util.control.Breaks.{break, breakable}

package object geotiff {

  private val logger = LoggerFactory.getLogger(getClass)
  private val secondsPerDay = 86400L

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


  def saveRDDTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                      path: String,
                      zLevel: Int = 6,
                      cropBounds: Option[Extent] = Option.empty[Extent],
                      formatOptions: GTiffOptions = new GTiffOptions
                     ): java.util.List[(String, String, Extent)] = {
    rdd.sparkContext.setCallSite(s"save_result(GTiff, temporal)")
    val ret = saveRDDTemporalAllowAssetPerBand(rdd, path, zLevel, cropBounds, formatOptions).asScala
    logger.warn("Calling backwards compatibility version for saveRDDTemporalConsiderAssetPerBand")
    //    val duplicates = ret.groupBy(_._2).filter(_._2.size > 1)
    //    if (duplicates.nonEmpty) {
    //      throw new Exception(s"Multiple returned files with same timestamp: ${duplicates.keys.mkString(", ")}")
    //    }
    ret.map(t => (t._1, t._2, t._3)).asJava
  }

  /**
   * Save temporal rdd, on the executors
   *
   * @param rdd
   * @param path
   * @param zLevel
   * @param cropBounds
   */
  //noinspection ScalaWeakerAccess
  def saveRDDTemporalAllowAssetPerBand(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                          path: String,
                                          zLevel: Int = 6,
                                          cropBounds: Option[Extent] = Option.empty[Extent],
                                          formatOptions: GTiffOptions = new GTiffOptions
                                         ): java.util.List[(String, String, Extent, java.util.List[Int])] = {
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
    val bandLabels = formatOptions.tags.bandTags.map(_("DESCRIPTION"))

    val res = preprocessedRdd.flatMap { case (key: SpaceTimeKey, multibandTile: MultibandTile) =>
      var bandIndex = -1
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val theCompressor = compression.createCompressor(multibandTile.bandCount)
      multibandTile.bands.map {
        tile =>
          bandIndex += 1
          val layoutCol = key.getComponent[SpatialKey]._1
          val layoutRow = key.getComponent[SpatialKey]._2
          val bandSegmentOffset = bandSegmentCount * (if (formatOptions.separateAssetPerBand) 0 else bandIndex)
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
          //tiff format seems to require that we provide 'full' tiles
          val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
          val compressedBytes = theCompressor.compress(bytes, 0)

          val isDays = Duration.between(fixedTimeOffset, key.time).getSeconds % secondsPerDay == 0
          val timePieceSlug = if (isDays) {
            "_" + DateTimeFormatter.ISO_DATE.format(key.time)
          } else {
            // ':' is not valid in a Windows filename
            "_" + DateTimeFormatter.ISO_ZONED_DATE_TIME.format(key.time).replace(":", "").replace("-", "")
          }
          // TODO: Get band names from metadata?
          val bandPiece = if (formatOptions.separateAssetPerBand) "_" + bandLabels(bandIndex) else ""
          //noinspection RedundantBlock
          val filename = s"${formatOptions.filenamePrefix}${timePieceSlug}${bandPiece}.tif"

          val timestamp = DateTimeFormatter.ISO_ZONED_DATE_TIME.format(key.time)
          val tiffBands = if (formatOptions.separateAssetPerBand) 1 else multibandTile.bandCount
          ((filename, timestamp, tiffBands), (index, (multibandTile.cellType, compressedBytes), bandIndex))
      }
    }.groupByKey().map { case ((filename: String, timestamp: String, tiffBands:Int), sequence) =>
      val cellTypes = sequence.map(_._2._1).toSet
      val tiffs: Predef.Map[Int, Array[Byte]] = sequence.map(tuple => (tuple._1, tuple._2._2)).toMap
      val bandIndices = sequence.map(_._3).toSet.toList.asJava

      val segmentCount = bandSegmentCount * tiffBands

      // Each executor writes to a unique folder to avoid conflicts:
      val uniqueFolderName = "tmp" + java.lang.Long.toUnsignedString(new java.security.SecureRandom().nextLong())
      val base = Paths.get(path + "/" + uniqueFolderName)
      Files.createDirectories(base)
      val thePath = base.resolve(filename).toString

      // filter band tags that match bandIndices
      val fo = formatOptions.deepClone()
      val newBandTags = formatOptions.tags.bandTags.zipWithIndex
        .filter { case (_, bandIndex) => bandIndices.contains(bandIndex) }
        .map { case (bandTags, _) => bandTags }
      fo.setBandTags(newBandTags)

      val correctedPath = writeTiff(thePath, tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs,
        tileLayout, compression, cellTypes.head, tiffBands, segmentCount, fo,
      )
      (correctedPath, timestamp, croppedExtent, bandIndices)
    }.collect().map({
      case (absolutePath, timestamp, croppedExtent, bandIndices) =>
        // Move output file to standard location. (On S3, a move is more a copy and delete):
        val relativePath = Path.of(path).relativize(Path.of(absolutePath)).toString
        val destinationPath = Path.of(path).resolve(relativePath.substring(relativePath.indexOf("/") + 1))
        Files.move(Path.of(absolutePath), destinationPath)
        (destinationPath.toString, timestamp, croppedExtent, bandIndices)
    }).toList.asJava

    // Clean up failed tasks:
    Files.list(Path.of(path)).forEach { p =>
      if (Files.isDirectory(p) && p.getFileName.toString.startsWith("tmp")) {
        FileUtils.deleteDirectory(p.toFile)
      }
    }
    res
  }


  def saveRDD(rdd: MultibandTileLayerRDD[SpatialKey],
              bandCount: Int,
              path: String,
              zLevel: Int = 6,
              cropBounds: Option[Extent] = Option.empty[Extent],
              formatOptions: GTiffOptions = new GTiffOptions
             ): java.util.List[String] = {
    rdd.sparkContext.setCallSite(s"save_result(GTiff, spatial, $bandCount)")
    val tmp = saveRDDAllowAssetPerBand(rdd, bandCount, path, zLevel, cropBounds, formatOptions).asScala
    logger.warn("Calling backwards compatibility version for saveRDDAllowAssetPerBand")
    //    if (tmp.size() > 1) {
    //      throw new Exception("Multiple returned files, probably meant to call saveRDDAllowAssetPerBand")
    //    }
    tmp.map(_._1).asJava
  }

  //noinspection ScalaWeakerAccess
  def saveRDDAllowAssetPerBand(rdd: MultibandTileLayerRDD[SpatialKey],
                               bandCount: Int,
                               path: String,
                               zLevel: Int = 6,
                               cropBounds: Option[Extent] = Option.empty[Extent],
                               formatOptions: GTiffOptions = new GTiffOptions
                              ): java.util.List[(String, java.util.List[Int])] = {
    if (formatOptions.separateAssetPerBand) {
      val bandLabels = formatOptions.tags.bandTags.map(_("DESCRIPTION"))
      val layout = rdd.metadata.layout
      val crs = rdd.metadata.crs
      val extent = rdd.metadata.extent
      val compression = Deflate(zLevel)

      val rdd_per_band = rdd.flatMap { case (key: SpatialKey, multibandTile: MultibandTile) =>
        var bandIndex = -1
        multibandTile.bands.map {
          tile =>
            bandIndex += 1
            val t = _root_.geotrellis.raster.MultibandTile(Seq(tile))
            val name = formatOptions.filenamePrefix + "_" + bandLabels(bandIndex) + ".tif"
            ((name, bandIndex), (key, t))
        }
      }
      val res = rdd_per_band.groupByKey().map { case ((name, bandIndex), tiles) =>
        val uniqueFolderName = "tmp" + java.lang.Long.toUnsignedString(new java.security.SecureRandom().nextLong())
        val fixedPath =
          if (path.endsWith("out")) {
            val base = path.substring(0, path.length - 3) + uniqueFolderName + "/"
            Files.createDirectories(Path.of(base))
            base + name
          }
          else {
            path
          }

        val fo = formatOptions.deepClone()
        // Keep only one band tag
        val newBandTags = List(formatOptions.tags.bandTags(bandIndex))
        fo.setBandTags(newBandTags)

        (stitchAndWriteToTiff(tiles, fixedPath, layout, crs, extent, None, None, compression, Some(fo)),
          Collections.singletonList(bandIndex))
      }.collect().map({
        case (absolutePath, y) =>
          if (path.endsWith("out")) {
            // Move output file to standard location. (On S3, a move is more a copy and delete):
            val beforeOut = path.substring(0, path.length - "out".length)
            val relativePath = Path.of(beforeOut).relativize(Path.of(absolutePath)).toString
            val destinationPath = beforeOut + relativePath.substring(relativePath.indexOf("/") + 1)
            Files.move(Path.of(absolutePath), Path.of(destinationPath))
            (destinationPath, y)
          } else {
            (absolutePath, y)
          }
      }).toList.sortBy(_._1).asJava
      // Clean up failed tasks:
      val beforeOut = path.substring(0, path.length - "out".length)
      Files.list(Path.of(beforeOut)).forEach { p =>
        if (Files.isDirectory(p) && p.getFileName.toString.startsWith("tmp")) {
          FileUtils.deleteDirectory(p.toFile)
        }
      }
      res
    } else {
      val tmp = saveRDDGeneric(rdd, bandCount, path, zLevel, cropBounds, formatOptions).asScala
      tmp.map(t => (t, (0 until bandCount).toList.asJava)).asJava
    }
  }

  def saveRDDTileGrid(rdd:MultibandTileLayerRDD[SpatialKey], bandCount:Int, path:String, tileGrid: String, zLevel:Int=6,cropBounds:Option[Extent]=Option.empty[Extent]) = {
    saveRDDGenericTileGrid(rdd,bandCount, path, tileGrid, zLevel, cropBounds)
  }

  private def gridBoundsFor(re: RasterExtent, subExtent: Extent, clamp: Boolean = true): GridBounds[Int] = {
    // West and North boundaries are a simple mapToGrid call.
    val colMin: Int = re.mapXToGrid(subExtent.xmin)
    val rowMin: Int = re.mapYToGrid(subExtent.ymax)

    // If South East corner is on grid border lines, we want to still only include
    // what is to the West and\or North of the point. However if the border point
    // is not directly on a grid division, include the whole row and/or column that
    // contains the point.
    val colMax: Long = Integral[Long].fromLong {
      val colMaxDouble = re.mapXToGridDouble(subExtent.xmax)

      if (math.abs(colMaxDouble - GridExtent.floorWithTolerance(colMaxDouble)) < GridExtent.epsilon)
        GridExtent.floorWithTolerance(colMaxDouble).toLong - 1L
      else
        GridExtent.floorWithTolerance(colMaxDouble).toLong
    }

    val rowMax: Long = Integral[Long].fromLong {
      val rowMaxDouble = re.mapYToGridDouble(subExtent.ymin)

      if (math.abs(rowMaxDouble - GridExtent.floorWithTolerance(rowMaxDouble)) < GridExtent.epsilon)
        GridExtent.floorWithTolerance(rowMaxDouble).toLong - 1L
      else
        GridExtent.floorWithTolerance(rowMaxDouble).toLong
    }

    if (clamp)
      GridBounds(
        colMin = colMin.max(0).min(re.cols - 1).intValue(),
        rowMin = rowMin.max(0).min(re.rows - 1).intValue(),
        colMax = colMax.max(0).min(re.cols - 1).intValue(),
        rowMax = rowMax.max(0).min(re.rows - 1).intValue())
    else
      GridBounds[Int](colMin, rowMin, colMax.toInt, rowMax.toInt)
  }

  def preProcess[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K],cropBounds:Option[Extent]): (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = {
    val re = rdd.metadata.toRasterExtent()
    /**
     * CLAMPING EP-4150
     * Gridbounds are clamped to the actually available rasterextent, this means that we don't add empty data if somehow a cropping bounds is provided that is larger than the actual datacube.
     * CroppedExtent needs to match exactly with whatever gridbounds that we got, so there we do not clamp. So even though we clamp when computing gridbounds, it can still have an extent that is larger than rasterextent!!!
     */
    var gridBounds = gridBoundsFor(re,cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = false)
    val filtered = new OpenEOProcesses().filterEmptyTile(rdd)
    val preprocessedRdd = {
      if (gridBounds.colMin != 0 || gridBounds.rowMin != 0) {
        logger.info(s"Gridbounds requires reprojection: ${gridBounds}")
        val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), rdd.metadata.tileCols,rdd.metadata.tileRows)
        val retiledRDD = filtered.reproject(rdd.metadata.crs, geotiffLayout)._2.crop(croppedExtent, Options(force = false))

        gridBounds = gridBoundsFor(retiledRDD.metadata.toRasterExtent(),cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
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

      val fixedPath =
      if(path.endsWith("out")) {
        path.substring(0,path.length-3) + formatOptions.filenamePrefix + ".tif"
      }else{
        //what if this is a directory?
        path
      }
      val stacItemPath = FilenameUtils.removeExtension(fixedPath) + "_item.json"
      val metadata = new STACItem()
      metadata.asset(fixedPath)
      metadata.write(stacItemPath)
      val finalPath = writeTiff( fixedPath,tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, preprocessedRdd.metadata.tileLayout, compression, cellType, detectedBandCount, segmentCount,formatOptions = formatOptions, overviews = overviews)
      return Collections.singletonList(finalPath)
    }finally {
      preprocessedRdd.unpersist()
    }



  }

  private def getCompressedTiles[K: SpatialComponent : Boundable : ClassTag](preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]],gridBounds: GridBounds[Int], compression: Compression): (collection.Map[Int, Array[Byte]], CellType, Double, Int) = {
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
    val tiffs: collection.Map[Int, Array[Byte]] = preprocessedRdd.flatMap { case (key: K, multibandTile: MultibandTile) => {
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
    }.collectAsMap()


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

  private def stitchAndWriteToTiff(tiles: Iterable[(SpatialKey, MultibandTile)], filePath: String,
                                   layout: LayoutDefinition, crs: CRS, geometry: Geometry,
                                   croppedExtent: Option[Extent], cropDimensions: Option[java.util.ArrayList[Int]],
                                   compression: Compression, formatOptions: Option[GTiffOptions] = None
                                  ) = {
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

    val fo = formatOptions match {
      case Some(fo) => fo
      case None =>
        val fo = new GTiffOptions()
        // If no formatOptions was specified, the default was to generate pyramids
        fo.overviews = "ALL"
        fo
    }
    var geotiff = MultibandGeoTiff(adjusted.tile, adjusted.extent, crs,
      fo.tags, GeoTiffOptions(compression))
    val gridBounds = adjusted.extent
    if (fo.overviews.toUpperCase == "ALL" ||
      fo.overviews.toUpperCase == "AUTO" && (gridBounds.width > 1024 || gridBounds.height > 1024)
    ) {
      geotiff = geotiff.withOverviews(NearestNeighbor, List(4, 8, 16))
    }
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

  def writeGeoTiff(geoTiff: MultibandGeoTiff, path: String): String = {
    val tempFile = getTempFile(null, ".tif")
    geoTiff.write(tempFile.toString, optimizedOrder = true)

    if (path.startsWith("s3:/")) {
      val correctS3Path = path.replaceFirst("s3:/(?!/)", "s3://")
      uploadToS3(tempFile, correctS3Path)
    } else {
      moveOverwriteWithRetries(tempFile, Path.of(path))
      path
    }
  }

  def moveOverwriteWithRetries(oldPath: Path, newPath: Path): Unit = {
    var try_count = 1
    breakable {
      while (true) {
        try {
          if (newPath.toFile.exists()) {
            // It might be a partial result of a previous failing task.
            logger.info(f"Will replace $newPath. (try $try_count)")
          }
          Files.deleteIfExists(newPath)
          Files.move(oldPath, newPath)
          break
        } catch {
          case e: FileAlreadyExistsException =>
            // Here if another executor wrote the file between the delete and the move statement.
            try_count += 1
            if (try_count > 5) {
              throw e
            }
        }
      }
    }
  }

  def uploadToS3(localFile: Path, s3Path: String) = {
    val s3Uri = new AmazonS3URI(s3Path)
    val objectRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build

    CreoS3Utils.getCreoS3Client().putObject(objectRequest, RequestBody.fromFile(localFile))
    s3Path
  }

  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }

  def testColormap(sc: SparkContext): Unit = {
    //    val sc = SparkContext.getOrCreate
    val mCopy = "0.0:aec7e8ff;1.0:d62728ff;2.0:f7b6d2ff;3.0:dbdb8dff;4.0:c7c7c7ff".split(";").map(x => {
      val l = x.split(":")
      // parseUnsignedInt, because there is no minus sign in the hexadecimal representation.
      // When casting an unsigned int to an int, it will correctly overflow
      Tuple2(l(0).toDouble, Integer.parseUnsignedInt(l(1), 16))
    }).toMap
    val colorMap = new _root_.geotrellis.raster.render.DoubleColorMap(mCopy)
    val formatOptions = new GTiffOptions()
    formatOptions.setColorMap(colorMap)


    val spire = new algebra.instances.DoubleAlgebra()

    val data = sc.parallelize(Seq(1, 2, 3))
    val result = data.map(x => {
      println(spire)
      println(formatOptions)
      x
    })
    result.collect()
    print("test done")
  }
}
