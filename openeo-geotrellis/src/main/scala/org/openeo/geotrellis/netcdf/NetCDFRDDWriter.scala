package org.openeo.geotrellis.netcdf

//import ucar.ma2.Array
import geotrellis.layer.TileLayerMetadata.toLayoutDefinition
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.store.hadoop.KeyPartitioner
import geotrellis.store.s3.AmazonS3URI
import geotrellis.util._
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.openeo.geotrellis.geotiff.{getCreoS3Client, preProcess}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import org.openeo.geotrelliscommon.ByKeyPartitioner
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import ucar.ma2.{ArrayDouble, ArrayInt, DataType}
import ucar.nc2.write.Nc4ChunkingDefault
import ucar.nc2.{Attribute, Dimension, NetcdfFileWriter, Variable}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util
import java.util.{ArrayList, Collections}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag


object NetCDFRDDWriter {

  val logger = LoggerFactory.getLogger(NetCDFRDDWriter.getClass)

  val fixedTimeOffset = ZonedDateTime.parse("1990-01-01T00:00:00Z")
  val LON = "lon"
  val LAT = "lat"
  val X = "x"
  val Y = "y"
  val TIME = "t"
  val cfDatePattern = DateTimeFormatter.ofPattern("YYYY-MM-dd")

  class OpenEOChunking(deflateLevel:Int) extends Nc4ChunkingDefault(deflateLevel,false) {

    override def computeChunking(v: Variable): Array[Long] = {
      val attributeBasedChunking = super.computeChunkingFromAttribute(v)
      if(attributeBasedChunking!=null)
        super.convertToLong(attributeBasedChunking)
        else{
        super.computeChunking(v)
      }

    }
  }

  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }

  def writeRasters(rdd:Object,path:String,options:NetCDFOptions): java.util.List[String] = {

    rdd match {
      case rdd1 if rdd.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        saveSingleNetCDFGeneric(rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]], path, options)
      case rdd2 if rdd.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey]  =>
        saveSingleNetCDFGeneric(rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]], path, options)
      case _ => throw new IllegalArgumentException("Unsupported rdd type to write to netCDF: ${rdd}")
    }

  }

  def saveSingleNetCDFSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                       path: String,
                       bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String],
                       zLevel:Int
                      ): java.util.List[String] = {
    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, zLevel)
  }

  def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String],
                       zLevel:Int
                 ): java.util.List[String] = {

    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, zLevel)
  }

  def saveSingleNetCDFGeneric[K: SpatialComponent: Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], path:String, options:NetCDFOptions): java.util.List[String] = {
    saveSingleNetCDFGeneric(rdd,path,options.bandNames.orNull,options.dimensionNames.orNull,options.attributes.orNull,options.zLevel,options.cropBounds)
  }

  def saveSingleNetCDFGeneric[K: SpatialComponent: Boundable : ClassTag](rdd: MultibandTileLayerRDD[K],
                       path: String,
                       bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String],
                       zLevel:Int,
                       cropBounds:Option[Extent]= None
                      ): java.util.List[String] = {

    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd,cropBounds)
    val extent = preProcessResult._2
    val preProcessedRdd = preProcessResult._3

    val rasterExtent = RasterExtent(extent = extent, cellSize = preProcessedRdd.metadata.cellSize)

    val cachedRDD: RDD[(K, MultibandTile)] = cacheAndRepartition(preProcessedRdd)

    val dates =
      cachedRDD.keys.flatMap {
        case key: SpaceTimeKey => Some(Duration.between(fixedTimeOffset, key.time).toDays.toInt)
        case _ =>
          None
      }.distinct().collect().sorted.toList


    val intermediatePath =
      if (path.startsWith("s3:/")) {
        Files.createTempFile(null, null).toString
      }else{
        path
      }

    var netcdfFile: NetcdfFileWriter = null
    for(tuple <- cachedRDD.toLocalIterator){

      val cellType = tuple._2.cellType
      val timeDimIndex =
        if(dates.nonEmpty){
          val timeOffset = Duration.between(fixedTimeOffset, tuple._1.asInstanceOf[SpaceTimeKey].time).toDays.toInt
          dates.indexOf(timeOffset)
        }else{
          -1
        }


      if(netcdfFile == null){
        netcdfFile = setupNetCDF(intermediatePath, rasterExtent, null, bandNames, preProcessedRdd.metadata.crs, cellType,dimensionNames,attributes,zLevel,writeTimeDimension = dates.nonEmpty)
      }
      val multibandTile = tuple._2

      for (bandIndex <- bandNames.asScala.indices) {

        if(bandIndex < multibandTile.bandCount){
          //gridBoundsFor considers the south/east border as _exclusive_ which means a row of pixels can get dropped
          val gridExtent = rasterExtent.gridBoundsFor(tuple._1.getComponent[SpatialKey].extent(preProcessedRdd.metadata))
          if(gridExtent.colMax >= rasterExtent.cols || gridExtent.rowMax >= rasterExtent.rows){
            logger.warn("Can not write tile beyond raster bounds: " + gridExtent)
          }else{
            val origin: Array[Int] = if(timeDimIndex>=0){
              scala.Array(timeDimIndex.toInt, gridExtent.rowMin.toInt, gridExtent.colMin.toInt)
            }else{
              scala.Array( gridExtent.rowMin.toInt, gridExtent.colMin.toInt)
            }
            val variable = bandNames.get(bandIndex)

            var tile = multibandTile.band(bandIndex)

            if(gridExtent.colMin + tile.cols > rasterExtent.cols || gridExtent.rowMin + tile.rows > rasterExtent.rows){
              tile = tile.crop(rasterExtent.cols-gridExtent.colMin,rasterExtent.rows-gridExtent.rowMin,raster.CropOptions(force=true))
              logger.warn(s"Cropping output tile to avoid going out of variable (${variable}) bounds ${gridExtent}.")
            }
            try{
              writeTile(variable, origin, tile, netcdfFile)
            }catch {
              case t: IOException => {
                logger.error("Failed to write subtile: " + gridExtent + " to variable: " + variable + " with shape: " + netcdfFile.findVariable(variable).getShape.mkString("Array(", ", ", ")"),t)
              }
              case t: Throwable =>  throw t
            }
          }
        }
      }
    }

    if(dates.nonEmpty) {
      val timeDimName = if(dimensionNames!=null) dimensionNames.getOrDefault(TIME,TIME) else TIME
      writeTime(timeDimName, netcdfFile, dates)
    }

    if(netcdfFile!=null) {
      netcdfFile.close()
    }else{
      logger.error(s"No netCDF written to ${path}, the datacube was empty.")
    }
    cachedRDD.unpersist(blocking = false)
    if (path.startsWith("s3:/")) {
      uploadToS3(path, intermediatePath)
    }

    return Collections.singletonList(path)
  }


  private def cacheAndRepartition[K](rdd: MultibandTileLayerRDD[K]) = {
    val cachedRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val count = cachedRDD.count()
    cachedRDD.name = s"netCDF RDD ${count} elements"
    logger.info(s"Writing NetCDF from rdd with : ${count} elements and ${rdd.getNumPartitions} partitions.")
    val elementsPartitionRatio = count / rdd.getNumPartitions
    val shuffledRDD =
      if (elementsPartitionRatio < 4) {
        //avoid iterating over many empty partitions
        cachedRDD.repartition(math.max(1, (count / 4).toInt))()
      } else {
        cachedRDD
      }
    shuffledRDD
  }

  private def writeTile(variable: String, origin: Array[Int], tile: Tile, netcdfFile: NetcdfFileWriter) = {
    val cols = tile.cols
    val rows = tile.rows

    val geotrellisArrayTile = tile.toArrayTile()

    val shape = if(origin.length==3) scala.Array[Int](1, rows, cols) else scala.Array[Int]( rows, cols)
    val bandArray =
      geotrellisArrayTile match {
        case t: BitArrayTile => ucar.ma2.Array.factory(DataType.BOOLEAN, shape, t.array)
        case t: ByteArrayTile => ucar.ma2.Array.factory(DataType.BYTE, shape, t.array)
        case t: UByteArrayTile => ucar.ma2.Array.factory(DataType.UBYTE, shape, t.array)
        case t: ShortArrayTile => ucar.ma2.Array.factory(DataType.SHORT, shape, t.array)
        case t: UShortArrayTile => ucar.ma2.Array.factory(DataType.USHORT, shape, t.array)
        case t: IntArrayTile => ucar.ma2.Array.factory(DataType.INT, shape, t.array)
        case t: FloatArrayTile => ucar.ma2.Array.factory(DataType.FLOAT, shape, t.array)
        case t: DoubleArrayTile => ucar.ma2.Array.factory(DataType.DOUBLE, shape, t.array)
      }


    netcdfFile.write(variable, origin, bandArray)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String]
                  ): java.util.List[String] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames = null, attributes = null)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String]
                 ): java.util.List[String] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.zip(reprojected.polygons)
    logger.info(s"Using metadata: ${rdd.metadata}.")
    logger.info(s"Using features: ${features}.")
    groupByFeatureAndWriteToNetCDF(rdd, features, path, bandNames, dimensionNames, attributes)
  }

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String]
                 ): java.util.List[String] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToNetCDFSpatial(rdd,  features,path,bandNames,dimensionNames,attributes)

  }

  private def groupByFeatureAndWriteToNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey], features: Seq[(String, Geometry)],
                                           path:String,bandNames: ArrayList[String],
                                           dimensionNames: java.util.Map[String,String],
                                           attributes: java.util.Map[String,String]): util.List[String] = {
    val featuresBC: Broadcast[Seq[(String, Geometry)]] = SparkContext.getOrCreate().broadcast(features)

    val crs = rdd.metadata.crs
    val groupedBySample = stitchRDDBySample(rdd, featuresBC)
    logger.info(s"Writing ${groupedBySample.count()} samples to disk.")
    groupedBySample.map { case (name, tiles: Iterable[(Long, Raster[MultibandTile])]) =>
        val outputAsPath: Path = getSamplePath(name, path)
        val filePath = outputAsPath.toString

        // Sort by date before writing.
        val sorted = tiles.toSeq.sortBy(_._1)
        val dates = sorted.map(  t=> ZonedDateTime.ofInstant(t._1, ZoneOffset.UTC))
        logger.info(s"Writing ${name} with dates ${dates}.")
        try{
          writeToDisk(sorted.map(_._2), dates, filePath, bandNames, crs, dimensionNames, attributes)
          filePath
        }catch {
          case t: IOException => {
            handleSampleWriteError(t, name, outputAsPath)
          }
          case t: Throwable =>  throw t
        }

      }.collect()
      .toList.asJava
  }

  private def stitchRDDBySample(rdd: MultibandTileLayerRDD[SpaceTimeKey], featuresBC: Broadcast[Seq[(String, Geometry)]]) = {
    val layout = rdd.metadata.layout

    val sampleNames = featuresBC.value.map { case (sampleName, _) => sampleName }
    logger.info(s"Grouping result by ${featuresBC.value.size} features to write netCDFs.")
    val filtered = new OpenEOProcesses().filterEmptyTile(rdd)
    //the logging below is rather expensive
    //logger.info(s"Filtered out ${rdd.count() - filtered.count()} empty tiles. ${rdd.count()} -> ${filtered.count()}")
    val groupedByInstant = filtered.flatMap {
      case (key, tile) => featuresBC.value.filter { case (_, geometry) =>
        layout.mapTransform.keysForGeometry(geometry) contains key.spatialKey
      }.map { case (sampleName, geometry) =>
        val keyExtent = layout.mapTransform.keyToExtent(key.spatialKey)
        val sample = tile.mask(keyExtent, geometry)
        ((sampleName, key.instant), (key.spatialKey, sample))
      }
    }.groupByKey()
    val stitchedByInstant = groupedByInstant.map(sample => {
        val tiles: Iterable[(SpatialKey, MultibandTile)] = sample._2
        val raster: Raster[MultibandTile] = ContextSeq(tiles, layout).stitch()
        (sample._1, raster)
      }
    )
    val keyedBySample = stitchedByInstant.map { case ((sampleName, instant), raster) => (sampleName, (instant, raster)) }
    val groupedBySample = keyedBySample.groupByKey(new ByKeyPartitioner(sampleNames.toArray))
    groupedBySample
  }

  private def groupRDDBySample[K: SpatialComponent: Boundable: ClassTag](rdd: MultibandTileLayerRDD[K],featuresBC: Broadcast[List[(String, Extent)]]) = {
    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
    val keys = featuresBC.value.map(_._1)
    logger.info(s"Grouping result by ${featuresBC.value.size} features to write netCDFs.")
    rdd.flatMap {
      case (key, tile) => featuresBC.value.filter { case (_, extent) =>
        val tileBounds = layout.mapTransform(extent)

        if (KeyBounds(tileBounds).includes(key.getComponent[SpatialKey])) true else false
      }.map { case (name, extent) =>
        (name, (extent,(key, tile)))
      }
    }.groupByKey(new KeyPartitioner(keys.toArray)).map {
      case (name, tiles) => {
        val extent = tiles.head._1
        ((name, ProjectedExtent(extent,crs)),tiles.map(_._2))
      }
    }
  }

  private def stitchAndCropTiles(tilesForDate: Iterable[(SpatialKey, MultibandTile)], cropExtent: ProjectedExtent, layout: LayoutDefinition) = {
    val raster: Raster[MultibandTile] = ContextSeq(tilesForDate, layout).stitch()
    val re: RasterExtent = raster.rasterExtent
    val alignedExtent = re.createAlignedGridExtent(cropExtent.extent).extent
    val sample = raster.crop(alignedExtent)
    sample
  }

  private def groupByFeatureAndWriteToNetCDFSpatial(rdd: MultibandTileLayerRDD[SpatialKey], features: List[(String, Extent)],
                                           path:String, bandNames: ArrayList[String],
                                           dimensionNames: java.util.Map[String,String],
                                           attributes: java.util.Map[String,String]) = {
    val featuresBC: Broadcast[List[(String, Extent)]] = SparkContext.getOrCreate().broadcast(features)
    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs

    groupRDDBySample(rdd,featuresBC)
      .map { case ((name, extent), tiles) =>

        val outputAsPath: Path = getSamplePath(name, path)
        val sample: Raster[MultibandTile] = stitchAndCropTiles(tiles, extent, layout)

        try{
          writeToDisk(Seq(sample),null,outputAsPath.toString,bandNames,crs,dimensionNames,attributes)
          outputAsPath.toString
        }catch {
          case t: IOException => {
            handleSampleWriteError(t, name, outputAsPath)
          }
          case t: Throwable =>  throw t
        }


      }.collect()
      .toList.asJava
  }

  private def handleSampleWriteError(t: IOException, sampleName: String, outputAsPath: Path) = {
    logger.error("Failed to write sample: " + sampleName, t)
    val theFile = outputAsPath.toFile
    if (theFile.exists()) {
      val failedPath = outputAsPath.resolveSibling(outputAsPath.getFileName().toString + "_FAILED")
      Files.move(outputAsPath, failedPath)
      failedPath.toString
    } else {
      outputAsPath.toString
    }
  }

  private def getSamplePath(sampleName: String, outputDirectory: String) = {
    val filename = s"openEO_${sampleName}.nc"
    val outputAsPath = Paths.get(outputDirectory).resolve(filename)
    outputAsPath
  }

  def writeToDisk(rasters: Seq[Raster[MultibandTile]], dates:Seq[ZonedDateTime], path:String,
                  bandNames: ArrayList[String],
                  crs:CRS, dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String]) = {
    val maxExtent: Extent = rasters.map(_._2).reduce((a, b) => if (a.area > b.area) a else b)
    val equalRasters = rasters.map(raster =>
      if (raster.extent != maxExtent) raster.crop(maxExtent, CropOptions(clamp = false, force = true)) else raster
    )
    val aRaster = equalRasters.head
    val rasterExtent = aRaster.rasterExtent

    val intermediatePath =
    if (path.startsWith("s3:/")) {
      Files.createTempFile(null, null).toString
    }else{
      path
    }

    val netcdfFile: NetcdfFileWriter = setupNetCDF(intermediatePath, rasterExtent, dates, bandNames, crs, aRaster.cellType,dimensionNames, attributes, writeTimeDimension= dates!=null)
    try{

      for (bandIndex <- bandNames.asScala.indices) {
        for (i <- equalRasters.indices) {
          writeTile(bandNames.get(bandIndex),  if(dates!=null)  scala.Array(i , 0, 0) else scala.Array( 0, 0), equalRasters(i).tile.band(bandIndex), netcdfFile)
        }
      }
    }finally {
      netcdfFile.close()
    }

    if (path.startsWith("s3:/")) {
      uploadToS3(path, intermediatePath)
    }

  }

  private def uploadToS3(objectStoragePath: String, localPath: String) = {
    val correctS3Path = objectStoragePath.replaceFirst("s3:/(?!/)", "s3://")
    val s3Uri = new AmazonS3URI(correctS3Path)

    val objectRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build

    getCreoS3Client().putObject(objectRequest, RequestBody.fromFile(Paths.get(localPath)))
  }

  private[netcdf] def setupNetCDF(path: String, rasterExtent: RasterExtent, dates: Seq[ZonedDateTime],
                                  bandNames: util.ArrayList[String], crs: CRS, cellType: CellType,
                                  dimensionNames: java.util.Map[String,String],
                                  attributes: java.util.Map[String,String], zLevel:Int =6, writeTimeDimension:Boolean = true) = {

    logger.info(s"Writing netCDF to $path with bands $bandNames, $cellType, $crs, $rasterExtent")
    val theChunking = new OpenEOChunking(zLevel)
    val netcdfFile: NetcdfFileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic,path, theChunking)

    import java.util

    netcdfFile.addGlobalAttribute("Conventions", "CF-1.9")
    netcdfFile.addGlobalAttribute("institution", "openEO platform")
    if(attributes != null) {
      for(attr <- attributes.asScala) {
        netcdfFile.addGlobalAttribute(attr._1, attr._2)
      }
    }
    val timeDimName = if(dimensionNames!=null) dimensionNames.getOrDefault(TIME,TIME) else TIME


    val timeDimension = if(writeTimeDimension) netcdfFile.addUnlimitedDimension(timeDimName) else null
    val yDimension = netcdfFile.addDimension(Y, rasterExtent.rows)
    val xDimension = netcdfFile.addDimension(X, rasterExtent.cols)

    val timeDimensions = new util.ArrayList[Dimension]
    timeDimensions.add(timeDimension)
    if(writeTimeDimension) {
      addTimeVariable(netcdfFile, dates, timeDimName, timeDimensions)
    }


    val xDimensions = new util.ArrayList[Dimension]
    xDimensions.add(xDimension)

    val yDimensions = new util.ArrayList[Dimension]
    yDimensions.add(yDimension)

    val units = crs.proj4jCrs.getProjection.getUnits.name
    if(units == "degree") {
      addNetcdfVariable(netcdfFile, xDimensions, X, DataType.DOUBLE, "longitude", "longitude", "degrees_east", null)
      addNetcdfVariable(netcdfFile, yDimensions, Y, DataType.DOUBLE, "latitude", "latitude", "degrees_north", null)
    }else{

      addNetcdfVariable(netcdfFile, xDimensions, X, DataType.DOUBLE, "projection_x_coordinate", "x coordinate of projection", "m", null)
      addNetcdfVariable(netcdfFile, yDimensions, Y, DataType.DOUBLE, "projection_y_coordinate", "y coordinate of projection", "m", null)
    }




    netcdfFile.addVariable("crs", DataType.CHAR, "")
    netcdfFile.addVariableAttribute("crs", "crs_wkt", crs.toWKT().get)
    netcdfFile.addVariableAttribute("crs", "spatial_ref", crs.toWKT().get) //this one is especially for gdal...
    //netcdfFile.addVariableAttribute("crs","GeoTransform", "some geotransform") // this is what old style gdal puts in there
    //netcdfFile.addVariableAttribute("crs","grid_mapping_name","latitude_longitude")
    //netcdfFile.addVariableAttribute("crs","false_easting",crs.proj4jCrs.getProjection.getFalseEasting)
    //netcdfFile.addVariableAttribute("crs","false_northing",crs.proj4jCrs.getProjection.getFalseNorthing)
    //netcdfFile.addVariableAttribute("crs","earth_radius",crs.proj4jCrs.getProjection.getEquatorRadius)
    //netcdfFile.addVariableAttribute("crs","latitude_of_projection_origin",crs.proj4jCrs.getProjection.getProjectionLatitudeDegrees)
    //netcdfFile.addVariableAttribute("crs","longitude_of_projection_origin",crs.proj4jCrs.getProjection.getProjectionLongitudeDegrees)

    val bandDimension = new util.ArrayList[Dimension]
    if(writeTimeDimension) {
      bandDimension.add(timeDimension)
    }
    bandDimension.add(yDimension)
    bandDimension.add(xDimension)

    val (netcdfType:DataType,nodata:Option[Number]) = cellType match {
      case BitCellType => (DataType.BOOLEAN,None)
      case ByteCellType => (DataType.BYTE,None)
      case UByteCellType => (DataType.UBYTE,None)
      case ShortCellType => (DataType.SHORT,None)
      case UShortCellType => (DataType.USHORT,None)
      case IntCellType => (DataType.INT,None)
      case FloatCellType => (DataType.FLOAT,None)
      case DoubleCellType => (DataType.DOUBLE,None)
      case ByteConstantNoDataCellType => (DataType.BYTE,Some(byteNODATA))
      case UByteConstantNoDataCellType => (DataType.UBYTE,Some(ubyteNODATA))
      case ShortConstantNoDataCellType => (DataType.SHORT,Some(shortNODATA))
      case UShortConstantNoDataCellType => (DataType.USHORT,Some(ushortNODATA))
      case IntConstantNoDataCellType => (DataType.INT,Some(NODATA))
      case FloatConstantNoDataCellType => (DataType.FLOAT,Some(floatNODATA.toFloat))
      case DoubleConstantNoDataCellType => (DataType.DOUBLE,Some(doubleNODATA.toDouble))
      case ct: ByteUserDefinedNoDataCellType => (DataType.BYTE,Some(ct.noDataValue))
      case ct: UByteUserDefinedNoDataCellType => (DataType.UBYTE,Some(ct.widenedNoData.asInt))
      case ct: ShortUserDefinedNoDataCellType => (DataType.SHORT,Some(ct.noDataValue))
      case ct: UShortUserDefinedNoDataCellType => (DataType.USHORT,Some(ct.widenedNoData.asInt))
      case ct: IntUserDefinedNoDataCellType => (DataType.INT,Some(ct.widenedNoData.asInt))
      case ct: FloatUserDefinedNoDataCellType => (DataType.FLOAT,Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
    }



    for (bandName <- bandNames.asScala) {
      val varName = bandName.replace("/","_")
      addNetcdfVariable(netcdfFile, bandDimension, varName, netcdfType, null, varName, "", null, nodata.getOrElse(0), null)
      netcdfFile.addVariableAttribute(varName, "grid_mapping", "crs")
      if(rasterExtent.cols>256 && rasterExtent.rows>256){
        val chunking = new ArrayInt.D1(if(writeTimeDimension) 3 else 2,false)
        if(writeTimeDimension){
          chunking.set(0,1)
          chunking.set(1,256)
          chunking.set(2,256)
        }else{
          chunking.set(0,256)
          chunking.set(1,256)
        }
        netcdfFile.addVariableAttribute(varName, new Attribute("_ChunkSizes", chunking))
      }

    }

    //First define all variable and dimensions, then create the netcdf, after creation values can be written to variables
    netcdfFile.create()


    val xValues = for (x <- 0 until rasterExtent.cols) yield rasterExtent.extent.xmin + x * rasterExtent.cellwidth + rasterExtent.cellwidth / 2.0
    val yValues = for (y <- 0 until rasterExtent.rows) yield rasterExtent.extent.ymax - y * rasterExtent.cellheight - rasterExtent.cellheight / 2.0

    //Write values to variable

    if(dates!=null){
      val daysSince = dates.map(Duration.between(fixedTimeOffset, _).toDays.toInt)
      writeTime(timeDimName, netcdfFile, daysSince)
    }
    write1DValues(netcdfFile, xValues, X)
    write1DValues(netcdfFile, yValues, Y)
    netcdfFile
  }

  private def addTimeVariable(netcdfFile: NetcdfFileWriter, dates: Seq[ZonedDateTime], timeDimName: String, timeDimensions: util.ArrayList[Dimension]) = {
    addNetcdfVariable(netcdfFile, timeDimensions, timeDimName, DataType.INT, TIME, TIME, "days since " + cfDatePattern.format(fixedTimeOffset), "T")
  }

  import java.io.IOException
  import java.util

  private def addNetcdfVariable(netcdfFile: NetcdfFileWriter, dimensions: util.ArrayList[Dimension], variableName: String, dataType: DataType, standardName: String, longName: String, units: String, axis: String): Unit = {
    netcdfFile.addVariable(variableName, dataType, dimensions)
    netcdfFile.addVariableAttribute(variableName, "standard_name", standardName)
    netcdfFile.addVariableAttribute(variableName, "long_name", longName)
    netcdfFile.addVariableAttribute(variableName, "units", units)
    if(axis !=null) {
      netcdfFile.addVariableAttribute(variableName, "axis", axis)
    }
  }

  private def addNetcdfVariable(netcdfFile: NetcdfFileWriter, dimensions: util.ArrayList[Dimension], variableName: String, dataType: DataType, standardName: String, longName: String, units: String, axis: String, fillValue: Number, coordinates: String): Unit = {
    netcdfFile.addVariable(variableName, dataType, dimensions)
    if (standardName != null) netcdfFile.addVariableAttribute(variableName, "standard_name", standardName)
    if (longName != null) netcdfFile.addVariableAttribute(variableName, "long_name", longName)
    if (units != null) netcdfFile.addVariableAttribute(variableName, "units", units)
    if (axis != null) netcdfFile.addVariableAttribute(variableName, "axis", axis)
    if (fillValue != Integer.MIN_VALUE) netcdfFile.addVariableAttribute(variableName, "_FillValue", fillValue)
    if (coordinates != null) netcdfFile.addVariableAttribute(variableName, "coordinates", coordinates)
  }

  import org.opengis.coverage.grid.InvalidRangeException

  @throws[IOException]
  @throws[InvalidRangeException]
  private def writeTime(dimName:String, netcdfFile: NetcdfFileWriter, convertedTimeArray: Seq[Int]): Unit = {
    val timeArray = new ArrayInt.D1(convertedTimeArray.length,false)
    for (i <- convertedTimeArray.indices) {
      timeArray.set(i, convertedTimeArray(i))
    }
    netcdfFile.write(dimName, timeArray)
  }


  @throws[IOException]
  @throws[InvalidRangeException]
  private def write1DValues(netcdfFile: NetcdfFileWriter, yValues: IndexedSeq[Double], variableName: String): Unit = {
    val yArray = new ArrayDouble.D1(yValues.length)
    for (i <- yValues.indices) {
      yArray.set(i, yValues(i))
    }
    netcdfFile.write(variableName, yArray)
  }
}
