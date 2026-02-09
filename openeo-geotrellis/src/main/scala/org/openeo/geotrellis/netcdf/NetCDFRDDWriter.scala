package org.openeo.geotrellis.netcdf

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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, TaskContext}
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.geotiff.preProcess
import org.openeo.geotrellis.stac.{Asset, Item}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons, TemporalResolution}
import org.openeo.geotrelliscommon.ByKeyPartitioner
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest
import ucar.ma2.{ArrayDouble, ArrayInt, DataType, InvalidRangeException}
import ucar.nc2.write.Nc4ChunkingDefault
import ucar.nc2.{Attribute, Dimension, NetcdfFileWriter, Variable}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util
import java.util.{ArrayList, Collections, UUID}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag


object NetCDFRDDWriter {

  val logger = LoggerFactory.getLogger(NetCDFRDDWriter.getClass)

  val fixedTimeOffset = ZonedDateTime.parse("1990-01-01T00:00:00Z")
  val LON = "lon"
  val LAT = "lat"
  val X = "x"
  val Y = "y"
  val TIME = "t"
  val secondsPerDay = 86400L

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

  def writeRasters(rdd:Object,path:String,options:NetCDFOptions): java.util.List[Item] = {

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
                       bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                       zLevel:Int,
                      ): java.util.List[Item] = {
    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, bandsMetadata, zLevel, addBandsStatistics=false)
  }

  def saveSingleNetCDFSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                               path: String,
                               options:NetCDFOptions,
                             ): java.util.List[Item] = {
    saveSingleNetCDFSpatial(rdd, path, options.bandNames.get,options.dimensionNames.orNull,options.attributes.orNull,options.bandsMetadata.orNull, options.zLevel, options.addBandStatistics)
  }

  def saveSingleNetCDFSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                              path: String,
                              bandNames: ArrayList[String],
                              dimensionNames: java.util.Map[String,String],
                              attributes: java.util.Map[String,String],
                              bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                              zLevel:Int,
                              addBandsStatistics: Boolean,
                             ): java.util.List[Item] = {
    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, bandsMetadata, zLevel, addBandsStatistics)
  }

  def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata:java.util.Map[String,java.util.Map[String,String]],
                  zLevel:Int,
                 ): java.util.List[Item] = {

    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, bandsMetadata, zLevel, addBandsStatistics = false)
  }

  def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata:java.util.Map[String,java.util.Map[String,String]],
                  zLevel:Int,
                  addBandsStatistics: Boolean,
                 ): java.util.List[Item] = {

    saveSingleNetCDFGeneric(rdd,path,bandNames, dimensionNames, attributes, bandsMetadata, zLevel, addBandsStatistics)
  }

    def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                         path: String,
                         options:NetCDFOptions,
                        ): java.util.List[Item] = {
      saveSingleNetCDF(rdd, path, options.bandNames.get,options.dimensionNames.orNull,options.attributes.orNull,options.bandsMetadata.orNull,options.zLevel, options.addBandStatistics)
    }

  def saveSingleNetCDFGeneric[K: SpatialComponent: Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], path:String, options:NetCDFOptions): java.util.List[Item] = {
    saveSingleNetCDFGeneric(rdd,path,options.bandNames.orNull,options.dimensionNames.orNull,options.attributes.orNull,options.bandsMetadata.orNull, options.zLevel, options.addBandStatistics, options.cropBounds)
  }

  def saveSingleNetCDFGeneric[K: SpatialComponent: Boundable : ClassTag](rdd: MultibandTileLayerRDD[K],
                       path: String,
                       bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String],
                       bandsMetadata:java.util.Map[String,java.util.Map[String,String]],
                       zLevel:Int,
                       addBandsStatistics: Boolean,
                       cropBounds:Option[Extent]= None,
                      ): java.util.List[Item] = {

    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd,cropBounds)
    val extent = preProcessResult._2
    val preProcessedRdd = preProcessResult._3

    val rasterExtent = RasterExtent(extent = extent, cellSize = preProcessedRdd.metadata.cellSize)

    val cachedRDD: RDD[(K, MultibandTile)] = cacheAndRepartition(preProcessedRdd)

    val temporalResolution = if (cachedRDD.keys.filter({
      case key: SpaceTimeKey =>
        // true if not exactly n days:
        Duration.between(fixedTimeOffset, key.time).getSeconds % secondsPerDay != 0
      case _ =>
        false
    }).isEmpty()) TemporalResolution.days else TemporalResolution.seconds

    val dates =
      cachedRDD.keys.flatMap {
        case key: SpaceTimeKey =>
          val duration = Duration.between(fixedTimeOffset, key.time)
          Some((temporalResolution match {
            case TemporalResolution.days => duration.toDays
            case TemporalResolution.seconds => duration.getSeconds
          }).toInt)
        case _ =>
          None
      }.distinct().collect().sorted.toList


    val forceTempFile = !rdd.context.getConf.get("spark.kubernetes.namespace", "nothing").equals("nothing")
    val intermediatePath =
      if (path.startsWith("s3:/") || forceTempFile) {
        Files.createTempFile(null, null).toString
      }else{
        path
      }
    val bandStatistics = collection.mutable.Map[String,(Double,Double,Option[Double],Int,Int)]()
    var netcdfFile: NetcdfFileWriter = null
    for(tuple <- cachedRDD.toLocalIterator){

      val cellType = tuple._2.cellType
      val timeDimIndex =
        if(dates.nonEmpty){
          val duration = Duration.between(fixedTimeOffset, tuple._1.asInstanceOf[SpaceTimeKey].time)
          val timeOffset = (temporalResolution match {
            case TemporalResolution.days => duration.toDays
            case TemporalResolution.seconds => duration.getSeconds
          }).toInt
          dates.indexOf(timeOffset)
        }else{
          -1
        }


      val multibandTile = tuple._2

      val actualBandNames: util.List[String] =
      if(bandNames.size() < multibandTile.bandCount){
        logger.error(s"Your cube metadata has these band names ${bandNames.toArray.mkString(",")} but we got data from your cube with more bands: ${multibandTile.bandCount}. You can fix band metadata using rename_labels.")
        val unknowns: util.List[String] = (bandNames.size() until multibandTile.bandCount toList).map(i => f"unkown_band_$i").asJava
        bandNames.addAll(unknowns)
        bandNames
      }else if(bandNames.size() < multibandTile.bandCount){
        logger.error(s"Your cube metadata has these band names ${bandNames.toArray.mkString(",")} but we got data from your cube with fewer bands: ${multibandTile.bandCount}. You can fix band metadata using rename_labels.")
        bandNames.subList(0,multibandTile.bandCount)
      }else{
        bandNames
      }


      if(netcdfFile == null){
        netcdfFile = setupNetCDF(intermediatePath, rasterExtent, null, actualBandNames, preProcessedRdd.metadata.crs, cellType, dimensionNames, temporalResolution, attributes, bandsMetadata, zLevel, writeTimeDimension = dates.nonEmpty)
      }


      for (bandIndex <- actualBandNames.asScala.indices) {

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
            val variable = actualBandNames.get(bandIndex)

            var tile = multibandTile.band(bandIndex)

            if(gridExtent.colMin + tile.cols > rasterExtent.cols || gridExtent.rowMin + tile.rows > rasterExtent.rows){
              tile = tile.crop(rasterExtent.cols-gridExtent.colMin,rasterExtent.rows-gridExtent.rowMin,raster.CropOptions(force=true))
              logger.debug(s"Cropping output tile to avoid going out of variable (${variable}) bounds ${gridExtent}.")
            }
            if (addBandsStatistics) bandsStatistics(tile, bandStatistics, variable)
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
      //pseudo feature flag
      if(netcdfFile!=null ) {
        netcdfFile.flush()
      }
    }
    val assetsMetadata = setupAssetMetadata(rdd.metadata, dates, bandNames, preProcessResult._1, extent, addBandsStatistics, bandStatistics)
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

    val finalPath =
      if (path.startsWith("s3:/")) {
        // TODO: Change spark-jobs-staging-disabled back to spark-jobs-staging
        if(rdd.context.getConf.get("spark.kubernetes.namespace","nothing").equals("spark-jobs-staging-disabled")) {
          uploadToS3LargeFile(path, intermediatePath)
        }else{
          uploadToS3(path, intermediatePath)
        }
      }else if(forceTempFile) {
        Files.move(Paths.get(intermediatePath),Paths.get(path),java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        path
      }
      else{
        path
      }

    val item = Item(id = UUID.randomUUID().toString, bbox = cropBounds.getOrElse(extent), datetime = null,
      assets = Collections.singletonMap("openEO", Asset(finalPath,metadata = assetsMetadata)))

    Collections.singletonList(item)
  }


  private def cacheAndRepartition[K](rdd: MultibandTileLayerRDD[K]) = {
    val cachedRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val count = cachedRDD.count()
    cachedRDD.name = s"netCDF RDD ${count} elements"
    logger.info(s"Writing NetCDF from rdd with : ${count} elements and ${rdd.getNumPartitions} partitions.")

    val elementsPartitionRatio =
      if(rdd.getNumPartitions == 0) {
        1000 // just a large number
      } else{
        count / rdd.getNumPartitions
      }

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
        case t: BitArrayTile => ucar.ma2.Array.factory(DataType.UBYTE, shape, t.convert(UByteUserDefinedNoDataCellType(255.byteValue())).asInstanceOf[UByteArrayTile].array)
        case t: ByteArrayTile => ucar.ma2.Array.factory(DataType.BYTE, shape, t.array)
        case t: UByteArrayTile => ucar.ma2.Array.factory(DataType.UBYTE, shape, t.array)
        case t: ShortArrayTile => ucar.ma2.Array.factory(DataType.SHORT, shape, t.array)
        case t: UShortArrayTile => ucar.ma2.Array.factory(DataType.USHORT, shape, t.array)
        case t: IntArrayTile => ucar.ma2.Array.factory(DataType.INT, shape, t.array)
        case t: FloatArrayTile => ucar.ma2.Array.factory(DataType.FLOAT, shape, t.array)
        case t: DoubleArrayTile => ucar.ma2.Array.factory(DataType.DOUBLE, shape, t.array)
      }

    var min:Int = Int.MaxValue
    var max:Int = Int.MinValue
    val iter = bandArray.getIndexIterator
    while (iter.hasNext){
      val nextInt = iter.getIntNext
      if (max < nextInt) max = nextInt
      if (nextInt < min) min = nextInt
    }
    logger.info(s"before write min and max is $min and $max")

    netcdfFile.write(variable, origin, bandArray)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                 ): java.util.List[Item] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames = null, attributes = null, bandsMetadata = null)

  // Overload to avoid: "multiple overloaded alternatives of method saveSamples define default arguments"
  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  filenamePrefix: Option[String],
                  ): java.util.List[Item] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames = null, attributes = null, bandsMetadata = null, filenamePrefix)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  filenamePrefix: Option[String],
                  addBandsStatistics:Boolean
                 ): java.util.List[Item] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames = null, attributes = null, bandsMetadata = null, addBandsStatistics = addBandsStatistics, filenamePrefix)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String, String],
                  attributes: java.util.Map[String, String],
                 ): java.util.List[Item] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames, attributes, bandsMetadata = null, None)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String, String],
                  attributes: java.util.Map[String, String],
                  bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                 ): java.util.List[Item] =
    saveSamples(rdd, path, polygons, sampleNames, bandNames, dimensionNames, attributes, bandsMetadata, None)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                  filenamePrefix: Option[String],
                 ): java.util.List[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toSeq.zip(reprojected.polygons)
    logger.info(s"Using metadata: ${rdd.metadata}.")
    logger.info(s"Using features: ${features}.")
    groupByFeatureAndWriteToNetCDF(rdd, features, path, bandNames, dimensionNames, attributes, bandsMetadata, addBandsStatistics = false, filenamePrefix)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  options:NetCDFOptions,
                  filenamePrefix: Option[String],
                 ): java.util.List[Item] = {
    if (options.bandNames.isEmpty) logger.error("Couldn't find bandNames in options. It cannot be empty")
    saveSamples(rdd, path, polygons, sampleNames, options.bandNames.get, options.dimensionNames.orNull, options.attributes.orNull, options.bandsMetadata.orNull, options.addBandStatistics, filenamePrefix)
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                  addBandsStatistics: Boolean,
                  filenamePrefix: Option[String],
                 ): java.util.List[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toSeq.zip(reprojected.polygons)
    logger.info(s"Using metadata: ${rdd.metadata}.")
    logger.info(s"Using features: ${features}.")
    groupByFeatureAndWriteToNetCDF(rdd, features, path, bandNames, dimensionNames, attributes, bandsMetadata, addBandsStatistics, filenamePrefix)
  }

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata:java.util.Map[String,java.util.Map[String,String]],
                  filenamePrefix: Option[String] = None,
                 ): java.util.List[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToNetCDFSpatial(rdd,  features,path,bandNames,dimensionNames,attributes, bandsMetadata, addBandsStatistics = false, filenamePrefix)
  }

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                         path: String,
                         polygons:ProjectedPolygons,
                         sampleNames: ArrayList[String],
                         options:NetCDFOptions,
                         filenamePrefix: Option[String],
                        ): java.util.List[Item] = {
    if (options.bandNames.isEmpty) logger.error("Couldn't find bandNames in options. It cannot be empty")
    saveSamplesSpatial(rdd,path,polygons,sampleNames,options.bandNames.get,options.dimensionNames.orNull,options.attributes.orNull,options.bandsMetadata.orNull, options.addBandStatistics, filenamePrefix)
  }

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                         path: String,
                         polygons:ProjectedPolygons,
                         sampleNames: ArrayList[String],
                         bandNames: ArrayList[String],
                         dimensionNames: java.util.Map[String,String],
                         attributes: java.util.Map[String,String],
                         bandsMetadata:java.util.Map[String,java.util.Map[String,String]],
                         addBandsStatistics: Boolean,
                         filenamePrefix: Option[String],
                        ): java.util.List[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToNetCDFSpatial(rdd,  features,path,bandNames,dimensionNames,attributes, bandsMetadata, addBandsStatistics, filenamePrefix)
  }

  private def groupByFeatureAndWriteToNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey], features: Seq[(String, Geometry)],
                                           path:String,bandNames: ArrayList[String],
                                           dimensionNames: java.util.Map[String,String],
                                           attributes: java.util.Map[String,String],
                                           bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                                           addBandsStatistics: Boolean,
                                           filenamePrefix: Option[String] = None,
                                           ): java.util.List[Item] = {
    val featuresBC: Broadcast[Seq[(String, Geometry)]] = SparkContext.getOrCreate().broadcast(features)

    val crs = rdd.metadata.crs
    val groupedBySample = stitchRDDBySample(rdd, featuresBC)
    //doing a count triggers full job execution, and there's already logging in previous block
    //logger.info(s"Writing ${groupedBySample.keys.count()} samples to disk.")
    groupedBySample.map { case (name, tiles: Iterable[(Long, Raster[MultibandTile], Extent)]) =>
        val outputAsPath: Path = getSamplePath(name, path, filenamePrefix)

        // Sort by date before writing.
        val sorted = tiles.toSeq.sortBy { case (instant, _, _) => instant }
        val dates = sorted.map { case (instant, _, _) => ZonedDateTime.ofInstant(instant, ZoneOffset.UTC) }
        logger.info(s"Writing $name with dates $dates.")
        val extent = sorted.head._2.extent
        val assetsMetadata = setupAssetMetadata(rdd.metadata,sorted.map(_._2), dates=dates, bandNames,addBandsStats = addBandsStatistics)
        val assetPath = try{
          writeToDisk(sorted.map(_._2), dates, outputAsPath.toString, bandNames, crs, dimensionNames, attributes, bandsMetadata)
        }catch {
          case t: IOException => {
            if(TaskContext.get().attemptNumber()<2){
              logger.warn(s"save_result netCDF: Failed to write sample: $name error: ${t.getMessage}", t)
              throw t
            }else{
              handleSampleWriteError(t, name, outputAsPath)
            }
          }
          case t: Throwable =>
            logger.error(s"save_result netCDF: Failed to write sample: $name error: ${t.getMessage}", t)
            throw t

        }
        val bands = assetsMetadata.get("bands")
        logger.info(s"assets contain bands: $bands")

        Item(id = UUID.randomUUID().toString, datetime = null , bbox = tiles.head._3,
          assets = Collections.singletonMap("openEO", Asset(path = assetPath,metadata = assetsMetadata)))
      }.collect()
      .toList.asJava
  }

  private def stitchRDDBySample(rdd: MultibandTileLayerRDD[SpaceTimeKey], featuresBC: Broadcast[Seq[(String, Geometry)]]) = {
    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
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
        ((sampleName, key.instant), ((key.spatialKey, sample),geometry.extent))
      }
    }.groupByKey()
    val stitchedByInstant = groupedByInstant.map(sample => {
        val tiles: Iterable[(SpatialKey, MultibandTile)] = sample._2.map(_._1)
        val extent = sample._2.map(_._2).head
        val raster = stitchAndCropTiles(tiles,ProjectedExtent(extent,crs),layout)

        (sample._1, raster, extent)
      }
    )
    val keyedBySample = stitchedByInstant.map { case ((sampleName, instant), raster, extent) => (sampleName, (instant, raster, extent)) }
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
                                           attributes: java.util.Map[String,String],
                                           bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                                           addBandsStatistics: Boolean,
                                           filenamePrefix: Option[String],
                                           ): java.util.List[Item] = {
    val featuresBC: Broadcast[List[(String, Extent)]] = SparkContext.getOrCreate().broadcast(features)
    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs

    groupRDDBySample(rdd, featuresBC)
      .map { case ((name, extent), tiles) =>
        val outputAsPath: Path = getSamplePath(name, path, filenamePrefix)
        val sample: Raster[MultibandTile] = stitchAndCropTiles(tiles, extent, layout)
        val assetMetadata = setupAssetMetadata(rdd.metadata, Seq(sample), dates=null, bandNames, addBandsStatistics)
        val assetPath = try {
          writeToDisk(Seq(sample), dates = null, outputAsPath.toString, bandNames, crs, dimensionNames, attributes, bandsMetadata)
        } catch {
          case e: IOException => handleSampleWriteError(e, name, outputAsPath)
        }

        val bands = assetMetadata.get("bands")
        logger.info(s"assets contain the bands: $bands")

        Item(id = UUID.randomUUID().toString, datetime = null, bbox = extent.extent,
          assets = Collections.singletonMap("openEO", Asset(assetPath, metadata = assetMetadata)))
      }.collect()
      .toList.asJava
  }

  private def handleSampleWriteError(t: IOException, sampleName: String, outputAsPath: Path): String = {
    logger.error(s"save_result netCDF: Failed to write sample: $sampleName error: ${t.getMessage}", t)
    val theFile = outputAsPath.toFile
    if (theFile.exists()) {
      val failedPath = outputAsPath.resolveSibling(outputAsPath.getFileName().toString + "_FAILED")
      Files.move(outputAsPath, failedPath)
      failedPath.toString
    } else {
      outputAsPath.toString
    }
  }

  private def getSamplePath(sampleName: String, outputDirectory: String, filenamePrefix: Option[String]) = {
    val filename = s"${filenamePrefix.getOrElse("openEO")}_${sampleName}.nc"
    val outputAsPath = Paths.get(outputDirectory).resolve(filename)
    outputAsPath
  }

  def writeToDisk(rasters: Seq[Raster[MultibandTile]], dates:Seq[ZonedDateTime], path:String,
                  bandNames: ArrayList[String],
                  crs:CRS, dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String],
                  bandsMetadata: java.util.Map[String,java.util.Map[String,String]]): String = {
    val areas = rasters.map(raster => raster.extent.area)
    logger.info(s"Writing ${rasters.size} rasters to disk. Areas: ${areas.mkString(",")}")
    val maxExtent:Extent = rasters.map(_._2).reduce((a, b) => a.union(b).extent)
    logger.info(s"Cropping rasters to max extent: $maxExtent")
    val equalRasters = rasters.map(raster =>
      if (raster.extent != maxExtent) raster.crop(maxExtent, CropOptions(clamp = false, force = true)) else raster
    )
    var aRaster = equalRasters.head
    if (aRaster.tile.cols == 0 || aRaster.tile.rows == 0) {
      logger.warn("At least one of the rasters in writeToDisk has 0 cols or rows. Trying to find a valid raster.")
      val aRasterOption = equalRasters.find(raster => raster.tile.cols > 0 && raster.tile.rows > 0)
      if (aRasterOption.isEmpty) {
        throw new IllegalArgumentException("No valid raster data found.")
      }
      aRaster = aRasterOption.get
    }
    val rasterExtent: RasterExtent = aRaster.rasterExtent

    val intermediatePath =
    if (path.startsWith("s3:/")) {
      Files.createTempFile(null, null).toString
    }else{
      path
    }

    val temporalResolution = if (dates == null) TemporalResolution.undefined else if (!dates.exists {
      time =>
        // true if not exactly n days:
        Duration.between(fixedTimeOffset, time).getSeconds % secondsPerDay != 0
    }) TemporalResolution.days else TemporalResolution.seconds

    val netcdfFile: NetcdfFileWriter = setupNetCDF(intermediatePath, rasterExtent, dates, bandNames, crs, aRaster.cellType, dimensionNames, temporalResolution, attributes, bandsMetadata, writeTimeDimension = dates != null)
    try{

      for (bandIndex <- bandNames.asScala.indices) {
        for (i <- equalRasters.indices) {
          writeTile(bandNames.get(bandIndex),  if(dates!=null)  scala.Array(i , 0, 0) else scala.Array( 0, 0), equalRasters(i).tile.band(bandIndex), netcdfFile)
        }
        netcdfFile.flush()
      }
    }finally {
      netcdfFile.close()
    }

    if (path.startsWith("s3:/")) {
      uploadToS3(path, intermediatePath)
    }else{
      path
    }

  }

  private def uploadToS3LargeFile(objectStoragePath: String, localPath: String) = {
    val correctS3Path = objectStoragePath.replaceFirst("s3:/(?!/)", "s3://")
    val s3Uri = new AmazonS3URI(correctS3Path)

    val putRequest = PutObjectRequest.builder().bucket(s3Uri.getBucket).key(s3Uri.getKey).build()
    val uploadFileRequest = UploadFileRequest.builder().putObjectRequest(putRequest).source(Paths.get(localPath)).build

    val transferManager = S3TransferManager.builder()
      .s3Client(CreoS3Utils.getAsyncClient)
      .build();
    val fileUpload = transferManager.uploadFile(uploadFileRequest)

    val uploadResult = fileUpload.completionFuture.join
    correctS3Path

  }

  private def uploadToS3(objectStoragePath: String, localPath: String):String = {
    val correctS3Path = objectStoragePath.replaceFirst("s3:/(?!/)", "s3://")
    val s3Uri = new AmazonS3URI(correctS3Path)

    val objectRequest = PutObjectRequest.builder
      .bucket(s3Uri.getBucket)
      .key(s3Uri.getKey)
      .build

    CreoS3Utils.getCreoS3Client().putObject(objectRequest, RequestBody.fromFile(Paths.get(localPath)))
    correctS3Path
  }

  private[netcdf] def setupNetCDF(path: String, rasterExtent: RasterExtent, dates: Seq[ZonedDateTime],
                                  bandNames: util.List[String], crs: CRS, cellType: CellType,
                                  dimensionNames: java.util.Map[String, String],
                                  temporalResolution: TemporalResolution.Value,
                                  attributes: java.util.Map[String, String],
                                  bandsMetadata: java.util.Map[String,java.util.Map[String,String]],
                                  zLevel: Int = 6, writeTimeDimension: Boolean = true) = {

    logger.info(s"Writing netCDF to $path with bands $bandNames, $cellType, $crs, $rasterExtent, $dimensionNames, attributes $attributes, bands metadata $bandsMetadata, zLevel $zLevel")
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
      addTimeVariable(netcdfFile, dates, timeDimName, timeDimensions, temporalResolution)
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
    val maybeWKT = crs.toWKT()
    if(maybeWKT.isDefined) {
      netcdfFile.addVariableAttribute("crs", "crs_wkt", maybeWKT.get)
      netcdfFile.addVariableAttribute("crs", "spatial_ref", maybeWKT.get) //this one is especially for gdal...
    }
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

    val (netcdfType:DataType,nodata:Option[Number]) = getNoDataValue(cellType)



    for (bandName <- bandNames.asScala) {
      val varName = bandName.replace("/","_")
      addNetcdfVariable(netcdfFile, bandDimension, varName, netcdfType, null, varName, "", null, nodata.getOrElse(0), null)
      if (bandsMetadata!=null) addNetcdfBandsMetadata(netcdfFile,varName,bandsMetadata.getOrDefault(bandName, java.util.Collections.emptyMap[String,String]()))
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
      val timeSince = temporalResolution match {
        case TemporalResolution.days => dates.map(Duration.between(fixedTimeOffset, _).toDays.toInt)
        case TemporalResolution.seconds => dates.map(Duration.between(fixedTimeOffset, _).getSeconds.toInt)
      }
      writeTime(timeDimName, netcdfFile, timeSince)
    }
    write1DValues(netcdfFile, xValues, X)
    write1DValues(netcdfFile, yValues, Y)
    netcdfFile
  }

  private def addTimeVariable(netcdfFile: NetcdfFileWriter, dates: Seq[ZonedDateTime], timeDimName: String, timeDimensions: util.ArrayList[Dimension], temporalResolution: TemporalResolution.Value): Unit = {
    val units = temporalResolution match {
      case TemporalResolution.days => "days since " + DateTimeFormatter.ofPattern("YYYY-MM-dd").format(fixedTimeOffset)
      case TemporalResolution.seconds => "seconds since " + DateTimeFormatter.ISO_ZONED_DATE_TIME.format(fixedTimeOffset)
    }
    addNetcdfVariable(netcdfFile, timeDimensions, timeDimName, DataType.INT, TIME, TIME, units, "T")
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

  private def addNetcdfBandsMetadata(netcdfFile: NetcdfFileWriter, variableName: String, bandsMetadata:java.util.Map[String,String]): Unit = {
    if (bandsMetadata.containsKey("SCALE")) netcdfFile.addVariableAttribute(variableName,"scale_factor",bandsMetadata.get("SCALE").toFloat)
    if (bandsMetadata.containsKey("OFFSET")) netcdfFile.addVariableAttribute(variableName,"add_offset",bandsMetadata.get("OFFSET").toFloat)
  }

  private def setupAssetMetadata[K: SpatialComponent : Boundable : ClassTag](metadata: TileLayerMetadata[K], dates: List[Int], bandNames: ArrayList[String], gridBounds: GridBounds[Int], bbox: Extent, addBandsStats: Boolean, bandStatistics:scala.collection.mutable.Map[String,(Double,Double,Option[Double],Int,Int)]): java.util.Map[String, Any] = {
    val assetMetadata = if (dates.nonEmpty) {
      new util.HashMap[String,Any](util.Map.of("time", new util.HashMap[String,Any](util.Map.of("type", "temporal", "extent",Array(dates.head, dates.last), "values", dates.toArray))))
    } else new java.util.HashMap[String,Any]()
    val bands = if (addBandsStats) {
      val maps = new util.ArrayList[util.Map[String,Any]]()
      bandStatistics.foreach {case (bandName,(min,max,mean,validCount,size)) => {
        val mapStatistics = mean.fold(new util.HashMap[String, Any](util.Map.of("valid_percent", 0.0))){ mean =>
          new util.HashMap[String, Any](util.Map.of("maximum", max, "minimum", min, "mean", mean, "valid_percent", validCount.toDouble/size*100))
        }
        logger.info(s"computed statistics for the band ${bandName}: $mapStatistics")
        val band = new util.HashMap[String,Any](util.Map.of("name", bandName, "statistics", mapStatistics))
        maps.add(band)

      }}
      maps
    } else {
      val maps = new java.util.ArrayList[java.util.HashMap[String,Any]]()
      bandNames.forEach(name => {
        val rasterBands = new java.util.HashMap[String,Any]()
        rasterBands.put("name", name)
        maps.add(rasterBands)
      })
      maps
    }
    assetMetadata.put("bands", bands)
    assetMetadata.put("proj:bbox", Array(bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax))
    metadata.crs.epsgCode.foreach(epsg => assetMetadata.put("proj:epsg", epsg))
    assetMetadata.put("proj:shape", Array(gridBounds.height, gridBounds.width))
    assetMetadata
  }

  private def setupAssetMetadata[K: SpatialComponent : Boundable : ClassTag](metadata: TileLayerMetadata[K], rasters: Seq[Raster[MultibandTile]], dates: Seq[ZonedDateTime], bandNames: ArrayList[String], addBandsStats: Boolean): util.Map[String, Any] = {
    val assetMetadata = new util.HashMap[String,Any]()
    if (dates != null) {
      assetMetadata.put("time", Map("type" -> "temporal", "extent" -> Array(dates.head, dates.last), "values" -> dates.toArray))
    } else new util.HashMap[String,Any]()
    val bands = if (addBandsStats) {
      bandsStatistics(rasters, bandNames)
    } else {
      val maps = new util.ArrayList[java.util.HashMap[String,Any]]()
      bandNames.forEach(name => {
        val rasterBands = new java.util.HashMap[String,Any]()
        rasterBands.put("name", name)
        maps.add(rasterBands)
      })
      maps
    }
    assetMetadata.put("bands", bands)
    assetMetadata.put("proj:bbox",Array(rasters.head.extent.xmin, rasters.head.extent.ymin, rasters.head.extent.xmax, rasters.head.extent.ymax))
    metadata.crs.epsgCode.foreach(epsg => assetMetadata.put("proj:epsg", epsg))
    assetMetadata.put("proj:shape", Array(rasters.head.rows, rasters.head.cols))
    assetMetadata
  }

  private def bandsStatistics(tile:Tile, bandStatistics:collection.mutable.Map[String,(Double,Double,Option[Double],Int,Int)], bandName:String): Unit = {
    val (tempMin,tempMax, tempMean, tempValidCount) = tile.cellType match {
      case _:FloatCells => statsDouble(tile)
      case _:DoubleCells => statsDouble(tile)
      case _:ShortCells => statsInt(tile)
      case _:UShortCells => statsInt(tile)
      case _:IntCells => statsInt(tile)
      case _:DoubleCells => statsInt(tile)
    }
    val minmax = tile.toArrayTile().findMinMax
    logger.info(s"the calulated min and max are $tempMin and $tempMax while the function found $minmax")
    val result = if (bandStatistics.contains(bandName)) {
      val (curMin,curMax,curMean, curValidCount,size) = bandStatistics(bandName)
      val newMean = if (tempValidCount+curValidCount > 0){
        Some((tempMean.getOrElse(0.0)*tempValidCount + curMean.getOrElse(0.0)*curValidCount)/(tempValidCount+curValidCount))
      } else None
      (Math.min(tempMin,curMin), Math.max(tempMax,curMax),newMean,tempValidCount+curValidCount,size+tile.size)
    } else (tempMin,tempMax,tempMean,tempValidCount,tile.size)
    bandStatistics.update(bandName,result)
  }

  private def bandsStatistics(rasters:Seq[Raster[MultibandTile]], bandNames: ArrayList[String]): java.util.ArrayList[java.util.HashMap[String,Any]] = {
    val stats = new java.util.ArrayList[java.util.HashMap[String,Any]]()
    for (bandId <- 0 until bandNames.size()){
      val bandStatistics = rasters.map(raster => {
        val tile = raster.tile.band(bandId)
        val minmax = tile.toArrayTile().findMinMax
        val (min, max, mean, validCount) = tile.cellType match {
          case _:FloatCells => statsDouble(tile)
          case _:DoubleCells => statsDouble(tile)
          case _:ShortCells => statsInt(tile)
          case _:UShortCells => statsInt(tile)
          case _:IntCells => statsInt(tile)
          case _:DoubleCells => statsInt(tile)
        }
        logger.info(s"calulated min and max are $min and $max while the function found $minmax")
        (min, max, mean, validCount, raster.tile.size)
      })
      val (min,max,mean,validCount,size)= bandStatistics.reduce{(x,y) => {
          val (accMin, accMax, accMean, accValidCount, accSize) = x
          val (min, max, mean, validCount, size) = y
          val newMean = if (accValidCount+validCount > 0){
            Some((accMean.getOrElse(0.0)*accValidCount + mean.getOrElse(0.0)*validCount)/(accValidCount+validCount))
          } else None
          (Math.min(accMin,min), Math.max(accMax,max),newMean,accValidCount+validCount, accSize + size)
        }
      }
      val rasterBands = new java.util.HashMap[String,Any]()
      val bandStats = mean.fold(new java.util.HashMap[String,Any](java.util.Map.of("valid_percent", 0.0)))(mean => {
        new java.util.HashMap[String, Any](java.util.Map.of("mean", mean, "maximum", max, "minimum", min, "valid_percent", validCount.toDouble/ size*100))
      })
      logger.info(s"computed statistics for band ${bandNames.get(bandId)}: $bandStats")
      rasterBands.put("statistics",bandStats)
      rasterBands.put("name",bandNames.get(bandId))
      stats.add(rasterBands)
    }
    stats
  }

  private def statsDouble(tile: Tile): (Double,Double,Option[Double],Int) = {
    var zmin = Double.NaN
    var zmax = Double.NaN
    var sum = 0.0
    var validCount = 0
    tile.foreachDouble { z =>
      if (isData(z)) {
        validCount +=1
        if(isNoData(zmin)) {
          zmin = z
          zmax = z
        } else {
          zmin = math.min(zmin, z)
          zmax = math.max(zmax, z)
          sum += z
        }
      }
    }
    val mean:Option[Double] = if(validCount == 0) {
      None
    }else{
      Some(sum/validCount)
    }
    (zmin,zmax,mean,validCount)
  }
  private def statsInt(tile:Tile): (Double,Double,Option[Double],Int) = {
    var zmin = Int.MaxValue
    var zmax = Int.MinValue
    var sum = 0
    var validCount = 0

    tile.foreach { z =>
      if (isData(z)) {
        validCount +=1
        zmin = math.min(zmin, z)
        zmax = math.max(zmax, z)
        sum += z
      }
    }

    val mean:Option[Double] = if(validCount == 0) {
      None
    }else{
      Some(sum.toDouble/validCount)
    }
    (zmin,zmax,mean,validCount)
  }


  private def getNoDataValue(cellType: CellType): (DataType,Option[Number]) = {
    cellType match {
      case BitCellType => (DataType.UBYTE,None)
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
      case FloatConstantNoDataCellType => (DataType.FLOAT,Some(floatNODATA))
      case DoubleConstantNoDataCellType => (DataType.DOUBLE,Some(doubleNODATA))
      case ct: ByteUserDefinedNoDataCellType => (DataType.BYTE,Some(ct.noDataValue))
      case ct: UByteUserDefinedNoDataCellType => (DataType.UBYTE,Some(ct.widenedNoData.asInt))
      case ct: ShortUserDefinedNoDataCellType => (DataType.SHORT,Some(ct.noDataValue))
      case ct: UShortUserDefinedNoDataCellType => (DataType.USHORT,Some(ct.widenedNoData.asInt.toShort))
      case ct: IntUserDefinedNoDataCellType => (DataType.INT,Some(ct.widenedNoData.asInt))
      case ct: FloatUserDefinedNoDataCellType => (DataType.FLOAT,Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
    }
  }

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
