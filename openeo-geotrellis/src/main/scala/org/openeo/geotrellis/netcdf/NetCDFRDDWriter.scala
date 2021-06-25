package org.openeo.geotrellis.netcdf

//import ucar.ma2.Array
import java.io.IOException
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZonedDateTime}
import java.util
import java.util.{ArrayList, Collections}

import geotrellis.layer.TileLayerMetadata.toLayoutDefinition
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.ProjectedPolygons
import org.slf4j.LoggerFactory
import ucar.ma2.{ArrayDouble, ArrayInt, DataType}
import ucar.nc2.write.Nc4ChunkingDefault
import ucar.nc2.{Attribute, Dimension, NetcdfFileWriter, Variable}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet


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

  def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String],
                       zLevel:Int
                 ): java.util.List[String] = {

    var dates = new ListSet[ZonedDateTime]()
    val extent = rdd.metadata.apply(rdd.metadata.tileBounds)

    val rasterExtent = RasterExtent(extent = extent, cellSize = rdd.metadata.cellSize)

    var netcdfFile: NetcdfFileWriter = null
    for(tuple <- rdd.toLocalIterator){

      val cellType = tuple._2.cellType
      dates = dates + tuple._1.time
      if(netcdfFile == null){
        netcdfFile = setupNetCDF(path, rasterExtent, null, bandNames, rdd.metadata.crs, cellType,dimensionNames,attributes,zLevel)
      }
      val multibandTile = tuple._2
      val timeDimIndex = dates.toIndexedSeq.indexOf(tuple._1.time)
      for (bandIndex <- bandNames.asScala.indices) {

        val gridExtent = rasterExtent.gridBoundsFor(tuple._1.spatialKey.extent(rdd.metadata))
        val origin: Array[Int] = scala.Array(timeDimIndex.toInt, gridExtent.rowMin.toInt, gridExtent.colMin.toInt)
        val variable = bandNames.get(bandIndex)

        val tile = multibandTile.band(bandIndex)
        writeTile(variable, origin, tile, netcdfFile)
      }
    }

    val daysSince = dates.map(Duration.between(fixedTimeOffset, _).toDays.toInt).toSeq
    val timeDimName = if(dimensionNames!=null) dimensionNames.getOrDefault(TIME,TIME) else TIME
    writeTime(timeDimName, netcdfFile, daysSince)

    netcdfFile.close()
    return Collections.singletonList(path)
  }


  private def writeTile(variable: String, origin: Array[Int], tile: Tile, netcdfFile: NetcdfFileWriter) = {
    val cols = tile.cols
    val rows = tile.rows

    val geotrellisArrayTile = tile.toArrayTile()

    val shape = scala.Array[Int](1, rows, cols)
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
                  ): java.util.List[String] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToTiff(rdd,  features,path,bandNames,null,null)

  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String],
                  dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String]
                 ): java.util.List[String] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToTiff(rdd,  features,path,bandNames,dimensionNames,attributes)

  }

  private def groupByFeatureAndWriteToTiff(rdd: MultibandTileLayerRDD[SpaceTimeKey], features: List[(String, Extent)],
                                           path:String,bandNames: ArrayList[String],
                                           dimensionNames: java.util.Map[String,String],
                                           attributes: java.util.Map[String,String]) = {
    val featuresBC: Broadcast[List[(String, Extent)]] = SparkContext.getOrCreate().broadcast(features)


    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
    rdd.flatMap {
      case (key, tile) => featuresBC.value.filter { case (_, extent) =>
        val tileBounds = layout.mapTransform(extent)

        if (KeyBounds(tileBounds).includes(key.spatialKey)) true else false
      }.map { case (name, extent) =>
        ((name, ProjectedExtent(extent, crs)), (key, tile))
      }
    }.groupByKey()
      .map { case ((name, extent), tiles) =>

        val filename = s"openEO_${name}.nc"
        val outputAsPath = Paths.get(path).resolve(filename)
        val filePath = outputAsPath.toString

        val grouped = tiles.groupBy(_._1.time)
        val allRasters = for(key_tile <- grouped) yield {
          val raster: Raster[MultibandTile] = ContextSeq(key_tile._2.map{ tuple => (tuple._1.spatialKey,tuple._2)}, layout).stitch()
          val re: RasterExtent = raster.rasterExtent
          val alignedExtent = re.createAlignedGridExtent(extent.extent).extent
          (key_tile._1, raster.crop(alignedExtent))
        }

        val sorted = allRasters.toSeq.sortBy(_._1.toEpochSecond)
        try{
          writeToDisk(sorted.map(_._2),sorted.map(_._1),filePath,bandNames,crs,dimensionNames,attributes)
          filePath
        }catch {
          case t: IOException => {
            logger.error("Failed to write sample: " + name, t)
            val theFile = outputAsPath.toFile
            if (theFile.exists()) {
              val failedPath = outputAsPath.resolveSibling(outputAsPath.getFileName().toString + "_FAILED")
              Files.move(outputAsPath, failedPath)
              failedPath.toString
            }else{
              filePath
            }
          }
          case t: Throwable =>  throw t
        }


      }.collect()
      .toList.asJava
  }

  def writeToDisk(rasters: Seq[Raster[MultibandTile]],dates:Seq[ZonedDateTime], path:String,
                  bandNames: ArrayList[String],
                  crs:CRS,dimensionNames: java.util.Map[String,String],
                  attributes: java.util.Map[String,String]) = {
    val aRaster = rasters.head
    val rasterExtent = aRaster.rasterExtent

    val netcdfFile: NetcdfFileWriter = setupNetCDF(path, rasterExtent, dates, bandNames, crs, aRaster.cellType,dimensionNames, attributes)
    try{

      for (bandIndex <- bandNames.asScala.indices) {
        for (i <- rasters.indices) {
          writeTile(bandNames.get(bandIndex),  scala.Array(i, 0, 0), rasters(i).tile.band(bandIndex), netcdfFile)
        }
      }
    }finally {
      netcdfFile.close()
    }

  }

  private[netcdf] def setupNetCDF(path: String, rasterExtent: RasterExtent, dates: Seq[ZonedDateTime],
                          bandNames: util.ArrayList[String], crs: CRS, cellType: CellType,
                          dimensionNames: java.util.Map[String,String],
                          attributes: java.util.Map[String,String],zLevel:Int =6) = {

    val theChunking = new OpenEOChunking(zLevel)
    val netcdfFile: NetcdfFileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic,path, theChunking)

    import java.util

    netcdfFile.addGlobalAttribute("Conventions", "CF-1.8")
    netcdfFile.addGlobalAttribute("institution", "openEO platform")
    if(attributes != null) {
      for(attr <- attributes.asScala) {
        netcdfFile.addGlobalAttribute(attr._1, attr._2)
      }
    }
    val timeDimName = if(dimensionNames!=null) dimensionNames.getOrDefault(TIME,TIME) else TIME

    val timeDimension = netcdfFile.addUnlimitedDimension(timeDimName)
    val yDimension = netcdfFile.addDimension(Y, rasterExtent.rows)
    val xDimension = netcdfFile.addDimension(X, rasterExtent.cols)

    val timeDimensions = new util.ArrayList[Dimension]
    timeDimensions.add(timeDimension)

    addTimeVariable(netcdfFile, dates, timeDimName, timeDimensions)


    val xDimensions = new util.ArrayList[Dimension]
    xDimensions.add(xDimension)
    addNetcdfVariable(netcdfFile, xDimensions, X, DataType.DOUBLE, "projection_x_coordinate", "x coordinate", "degrees_east", "X")


    val yDimensions = new util.ArrayList[Dimension]
    yDimensions.add(yDimension)
    addNetcdfVariable(netcdfFile, yDimensions, Y, DataType.DOUBLE, "projection_y_coordinate", "y coordinate", "degrees_north", "Y")


    netcdfFile.addVariable("crs", DataType.CHAR, "")
    netcdfFile.addVariableAttribute("crs", "crs_wkt", crs.toWKT().get)
    netcdfFile.addVariableAttribute("crs", "spatial_ref", crs.toWKT().get) //this one is especially for gdal...
    //netcdfFile.addVariableAttribute("crs","grid_mapping_name","transverse_mercator")
    //netcdfFile.addVariableAttribute("crs","false_easting",crs.proj4jCrs.getProjection.getFalseEasting)
    //netcdfFile.addVariableAttribute("crs","false_northing",crs.proj4jCrs.getProjection.getFalseNorthing)
    //netcdfFile.addVariableAttribute("crs","earth_radius",crs.proj4jCrs.getProjection.getEquatorRadius)
    //netcdfFile.addVariableAttribute("crs","latitude_of_projection_origin",crs.proj4jCrs.getProjection.getProjectionLatitudeDegrees)
    //netcdfFile.addVariableAttribute("crs","longitude_of_projection_origin",crs.proj4jCrs.getProjection.getProjectionLongitudeDegrees)

    val bandDimension = new util.ArrayList[Dimension]
    bandDimension.add(timeDimension)
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
      case ct: FloatUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
    }



    for (bandName <- bandNames.asScala) {
      addNetcdfVariable(netcdfFile, bandDimension, bandName, netcdfType, null, bandName, "", null, nodata.getOrElse(0), "y x")
      netcdfFile.addVariableAttribute(bandName, "grid_mapping", "crs")
      if(rasterExtent.cols>256 && rasterExtent.rows>256){
        val chunking = new ArrayInt.D1(3,false)
        chunking.set(0,1)
        chunking.set(1,256)
        chunking.set(2,256)
        netcdfFile.addVariableAttribute(bandName, new Attribute("_ChunkSizes", chunking))
      }

    }

    //First define all variable and dimensions, then create the netcdf, after creation values can be written to variables
    netcdfFile.create()


    val xValues = for (x <- 0 until rasterExtent.cols) yield rasterExtent.extent.xmin + x * rasterExtent.cellwidth + rasterExtent.cellwidth / 2.0
    val yValues = for (y <- 0 until rasterExtent.rows) yield rasterExtent.extent.ymax - y * rasterExtent.cellheight - rasterExtent.cellheight / 2.0

    write1DValues(netcdfFile, xValues, X)

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
    netcdfFile.addVariableAttribute(variableName, "axis", axis)
  }

  import java.util

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
