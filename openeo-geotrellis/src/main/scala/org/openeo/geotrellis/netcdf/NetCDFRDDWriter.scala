package org.openeo.geotrellis.netcdf

//import ucar.ma2.Array
import java.nio.file.Paths
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
import org.apache.spark.storage.StorageLevel
import org.openeo.geotrellis.ProjectedPolygons
import ucar.ma2.{ArrayDouble, ArrayInt, DataType}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingStrategy}
import ucar.nc2.{Dimension, NetcdfFileWriter}

import scala.collection.JavaConverters._


object NetCDFRDDWriter {


  val LON = "lon"
  val LAT = "lat"
  val X = "x"
  val Y = "y"
  val TIME = "t"
  val cfDatePattern = DateTimeFormatter.ofPattern("YYYY-MM-dd")

  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }

  def saveSingleNetCDF(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  bandNames: ArrayList[String],
                       dimensionNames: java.util.Map[String,String],
                       attributes: java.util.Map[String,String]
                 ): java.util.List[String] = {

    val cached = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val dates = cached.keys.map(_.time).distinct().collect()
    val extent = rdd.metadata.apply(rdd.metadata.tileBounds)



    val rasterExtent = RasterExtent(extent = extent, cellSize = rdd.metadata.cellSize)
    val sortedDates = dates.sortBy(_.toEpochSecond)


    var netcdfFile: NetcdfFileWriter = null
    for(tuple <- cached.toLocalIterator){

      val cellType = tuple._2.cellType
      if(netcdfFile == null){
        netcdfFile = setupNetCDF(path, rasterExtent, dates, bandNames, rdd.metadata.crs, cellType)
      }
      val multibandTile = tuple._2
      val daysSince = sortedDates.indexOf(tuple._1.time)
      for (bandIndex <- bandNames.asScala.indices) {

        val gridExtent = rasterExtent.gridBoundsFor(tuple._1.spatialKey.extent(rdd.metadata))
        val origin: Array[Int] = scala.Array(daysSince.toInt, gridExtent.rowMin.toInt, gridExtent.colMin.toInt)
        val variable = bandNames.get(bandIndex)

        val tile = multibandTile.band(bandIndex)
        writeTile(variable, origin, tile, netcdfFile)
      }
    }

    netcdfFile.close()
    cached.unpersist(blocking = false)
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
    netcdfFile.flush()
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String]
                  ): java.util.List[String] = {
    val reprojected = ProjectedPolygons.reproject(polygons,rdd.metadata.crs)
    val features = sampleNames.asScala.toList.zip(reprojected.polygons.map(_.extent))
    groupByFeatureAndWriteToTiff(rdd,  features,path,bandNames)

  }

  private def groupByFeatureAndWriteToTiff(rdd: MultibandTileLayerRDD[SpaceTimeKey], features: List[(String, Extent)],path:String,bandNames: ArrayList[String]) = {
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
        val filePath = Paths.get(path).resolve(filename).toString

        val grouped = tiles.groupBy(_._1.time)
        val allRasters = for(key_tile <- grouped) yield {
          val raster: Raster[MultibandTile] = ContextSeq(key_tile._2.map{ tuple => (tuple._1.spatialKey,tuple._2)}, layout).stitch()
          val re: RasterExtent = raster.rasterExtent
          val alignedExtent = re.createAlignedGridExtent(extent.extent).extent
          (key_tile._1, raster.crop(alignedExtent))
        }

        val sorted = allRasters.toSeq.sortBy(_._1.toEpochSecond)
        writeToDisk(sorted.map(_._2),sorted.map(_._1),filePath,bandNames,crs)

        filePath
      }.collect()
      .toList.asJava
  }

  def writeToDisk(rasters: Seq[Raster[MultibandTile]],dates:Seq[ZonedDateTime], path:String,bandNames: ArrayList[String],crs:CRS) = {
    val aRaster = rasters.head
    val rasterExtent = aRaster.rasterExtent

    val netcdfFile: NetcdfFileWriter = setupNetCDF(path, rasterExtent, dates, bandNames, crs, aRaster.cellType)

    for (bandIndex <- bandNames.asScala.indices) {
      for (i <- rasters.indices) {
        writeTile(bandNames.get(bandIndex),  scala.Array(i, 0, 0), rasters(i).tile.band(bandIndex), netcdfFile)
      }
    }
    netcdfFile.close()

  }

  private def setupNetCDF(path: String, rasterExtent: RasterExtent, dates: Seq[ZonedDateTime], bandNames: util.ArrayList[String], crs: CRS, cellType: CellType) = {

    val netcdfFile: NetcdfFileWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4,path, Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard, 9, true))


    //danger: dates map to rasters, so sorting can break that order

    val sortedDates = dates.sortBy(_.toEpochSecond)
    val firstDate = sortedDates.head
    val daysSince = dates.map(Duration.between(firstDate, _).toDays)


    import java.util

    netcdfFile.addGlobalAttribute("Conventions", "CF-1.8")
    netcdfFile.addGlobalAttribute("institution", "openEO platform")

    val timeDimension = netcdfFile.addDimension(TIME, dates.length)
    val yDimension = netcdfFile.addDimension(Y, rasterExtent.rows)
    val xDimension = netcdfFile.addDimension(X, rasterExtent.cols)

    val timeDimensions = new util.ArrayList[Dimension]
    timeDimensions.add(timeDimension)

    val timeUnits = "days since " + cfDatePattern.format(firstDate)
    addNetcdfVariable(netcdfFile, timeDimensions, TIME, DataType.INT, TIME, TIME, timeUnits, "T")

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
      case FloatConstantNoDataCellType => (DataType.FLOAT,floatNODATA)
      case DoubleConstantNoDataCellType => (DataType.DOUBLE,doubleNODATA)
      case ct: ByteUserDefinedNoDataCellType => (DataType.BYTE,Some(ct.noDataValue))
      case ct: UByteUserDefinedNoDataCellType => (DataType.UBYTE,Some(ct.widenedNoData.asInt))
      case ct: ShortUserDefinedNoDataCellType => (DataType.SHORT,Some(ct.noDataValue))
      case ct: UShortUserDefinedNoDataCellType => (DataType.USHORT,Some(ct.widenedNoData.asInt))
      case ct: IntUserDefinedNoDataCellType => (DataType.INT,Some(ct.widenedNoData.asInt))
      case ct: FloatUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => (DataType.DOUBLE,Some(ct.noDataValue))
    }



    for (bandName <- bandNames.asScala) {
      addNetcdfVariable(netcdfFile, bandDimension, bandName, netcdfType, null, bandName, "", null, nodata.get, "y x")
      netcdfFile.addVariableAttribute(bandName, "grid_mapping", "crs")
    }

    //First define all variable and dimensions, then create the netcdf, after creation values can be written to variables
    netcdfFile.create()


    val xValues = for (x <- 0 until rasterExtent.cols) yield rasterExtent.extent.xmin + x * rasterExtent.cellwidth + rasterExtent.cellwidth / 2.0
    val yValues = for (y <- 0 until rasterExtent.rows) yield rasterExtent.extent.ymax - y * rasterExtent.cellheight - rasterExtent.cellheight / 2.0

    write1DValues(netcdfFile, xValues, X)

    //Write values to variable

    writeTime(netcdfFile, daysSince)
    write1DValues(netcdfFile, xValues, X)
    write1DValues(netcdfFile, yValues, Y)
    netcdfFile
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
  private def writeTime(netcdfFile: NetcdfFileWriter, convertedTimeArray: Seq[Long]): Unit = {
    val timeArray = new ArrayInt.D1(convertedTimeArray.length,false)
    for (i <- convertedTimeArray.indices) {
      timeArray.set(i, convertedTimeArray(i).toInt)
    }
    netcdfFile.write(TIME, timeArray)
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
