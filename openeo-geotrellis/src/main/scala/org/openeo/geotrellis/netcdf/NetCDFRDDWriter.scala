package org.openeo.geotrellis.netcdf

//import ucar.ma2.Array
import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{Duration, ZonedDateTime}
import java.util.ArrayList

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.ProjectedPolygons
import ucar.ma2.{ArrayDouble, ArrayFloat, DataType}
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

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons:ProjectedPolygons,
                  sampleNames: ArrayList[String],
                  bandNames: ArrayList[String]
                  ): java.util.List[String] = {
    val features = sampleNames.asScala.toList.zip(polygons.polygons.map(_.extent))
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
        val allRasters: Map[ZonedDateTime, Raster[MultibandTile]] = for(key_tile <- grouped) yield {
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
    val netcdfFile: NetcdfFileWriter = NetcdfFileWriter.createNew(path, false)

    //danger: dates map to rasters, so sorting can break that order
    val sortedDates = dates.sortBy(_.toEpochSecond)
    val firstDate = sortedDates.head
    val daysSince = dates.map(Duration.between(firstDate,_).toDays)

    val aRaster = rasters.head
    import java.util

    netcdfFile.addGlobalAttribute("Conventions","CF-1.8" )
    val timeDimension = netcdfFile.addDimension(TIME, rasters.length)
    val yDimension = netcdfFile.addDimension(Y, aRaster.rows)
    val xDimension = netcdfFile.addDimension(X, aRaster.cols)

    val timeDimensions = new util.ArrayList[Dimension]
    timeDimensions.add(timeDimension)

    val timeUnits = "days since " + cfDatePattern.format(firstDate)
    addNetcdfVariable(netcdfFile, timeDimensions, TIME, DataType.DOUBLE, TIME, TIME, timeUnits , "T")

    val xDimensions = new util.ArrayList[Dimension]
    xDimensions.add(xDimension)
    addNetcdfVariable(netcdfFile, xDimensions, X, DataType.DOUBLE, "projection_x_coordinate", "x coordinate", "degrees_east", "X")


    val yDimensions = new util.ArrayList[Dimension]
    yDimensions.add(yDimension)
    addNetcdfVariable(netcdfFile, yDimensions, Y, DataType.DOUBLE, "projection_y_coordinate", "y coordinate", "degrees_north", "Y")


    netcdfFile.addVariable("crs",DataType.CHAR,"")
    netcdfFile.addVariableAttribute("crs","crs_wkt",crs.toWKT().get)
    netcdfFile.addVariableAttribute("crs","spatial_ref",crs.toWKT().get)//this one is especially for gdal...
    //netcdfFile.addVariableAttribute("crs","grid_mapping_name","transverse_mercator")
    //netcdfFile.addVariableAttribute("crs","false_easting",crs.proj4jCrs.getProjection.getFalseEasting)
    //netcdfFile.addVariableAttribute("crs","false_northing",crs.proj4jCrs.getProjection.getFalseNorthing)
    //netcdfFile.addVariableAttribute("crs","earth_radius",crs.proj4jCrs.getProjection.getEquatorRadius)

    val bandDimension = new util.ArrayList[Dimension]
    bandDimension.add(timeDimension)
    bandDimension.add(yDimension)
    bandDimension.add(xDimension)

    for (bandName <- bandNames.asScala) {
      addNetcdfVariable(netcdfFile, bandDimension, bandName, DataType.FLOAT, null, bandName, "", null, -999, "y x")
      netcdfFile.addVariableAttribute(bandName,"grid_mapping","crs")
    }

    //First define all variable and dimensions, then create the netcdf, after creation values can be written to variables
    netcdfFile.create()

    val xValues =  for( x <- 0 until aRaster.rasterExtent.cols) yield aRaster.rasterExtent.extent.xmin + x * aRaster.rasterExtent.cellwidth + aRaster.rasterExtent.cellwidth/2.0
    val yValues =  for( y <- 0 until aRaster.rasterExtent.rows) yield aRaster.rasterExtent.extent.ymax - y * aRaster.rasterExtent.cellheight - aRaster.rasterExtent.cellheight/2.0

    write1DValues(netcdfFile, xValues, X)

    //Write values to variable

    writeTime(netcdfFile, daysSince)
    write1DValues(netcdfFile, xValues, X)
    write1DValues(netcdfFile, yValues, Y)

    for (bandIndex <- bandNames.asScala.indices) {
      writeBand(bandNames.get(bandIndex),bandIndex, netcdfFile, rasters)
    }

  }

  import java.io.IOException
  import java.util

  import org.opengis.coverage.grid.InvalidRangeException

  @throws[IOException]
  @throws[InvalidRangeException]
  private def writeBand(bandName:String,bandIndex:Int, netcdfFile: NetcdfFileWriter, floatValues: Seq[Raster[MultibandTile]]): Unit = {
    val cols = floatValues.head.cols
    val rows = floatValues.head.rows
    val airPressureArray = new ArrayFloat.D3(floatValues.length, rows, cols)
    for (i <- floatValues.indices) {
      for (j <- 0 until cols) {
        for (k <- 0 until rows) {
          airPressureArray.set(i, k, j, floatValues(i).tile.band(bandIndex).getDouble(j,k).toFloat)
        }
      }
    }
    netcdfFile.write(bandName, airPressureArray)
  }

  private def addNetcdfVariable(netcdfFile: NetcdfFileWriter, dimensions: util.ArrayList[Dimension], variableName: String, dataType: DataType, standardName: String, longName: String, units: String, axis: String): Unit = {
    netcdfFile.addVariable(variableName, dataType, dimensions)
    netcdfFile.addVariableAttribute(variableName, "standard_name", standardName)
    netcdfFile.addVariableAttribute(variableName, "long_name", longName)
    netcdfFile.addVariableAttribute(variableName, "units", units)
    netcdfFile.addVariableAttribute(variableName, "axis", axis)
  }

  import java.util

  private def addNetcdfVariable(netcdfFile: NetcdfFileWriter, dimensions: util.ArrayList[Dimension], variableName: String, dataType: DataType, standardName: String, longName: String, units: String, axis: String, fillValue: Int, coordinates: String): Unit = {
    netcdfFile.addVariable(variableName, dataType, dimensions)
    if (standardName != null) netcdfFile.addVariableAttribute(variableName, "standard_name", standardName)
    if (longName != null) netcdfFile.addVariableAttribute(variableName, "long_name", longName)
    if (units != null) netcdfFile.addVariableAttribute(variableName, "units", units)
    if (axis != null) netcdfFile.addVariableAttribute(variableName, "axis", axis)
    if (fillValue != Integer.MIN_VALUE) netcdfFile.addVariableAttribute(variableName, "_FillValue", fillValue)
    if (coordinates != null) netcdfFile.addVariableAttribute(variableName, "coordinates", coordinates)
  }

  import java.io.IOException

  import org.opengis.coverage.grid.InvalidRangeException

  @throws[IOException]
  @throws[InvalidRangeException]
  private def writeTime(netcdfFile: NetcdfFileWriter, convertedTimeArray: Seq[Long]): Unit = {
    val timeArray = new ArrayDouble.D1(convertedTimeArray.length)
    for (i <- convertedTimeArray.indices) {
      timeArray.set(i, convertedTimeArray(i))
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
