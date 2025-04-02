package org.openeo.geotrellis.zarr

import geotrellis.layer.{Boundable, Bounds, KeyBounds, SpaceTimeKey, SpatialComponent, TemporalKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.vector.Extent

import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag


trait ZarrAttributes extends Serializable {
  def toMap: java.util.Map[String,Object]
}

class variableAttribute(name:String) extends ZarrAttributes {
  def toMap:java.util.Map[String,Object] ={
    val varMap = new java.util.HashMap[String,Object]()
    varMap.put("_ARRAY_DIMENSIONS",Array[String](name))
    if (name=="time"){
      varMap.put("units","milliseconds since " + TemporalKey(0).time.format(DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss")))
      varMap.put("calendar", "proleptic_gregorian")
    }
    varMap
  }
}

class dataAttribute[K: SpatialComponent: Boundable : ClassTag](metadata: TileLayerMetadata[K],nBands:Int,hasTempDim:Boolean) extends ZarrAttributes {
  private val crs: CRS = metadata.crs
  private val bbox: Extent = metadata.extent
  private val dimensions: Array[String] = Array("y","x")
  private val hasTemp: Boolean = hasTempDim

  def toMap:java.util.HashMap[String,Object] = {
    val attributes = new java.util.HashMap[String,Object]()
    addDimensions(attributes)
    addCrs(attributes)
    addExtent(attributes)
  }

  private def addCrs(attributes:java.util.HashMap[String,Object]):java.util.HashMap[String,Object] = {
    val crsMap = new java.util.HashMap[String,Object]()
    if (crs.toWKT().isDefined) crsMap.put("wkt",crs.toWKT().get)
    crsMap.put("code",crs.proj4jCrs.getName)
    crsMap.put("proj:bbox",Array(bbox.ymin,bbox.xmin,bbox.ymax,bbox.xmax))
    crsMap.put("proj:shape",Array(metadata.rows.toInt,metadata.cols.toInt))
    attributes.put("_CRS",crsMap)
    attributes
  }

  private def addDimensions(attributes: java.util.HashMap[String,Object]): java.util.HashMap[String,Object] = {
    val tempDim = if (hasTemp) {"time" +: dimensions} else dimensions
    val bandDim = if (nBands > 1) {
      attributes.put("COLOR_INTERPRETATION", Array.fill(nBands)("Undefined"))
      "Band" +: tempDim
    } else tempDim
    attributes.put("_ARRAY_DIMENSIONS", bandDim)
    attributes
  }

  private def addExtent(attributes: java.util.HashMap[String,Object]): java.util.HashMap[String,Object] = {
    val extent = new java.util.HashMap[String,Object]()
    if (hasTemp) {
      val tempExtent =  new java.util.HashMap[String,Object]()
      if (metadata.bounds.nonEmpty) {
        val key = metadata.bounds.get.asInstanceOf[KeyBounds[SpaceTimeKey]]
        key.minKey.temporalKey
        val minTemp = key.minKey.temporalKey.time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "T" + key.minKey.temporalKey.time.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "Z"
        val maxTemp = key.maxKey.temporalKey.time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "T" + key.maxKey.temporalKey.time.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "Z"
        val tempArray = Array(minTemp, maxTemp)
        tempExtent.put("interval", Array(tempArray))
        extent.put("temporal", tempExtent)
      }
    }
    val spatialExtent =  new java.util.HashMap[String,Object]()
    spatialExtent.put("bbox", Array(Array(bbox.ymin,bbox.xmin,bbox.ymax,bbox.xmax)))
    extent.put("spatial",spatialExtent)
    attributes.put("extent",extent)
    attributes
  }
}

