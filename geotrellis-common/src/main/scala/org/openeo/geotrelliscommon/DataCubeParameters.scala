package org.openeo.geotrelliscommon

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}

import java.util
import java.util.Collections

class DataCubeParameters {
  var tileSize: Int = 256
  var maskingStrategyParameters: util.Map[String, Object] = Collections.emptyMap()
  var layoutScheme: String = "ZoomedLayoutScheme"
  var partitionerTemporalResolution: String = "ByDay"
  var partitionerIndexReduction: Int = 8
  var maskingCube: Option[Object] = Option.empty
  var globalExtent:Option[ProjectedExtent] = Option.empty

  override def toString = s"DataCubeParameters($tileSize, $maskingStrategyParameters, $layoutScheme, $partitionerTemporalResolution, $partitionerIndexReduction, $maskingCube)"

  def setPartitionerIndexReduction(reduction:Int): Unit = partitionerIndexReduction = reduction
  def setPartitionerTemporalResolution(res:String): Unit = partitionerTemporalResolution = res
  def setLayoutScheme(scheme:String): Unit = layoutScheme = scheme
  def setTileSize(size:Int): Unit = tileSize = size

  def setMaskingCube(aMaskingCube: Object): Unit = {
    maskingCube = Some(aMaskingCube)
  }

  def setGlobalExtent(xmin:Double,ymin:Double,xmax:Double,ymax:Double,crs:String): Unit = {
    globalExtent = Some(ProjectedExtent(Extent(xmin,ymin,xmax,ymax),CRS.fromName(crs)))
  }

}
