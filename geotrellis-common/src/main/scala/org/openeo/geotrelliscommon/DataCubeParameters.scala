package org.openeo.geotrelliscommon

import geotrellis.proj4.CRS
import geotrellis.raster.ResampleMethod
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.vector.{Extent, ProjectedExtent}

import java.util
import java.util.Collections

//noinspection ScalaUnusedSymbol
class DataCubeParameters extends Serializable {
  var tileSize: Int = 256
  var maskingStrategyParameters: util.Map[String, Object] = Collections.emptyMap()
  var layoutScheme: String = "ZoomedLayoutScheme"
  var partitionerTemporalResolution: String = "ByDay"
  var partitionerIndexReduction: Int = SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION
  var resampleMethod: ResampleMethod = NearestNeighbor
  var maskingCube: Option[Object] = Option.empty
  var globalExtent:Option[ProjectedExtent] = Option.empty
  var pixelBufferX:Double = 0.0
  var pixelBufferY:Double = 0.0
  var noResampleOnRead: Boolean = false
  var useNewFeatureExtentIntersection: Boolean = false
  var useNewFeatureExtentIntersection2: Boolean = false
  var timeDimensionFilter: Option[java.io.Serializable] = Option.empty
  var allowEmptyCube: Boolean = false
  var loadPerProduct: Boolean = false
  var rasterSource: Option[String] = Option.empty
  /**
   * A maximum size in megabytes that output partitions should have. Not all code paths support this yet.
   * If set, automatic tuning of other parameters such as index reduction should be applied.
   * If not set, we fall back to using fixed index reduction.
   */
  var maxPartitionSize: Option[Int] = None

  /**
   * Whether to filter out MultibandTiles that are empty (i.e. all bands are NODATA),
   * or to keep them as EmptyMultiBandTiles.
   */
  var retainNoDataTiles: Boolean = false

  /**
   * Configuration to override asset loading with synthetic data
   */
  var syntheticDataOverride: Option[SyntheticDataOverride] = None

  override def toString = s"DataCubeParameters($tileSize, $maskingStrategyParameters, $layoutScheme, $partitionerTemporalResolution, $partitionerIndexReduction, $maskingCube, $resampleMethod, $pixelBufferX, $pixelBufferY)"

  def setPartitionerIndexReduction(reduction:Int): Unit = partitionerIndexReduction = reduction
  def setPartitionerTemporalResolution(res:String): Unit = partitionerTemporalResolution = res
  def setLayoutScheme(scheme:String): Unit = layoutScheme = scheme
  def setTileSize(size:Int): Unit = tileSize = size

  def setMaxPartitionSize(size: Int): Unit = {
    if (size > 0) {
      maxPartitionSize = Some(size)
    } else {
      maxPartitionSize = None
    }
  }

  def setLoadPerProduct(loadPerProduct:Boolean): Unit = this.loadPerProduct = loadPerProduct

  def setMaskingCube(aMaskingCube: Object): Unit = {
    maskingCube = Some(aMaskingCube)
  }

  def setResampleMethod(aMethod: ResampleMethod): Unit  = {
    resampleMethod = aMethod
  }

  def setGlobalExtent(xmin:Double,ymin:Double,xmax:Double,ymax:Double,crs:String): Unit = {
    globalExtent = Some(ProjectedExtent(Extent(xmin,ymin,xmax,ymax),CRS.fromName(crs)))
  }

  def setPixelBuffer(x:Double, y:Double):Unit = {
    pixelBufferX = x
    pixelBufferY = y
  }

  def setNoResampleOnRead(noResample:Boolean):Unit = {
    noResampleOnRead = noResample
  }

  def setUseNewFeatureExtentIntersection(v: Boolean): Unit = {
    useNewFeatureExtentIntersection = v
  }

  def setUseNewFeatureExtentIntersection2(v: Boolean): Unit = {
    useNewFeatureExtentIntersection2 = v
  }

  def setTimeDimensionFilter(conditionProcessScriptBuilder:java.io.Serializable):Unit = {
    timeDimensionFilter = Some(conditionProcessScriptBuilder)
  }

  def setAllowEmptyCube(allowEmpty:Boolean):Unit = {
    allowEmptyCube = allowEmpty
  }

  def setRetainNoDataTiles(retain:Boolean):Unit = {
    retainNoDataTiles = retain
  }

  def setSyntheticDataOverride(syntheticData: SyntheticDataOverride): Unit = {
    syntheticDataOverride = Some(syntheticData)
  }
}

case class SyntheticDataOverride(cellType: String, udf: Option[String])

