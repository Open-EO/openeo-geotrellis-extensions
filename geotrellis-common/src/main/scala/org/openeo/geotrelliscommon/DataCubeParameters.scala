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
  var partitionerIndexReduction: Option[Int] = Option.empty
  var resampleMethod: ResampleMethod = NearestNeighbor
  var maskingCube: Option[Object] = Option.empty
  var globalExtent:Option[ProjectedExtent] = Option.empty
  var pixelBufferX:Double = 0.0
  var pixelBufferY:Double = 0.0
  var noResampleOnRead: Boolean = false
  var useNewFeatureExtentIntersection: Boolean = true
  var useNewFeatureExtentIntersection2: Boolean = true
  var timeDimensionFilter: Option[java.io.Serializable] = Option.empty
  var allowEmptyCube: Boolean = false
  var useRasterSourceProviders: Boolean = true
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
   * Whether to resolve overlapping tiles before reading data, by selecting the best source
   * based on distance to footprint and CRS matching.
   */
  var resolveTileOverlap: Boolean = true

  /**
   * Configuration to override asset loading with synthetic data
   */
  var syntheticDataOverride: Option[SyntheticDataOverride] = None

  override def toString = s"DataCubeParameters($tileSize, $maskingStrategyParameters, $layoutScheme, $partitionerTemporalResolution, $partitionerIndexReduction, $maskingCube, $resampleMethod, $pixelBufferX, $pixelBufferY, $noResampleOnRead, $useNewFeatureExtentIntersection, $useNewFeatureExtentIntersection2)"

  def setPartitionerIndexReduction(reduction:Int): Unit = {
    if (reduction < 0) {
      partitionerIndexReduction = Option.empty
    } else {
      partitionerIndexReduction = Some(reduction)
    }
  }
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

  def setUseNewFeatureExtentIntersection(newFeatureExtentIntersection: Boolean): Unit = {
    useNewFeatureExtentIntersection = newFeatureExtentIntersection
  }

  def setUseNewFeatureExtentIntersection2(newFeatureExtentIntersection2: Boolean): Unit = {
    useNewFeatureExtentIntersection2 = newFeatureExtentIntersection2
  }

  def setTimeDimensionFilter(conditionProcessScriptBuilder:java.io.Serializable):Unit = {
    timeDimensionFilter = Some(conditionProcessScriptBuilder)
  }

  def setAllowEmptyCube(allowEmpty:Boolean):Unit = {
    allowEmptyCube = allowEmpty
  }

  def setUseRasterSourceProviders(flag: Boolean): Unit = {
    useRasterSourceProviders = flag
  }

  def setRetainNoDataTiles(retain:Boolean):Unit = {
    retainNoDataTiles = retain
  }

  def setResolveTileOverlap(resolve: Boolean): Unit = {
    resolveTileOverlap = resolve
  }

  def setSyntheticDataOverride(syntheticData: SyntheticDataOverride): Unit = {
    syntheticDataOverride = Some(syntheticData)
  }
}

case class SyntheticDataOverride(cellType: String, udf: Option[String])

