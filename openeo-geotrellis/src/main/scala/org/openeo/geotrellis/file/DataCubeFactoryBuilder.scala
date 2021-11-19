package org.openeo.geotrellis.file

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize

import java.util

/**
 * A builder for data cube factories, that can be used from Python.
 * The builder pattern supports features like default values, that exist in Scala but are otherwise not usable from Python.
 * It also simplifies API migrations.
 */
class DataCubeFactoryBuilder() {

  private var openSearchEndpoint: String = _
  private var openSearchCollectionId: String = _
  private var openSearchLinkTitles: util.List[String] = _
  private var rootPath: String = ""
  private var maxSpatialResolution: CellSize = _
  private var crs:Option[String] = Option.empty
  private var experimental:Boolean=false

  def catalogURL(url:String): DataCubeFactoryBuilder = {
    openSearchEndpoint = url
    return this
  }

  def collectionId(id:String): DataCubeFactoryBuilder = {
    openSearchCollectionId = id
    this
  }

  def bandNames(names: util.List[String]): DataCubeFactoryBuilder = {
    openSearchLinkTitles = names
    this
  }

  def rootPath(path:String): DataCubeFactoryBuilder = {
    rootPath = path
    this
  }

  def maxSpatialResolution(x:Double, y:Double): DataCubeFactoryBuilder = {
    maxSpatialResolution = CellSize(x,y)
    this
  }

  /**
   * This is the CRS in which the cellsize is defined.
   */
  def defaultCRS(crs: String): DataCubeFactoryBuilder = {
    this.crs = Some(crs)
    this
  }

  def experimental(x:Boolean):DataCubeFactoryBuilder = {
    experimental = x
    this
  }

  def build(): Sentinel2PyramidFactory = {
    val factory = new Sentinel2PyramidFactory(openSearchEndpoint,openSearchCollectionId,openSearchLinkTitles,rootPath,maxSpatialResolution,experimental)
    if(this.crs.isDefined && this.crs.get != "UTM" && this.crs.get != "AUTO:42001") {
      //TODO: pyramid CRS has to remain webmercator
      factory.crs = CRS.fromName(this.crs.get)
    }
    factory
  }

}
