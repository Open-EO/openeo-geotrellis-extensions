package org.openeo.geotrellis.layers

import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.CRS
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.{RasterRegion, RasterSource}
import org.openeo.geotrellis.layers.raster_source.{GDALCloudRasterSource, IndexedRasterSource, ValueOffsetRasterSource}
import org.openeo.opensearch.OpenSearchResponses.Feature

import java.util
import scala.jdk.CollectionConverters._

object L1CFunctions {
  def getDilationDistance(maskParams: Map[String, Object]): Int = {
    // TODO: Find out best default dilation distance.
    maskParams.getOrElse("dilation_distance", "250").toString.toInt
  }

  def filterRasterSources(rasterSources: Seq[(RasterSource,Feature)],
                          maskParams: util.Map[String, Object]): Seq[(RasterSource,Feature)] = {
    val dilationDistance =  getDilationDistance(maskParams.asScala.toMap)
    // Filter out the entire BandCompositeRasterSource if it is fully clouded.
    val filteredSources = rasterSources.filter(compositeSource => {
      val rasterSource = compositeSource._1.asInstanceOf[BandCompositeRasterSource].sources.head
      rasterSource match {
        case rs: GDALCloudRasterSource =>
          // Filter out rasterSources that are fully clouded.
          !rs.getMergedPolygons(dilationDistance).exists(_.covers(rs.readExtent()))
        case _ => true // Keep raster sources that have no cloud data.
      }
    })
    if (filteredSources.isEmpty) throw new IllegalArgumentException("No non-clouded raster sources found")
    filteredSources
  }
  def isRegionFullyClouded(rasterRegion: RasterRegion, layoutCrs: CRS, layout: LayoutDefinition, dilationDistance: Int): Boolean = {
    val compositeRasterSource = rasterRegion.asInstanceOf[GridBoundsRasterRegion].source.asInstanceOf[BandCompositeRasterSource]
    val cloudRasterSource = (compositeRasterSource.sources.head match {
      case rsOffset: ValueOffsetRasterSource => rsOffset.rasterSource
      case indexedRasterSource: IndexedRasterSource => indexedRasterSource.rasterSource
      case rs => rs
    }).asInstanceOf[GDALCloudRasterSource]
    cloudRasterSource match {
      case rs: GDALCloudRasterSource =>
        val regionExtent = rasterRegion.extent.reproject(layoutCrs, rs.crs)
        // Filter out regions that are fully clouded.
        rs.getMergedPolygons(dilationDistance).exists(_.covers(regionExtent))
      case _ => false // Keep regions that have no cloud data.
    }
  }}

