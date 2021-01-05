package org.openeo.geotrellis.file

import java.util

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.{Extent, MultiPolygon}
import org.openeo.geotrellis.ProjectedPolygons

class Sentinel5PPyramidFactory(openSearchEndpoint: String, openSearchCollectionId: String,
                               openSearchLinkTitles: util.List[String], rootPath: String) {

  private val impl = new Sentinel2PyramidFactory(
    openSearchEndpoint,
    openSearchCollectionId,
    openSearchLinkTitles,
    rootPath,
    maxSpatialResolution = CellSize(0.05, 0.05)
  )

  impl.crs = LatLng

  def datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    impl.datacube_seq(polygons, from_date, to_date, metadata_properties, correlationId)

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any], correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    impl.pyramid_seq(polygons, polygons_crs, from_date, to_date, metadata_properties, correlationId)

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    impl.pyramid_seq(bbox, bbox_srs, from_date, to_date, metadata_properties, correlationId)
}
