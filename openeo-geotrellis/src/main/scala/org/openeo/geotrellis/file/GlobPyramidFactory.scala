package org.openeo.geotrellis.file

import java.time.ZonedDateTime
import java.util
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.AbstractGlobFileLayerProvider
import org.openeo.geotrelliscommon.DataCubeParameters

class GlobPyramidFactory(layerProvider: AbstractGlobFileLayerProvider) {
  def datacube_seq(projectedPolygons: ProjectedPolygons, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    datacube_seq(projectedPolygons, from_date, to_date,null,null,null)
  }

  def datacube_seq(projectedPolygons: ProjectedPolygons, from_date: String, to_date: String,metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    Seq(0 -> layerProvider.readMultibandTileLayer(from, to, projectedPolygons, layerProvider.maxZoom, sc,Option(dataCubeParameters)))
  }

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, ProjectedPolygons(polygons, polygons_crs), zoom, sc,Option.empty[DataCubeParameters])
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
  }
}
