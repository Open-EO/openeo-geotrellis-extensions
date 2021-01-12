package org.openeo.geotrellis.file

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.GlobalNetCdfFileLayerProvider

import java.time.ZonedDateTime

class CglsPyramidFactory(dataGlob: String, bandName: String, dateRegex: String) {
  private val layerProvider = new GlobalNetCdfFileLayerProvider(dataGlob, bandName, dateRegex.r)

  def datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: optimize for sparse polygons
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    Seq(0 -> layerProvider.readMultibandTileLayer(from, to,
      ProjectedExtent(polygons.polygons.toSeq.extent, polygons.crs), layerProvider.maxZoom, sc))
  }

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: optimize for sparse polygons
    val sc = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
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
