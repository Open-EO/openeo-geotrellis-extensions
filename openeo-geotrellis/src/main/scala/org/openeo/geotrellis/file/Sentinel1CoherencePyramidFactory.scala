package org.openeo.geotrellis.file

import java.time.ZonedDateTime
import java.util

import be.vito.eodata.extracttimeseries.geotrellis.Sentinel1CoherenceFileLayerProvider
import cats.data.NonEmptyList
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.collection.Map

class Sentinel1CoherencePyramidFactory(oscarsCollectionId: String, oscarsLinkTitles: util.List[String], rootPath: String) {
  require(oscarsLinkTitles.size() > 0)

  private def sentinel1CoherenceOscarsPyramidFactory(metadataProperties: Map[String, Any]) = new Sentinel1CoherenceFileLayerProvider(
    oscarsCollectionId,
    NonEmptyList.fromListUnsafe(oscarsLinkTitles.asScala.toList),
    rootPath,
    metadataProperties
  )

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val bbox = polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons_crs)
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val intersectsPolygons = AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

    val layerProvider = sentinel1CoherenceOscarsPyramidFactory(metadata_properties.asScala)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox,intersectsPolygons,polygons_crs, zoom, sc)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any] = util.Collections.emptyMap()): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val layerProvider = sentinel1CoherenceOscarsPyramidFactory(metadata_properties.asScala)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
  }

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, metadataProperties: Map[String, Any] = Map())(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    sentinel1CoherenceOscarsPyramidFactory(metadataProperties).readMultibandTileLayer(from, to, boundingBox, zoom, sc)
}
