package org.openeo.geotrellis.file

import java.time.ZonedDateTime
import java.util

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, LayoutScheme, SpaceTimeKey, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.FileLayerProvider

import scala.collection.JavaConverters._


/**
 * Pyramid factory based on OpenSearch metadata lookup and file based access.
 *
 * @param oscarsCollectionId
 * @param oscarsLinkTitles
 * @param rootPath
 */
class Sentinel2PyramidFactory(oscarsCollectionId: String, oscarsLinkTitles: util.List[String], rootPath: String) {
  require(oscarsLinkTitles.size() > 0)

  var crs: CRS = WebMercator

  private def sentinel2FileLayerProvider(metadataProperties: Map[String, Any],layoutScheme:LayoutScheme=ZoomedLayoutScheme(crs, 256)) = new FileLayerProvider(
    oscarsCollectionId,
    NonEmptyList.fromListUnsafe(oscarsLinkTitles.asScala.toList),
    rootPath,
    metadataProperties,
    layoutScheme
  )

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any] = util.Collections.emptyMap()): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val layerProvider = sentinel2FileLayerProvider(metadata_properties.asScala.toMap)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
  }

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val bbox = polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons_crs)
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val intersectsPolygons = AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

    val layerProvider = sentinel2FileLayerProvider(metadata_properties.asScala.toMap)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox,intersectsPolygons,polygons_crs, zoom, sc)
  }

  def datacube(polygons:ProjectedPolygons, from_date: String, to_date: String, metadata_properties: util.Map[String, Any]): MultibandTileLayerRDD[SpaceTimeKey] = {
    return datacube(polygons.polygons,polygons.crs,from_date,to_date,metadata_properties)
  }

  /**
   * Same as #datacube, but return same structure as pyramid_seq
   * @param polygons
   * @param from_date
   * @param to_date
   * @param metadata_properties
   * @return
   */
  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String, metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val cube = datacube(polygons.polygons, polygons.crs, from_date, to_date, metadata_properties)
    return Seq((0,cube))
  }

  def datacube(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String, metadata_properties: util.Map[String, Any] = util.Collections.emptyMap()): MultibandTileLayerRDD[SpaceTimeKey] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    val bbox = polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons_crs)
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val intersectsPolygons = AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

    val layerProvider = sentinel2FileLayerProvider(metadata_properties.asScala.toMap,FloatingLayoutScheme(256))
    layerProvider.readMultibandTileLayer(from, to, boundingBox,intersectsPolygons,polygons_crs, 0, sc)
  }


  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, metadataProperties: Map[String, Any] = Map())(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    sentinel2FileLayerProvider(metadataProperties).readMultibandTileLayer(from, to, boundingBox, zoom, sc)
}
