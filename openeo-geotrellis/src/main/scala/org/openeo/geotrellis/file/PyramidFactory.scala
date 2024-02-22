package org.openeo.geotrellis.file

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, LayoutScheme, SpaceTimeKey, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.CellSize
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient
import org.slf4j.LoggerFactory

import java.net.URL
import java.time.ZonedDateTime
import java.util
import scala.collection.JavaConverters._

object PyramidFactory {
  private val logger = LoggerFactory.getLogger(PyramidFactory.getClass)
}

/**
 * Uses OpenSearch metadata lookup and file based access to create Pyramids.
 * Pyramids are sequences of (zoom_level, MultibandTileLayerRDD) tuples.
 * The sequence is ordered from highest to lowest zoom level,
 * where higher zoom levels contain more pixels (= smaller cell size).
 * https://geotrellis.readthedocs.io/en/latest/guide/core-concepts.html#pyramids
 *
 * This factory works for most collection types: S1 (Sigma0/Coherence), S2, S5P, AgEra5, Cgls, etc.
 *
 * @param openSearchClient
 * @param openSearchCollectionId
 * @param openSearchLinkTitles
 * @param rootPath
 * @param maxSpatialResolution The spatial resolution used at the highest zoom level. Its units depend on the CRS.
 * @param experimental
 */
class PyramidFactory(openSearchClient: OpenSearchClient,
                     openSearchCollectionId: String,
                     openSearchLinkTitles: util.List[String],
                     rootPath: String,
                     maxSpatialResolution: CellSize,
                     experimental: Boolean = false) {
  require(openSearchLinkTitles.size() > 0)

  import PyramidFactory._

  var crs: CRS = WebMercator

  /**
   * The FileLayerProvider used to generate MultibandTileLayers from files.
   *
   * @param metadataProperties
   * @param correlationId
   * @param layoutScheme Mapping from zoom level to LayoutDefinition.
   *                     The LayoutDefinition defines the number of tiles at this level and their size (#pixels).
   *                     Normally, the number of tiles increases by a factor of 2 with each zoom level, while
   *                     the size of each tile remains constant.
   *                     In case of a ZoomedLayoutScheme, the entire crs extent is divided into tiles of the same size.
   *                     In case of a FloatingLayoutScheme, only the provided extent is divided.
   * @return
   */
  private def fileLayerProvider(metadataProperties: Map[String, Any],
                               correlationId: String,
                               layoutScheme: LayoutScheme = ZoomedLayoutScheme(crs, 256)) = FileLayerProvider(
    openSearchClient,
    openSearchCollectionId,
    NonEmptyList.fromListUnsafe(openSearchLinkTitles.asScala.toList),
    rootPath,
    maxSpatialResolution,
    SplitYearMonthDayPathDateExtractor,
    metadataProperties,
    layoutScheme,
    correlationId = correlationId,
    experimental = experimental
  )

  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val cube = datacube(polygons.polygons, polygons.crs, from_date, to_date, metadata_properties, correlationId,new DataCubeParameters())
    Seq((0,cube))
  }

  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val cube = datacube(polygons.polygons, polygons.crs, from_date, to_date, metadata_properties, correlationId, dataCubeParameters)
    Seq((0,cube))
  }

  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    datacube_seq(polygons, from_date, to_date, metadata_properties, correlationId = "")

  def datacube(polygons:ProjectedPolygons, from_date: String, to_date: String,
               metadata_properties: util.Map[String, Any], correlationId: String): MultibandTileLayerRDD[SpaceTimeKey] =
    datacube(polygons.polygons, polygons.crs, from_date, to_date, metadata_properties, correlationId)

  /**
   * Creates a MultibandTileLayerRDD from the given polygons (bbox) and time range.
   */
  def datacube(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
               metadata_properties: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String,
               dataCubeParameters: DataCubeParameters = new DataCubeParameters()):
  MultibandTileLayerRDD[SpaceTimeKey] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    var cleanedPolygons = 0
    val polysCleaned = polygons map { p =>
      if (p.isValid) p
      else {
        cleanedPolygons += 1
        val validPolygon = p.union()
        validPolygon match  {
          case p: Polygon => MultiPolygon(p)
          case mp: MultiPolygon => mp
          case _ => throw new IllegalArgumentException("Union of polygons should result in a Polygon or MultiPolygon")
        }

      }
    }
    if (cleanedPolygons > 0) logger.warn(f"Cleaned up $cleanedPolygons polygon(s)")


    val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)
    val layerProvider = fileLayerProvider(metadata_properties.asScala.toMap, correlationId, FloatingLayoutScheme(dataCubeParameters.tileSize))
    layerProvider.readMultibandTileLayer(from, to, boundingBox, polysCleaned, polygons_crs, 0, sc, Some(dataCubeParameters))
  }

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int,
            metadataProperties: Map[String, Any] = Map(), correlationId: String)(implicit sc: SparkContext):
  MultibandTileLayerRDD[SpaceTimeKey] =
    fileLayerProvider(metadataProperties, correlationId).readMultibandTileLayer(from, to, boundingBox, zoom, sc)

  /**
   * Pyramid_seq is used for viewing services, which is an experimental feature that is not really used.
   * (See GpsSecondaryServices in the Geopyspark Python Driver.)
   * It uses a layout for the entire crs extent (ZoomedLayoutScheme),
   * dividing it into tiles that have 256x256 pixels, each with their own SpatialKey.
   *
   * This method is called from Python (Py4j), which is sensitive to the signature.
   */
  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val layerProvider = fileLayerProvider(metadata_properties.asScala.toMap, correlationId)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, metadata_properties: util.Map[String, Any]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    pyramid_seq(bbox, bbox_srs, from_date, to_date, metadata_properties, correlationId = "")

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any], correlationId: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val bbox = polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons_crs)
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val intersectsPolygons = AbstractPyramidFactory.preparePolygons(polygons, polygons_crs,sc)

    val layerProvider = fileLayerProvider(metadata_properties.asScala.toMap, correlationId)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> layerProvider.readMultibandTileLayer(from, to, boundingBox,intersectsPolygons,polygons_crs, zoom, sc,Option.empty)
  }

  def pyramid_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                  metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    pyramid_seq(polygons, polygons_crs, from_date, to_date, metadata_properties, correlationId = "")
}
