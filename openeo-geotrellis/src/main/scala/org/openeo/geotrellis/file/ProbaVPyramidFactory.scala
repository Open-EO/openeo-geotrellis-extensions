package org.openeo.geotrellis.file

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.{FileLayerProvider, ProbaVPathDateExtractor}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.ZonedDateTime
import java.util
import scala.collection.JavaConverters._

object ProbaVPyramidFactory {
  // Mapping from band name to (geotiff file id, band index).
  // E.g. PROBAV_S10_TOC_X35Y12_20190801_333M_GEOMETRY_V201.TIFF contains the SWIRVZA band at index 3.
  private val bandNameToAssetBandIndex = Map(
    "NDVI" -> ("NDVI", 0),
    "RED" -> ("RED", 0),
    "NIR" -> ("NIR", 0),
    "BLUE" -> ("BLUE", 0),
    "SWIR" -> ("SWIR", 0),
    "SZA" -> ("GEOMETRY", 0),
    "SAA" -> ("GEOMETRY", 1),
    "SWIRVAA" -> ("GEOMETRY", 2),
    "SWIRVZA" -> ("GEOMETRY", 3),
    "VNIRVAA" -> ("GEOMETRY", 4),
    "VNIRVZA" -> ("GEOMETRY", 5),
    "SM" -> ("SM", 0)
  )
}

class ProbaVPyramidFactory(openSearchEndpoint: String,
                           openSearchCollectionId: String,
                           bandNames: util.List[String],
                           rootPath: String,
                           maxSpatialResolution: CellSize) extends Serializable {
  require(bandNames.size() > 0)

  import ProbaVPyramidFactory._

  private val openSearchEndpointUrl = new URL(openSearchEndpoint)
  private val _bandNames = bandNames.asScala

  private def fileLayerProvider(correlationId: String) = {
    val (assetTitles, bandIndices) = _bandNames.map(bandNameToAssetBandIndex).unzip

    new FileLayerProvider(
        OpenSearchClient(openSearchEndpointUrl),
        openSearchCollectionId,
        openSearchLinkTitles = NonEmptyList.of(assetTitles.head, assetTitles.tail: _*),
        rootPath,
        maxSpatialResolution = maxSpatialResolution,
        pathDateExtractor = ProbaVPathDateExtractor,
        layoutScheme = ZoomedLayoutScheme(LatLng, 256),
        bandIds = bandIndices.map(Seq(_)), // actually: bandIndices (TODO: is there a point to these inner Seqs?)
        correlationId = correlationId,
    )
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  correlationId: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val layerProvider = fileLayerProvider(correlationId)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
    yield zoom -> {
        val tileLayerRdd = layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
        ContextRDD(tileLayerRdd, tileLayerRdd.metadata)
      }
  }

  def pyramid_seq(bbox: Extent,
                  bbox_srs: String,
                  from_date: String, to_date: String
                 ): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    pyramid_seq(bbox, bbox_srs, from_date, to_date, correlationId = "")


  def datacube_seq(polygons:ProjectedPolygons,
                   from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any],
                   correlationId: String, dataCubeParameters: DataCubeParameters
                  ): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val layerProvider = fileLayerProvider(correlationId)
    val polygons_crs = polygons.crs

    val boundingBox = ProjectedExtent(polygons.polygons.toSeq.extent, polygons_crs)
    val cube = layerProvider.readMultibandTileLayer(from, to, boundingBox, polygons.polygons, polygons_crs, 0, sc, Some(dataCubeParameters))
    Seq((0,cube))
  }

}
