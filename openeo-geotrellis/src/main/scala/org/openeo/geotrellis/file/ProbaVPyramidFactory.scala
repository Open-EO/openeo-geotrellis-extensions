package org.openeo.geotrellis.file

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, MultibandTile}
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

  object Band extends Enumeration {
    // Jesus Christ almighty
    private[file] case class Val(fileMarker: String) extends super.Val
    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    val NDVI = Val("NDVI")
    val RED = Val("RADIOMETRY:0")
    val NIR = Val("RADIOMETRY:1")
    val BLUE = Val("RADIOMETRY:2")
    val SWIR = Val("RADIOMETRY:3")
    val SZA = Val("GEOMETRY:0")
    val SAA = Val("GEOMETRY:1")
    val SWIRVAA = Val("GEOMETRY:2")
    val SWIRVZA = Val("GEOMETRY:3")
    val VNIRVAA = Val("GEOMETRY:4")
    val VNIRVZA = Val("GEOMETRY:5")
    val SM = Val("SM")
  }
}

class ProbaVPyramidFactory(openSearchEndpoint: String, openSearchCollectionId: String, rootPath: String, maxSpatialResolution: CellSize) extends Serializable {

  import ProbaVPyramidFactory._

  private val openSearchEndpointUrl = new URL(openSearchEndpoint)

  private def probaVOpenSearchPyramidFactory(bands: Seq[Band.Value], correlationId: String) = {
    val openSearchLinkTitlesWithBandIds = bands.map(b => {
      val split = b.fileMarker.split(":")
      val band = split(0)
      val index = if (split.length > 1) split(1).toInt else 0

      (band, index)
    }).groupBy(_._1)
      .map({case (k, v) => (k, v.map(_._2))})
      .toList
    new FileLayerProvider(
      OpenSearchClient(openSearchEndpointUrl),
      openSearchCollectionId,
      NonEmptyList.fromListUnsafe(openSearchLinkTitlesWithBandIds.map(_._1)),
      rootPath,
      maxSpatialResolution = maxSpatialResolution,
      pathDateExtractor = ProbaVPathDateExtractor,
      layoutScheme = ZoomedLayoutScheme(LatLng, 256),
      bandIds = openSearchLinkTitlesWithBandIds.map(_._2),
      correlationId = correlationId
      )
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int],
                  correlationId: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[((Band.Value, Int), Int)] = bandsFromIndices(band_indices)

    val layerProvider = probaVOpenSearchPyramidFactory(bands.map(_._1._1), correlationId)

    for (zoom <- layerProvider.maxZoom to 0 by -1)
      yield zoom -> {
        val tileLayerRdd = layerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
        val orderedBandsRdd = tileLayerRdd
          .mapValues(t => MultibandTile(bands.sortBy(_._1._2).map(b => t.band(b._2)):_*))
        ContextRDD(orderedBandsRdd, tileLayerRdd.metadata)
      }
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    pyramid_seq(bbox, bbox_srs, from_date, to_date, band_indices, correlationId = "")

  private def bandsFromIndices(band_indices: util.List[Int]): Seq[((Band.Value, Int), Int)] = {
    val bands =
      if (band_indices == null) Band.values.toSeq
        else band_indices.asScala map Band.apply

    bands.zipWithIndex.sortBy(_._1.fileMarker).zipWithIndex
  }

  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters, band_indices: java.util.List[Int]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {

    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[((Band.Value, Int), Int)] = bandsFromIndices(band_indices)
    val layerProvider = probaVOpenSearchPyramidFactory(bands.map(_._1._1), correlationId)

    val polygons_crs = polygons.crs

    val boundingBox = ProjectedExtent(polygons.polygons.toSeq.extent, polygons_crs)
    val cube = layerProvider.readMultibandTileLayer(from, to, boundingBox, polygons.polygons, polygons_crs, 0, sc, Some(dataCubeParameters))
    Seq((0,cube))
  }

}
