package org.openeo.geotrellis.file

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.ProjectedPolygons

import java.time.LocalDate
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Collections.{emptyMap, singletonList}
// import org.openeo.geotrellis.TestImplicits._

object Sentinel5PPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel5PPyramidFactoryTest {

  @Test
  def testSentinel5P(): Unit = {
    val bbox = ProjectedExtent(Extent(-5.52612, 51.2654, -2.31262, 52.5864), LatLng)
    val srs = s"EPSG:${bbox.crs.epsgCode.get}"

    assertTrue(s"${bbox.extent.width}", bbox.extent.width > 0.05)
    assertTrue(s"${bbox.extent.height}", bbox.extent.height > 0.05)

    val date = LocalDate.of(2020, 1, 1).atStartOfDay(UTC)

    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
    val to_date = from_date
    val metadata_properties = emptyMap[String, Any]()
    val correlation_id = "testSentinel5P"

    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, srs)

    val dailyCOPyramidFactory = new Sentinel2PyramidFactory(
      openSearchEndpoint = "https://services.terrascope.be/catalogue",
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1",
      openSearchLinkTitles = singletonList("CO"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_CO_TD_V1",
      maxSpatialResolution = CellSize(0.05,0.05)
    )

    val Seq((_, baseLayerByDatacube_seq)) = dailyCOPyramidFactory.datacube_seq(
        projectedPolygons,
        from_date,
        to_date,
        metadata_properties,
        correlation_id
    )

    val baseLayerByPyramid_seq_polygons = {
      val layersByZoomLevel = dailyCOPyramidFactory.pyramid_seq(
        projectedPolygons.polygons,
        projectedPolygons.crs,
        from_date,
        to_date,
        metadata_properties,
        correlation_id
      )

      val (maxZoom, baseLayer) = layersByZoomLevel
        .maxBy { case (zoom, _) => zoom }

      assertEquals(5, maxZoom)

      baseLayer
    }

    val baseLayerByPyramid_seq_extent = {
      val layersByZoomLevel = dailyCOPyramidFactory.pyramid_seq(
        bbox.extent,
        srs,
        from_date,
        to_date,
        metadata_properties,
        correlation_id
      )

      val (maxZoom, baseLayer) = layersByZoomLevel
        .maxBy { case (zoom, _) => zoom }

      assertEquals(5, maxZoom)

      baseLayer
    }

    /*baseLayerByDatacube_seq.toSpatial(date).writeGeoTiff("/tmp/testSentinel5P_cropped_datacube.tif", bbox)
    baseLayerByPyramid_seq_polygons.toSpatial(date).writeGeoTiff("/tmp/testSentinel5P_cropped_polygons.tif", bbox)
    baseLayerByPyramid_seq_extent.toSpatial(date).writeGeoTiff("/tmp/testSentinel5P_cropped_extent.tif", bbox)*/

    def polygonalMean(layer: MultibandTileLayerRDD[SpaceTimeKey]): Double = {
      val Summary(Array(singleBandMean)) = layer
        .toSpatial(date)
        .polygonalSummaryValue(bbox.extent.toPolygon(), MeanVisitor)

      singleBandMean.mean
    }

    val qgisZonalStaticsPluginResult = 8.52374670184697
    assertEquals(qgisZonalStaticsPluginResult, polygonalMean(baseLayerByDatacube_seq), 0.01)
    assertEquals(qgisZonalStaticsPluginResult, polygonalMean(baseLayerByPyramid_seq_polygons), 0.01)
    assertEquals(qgisZonalStaticsPluginResult, polygonalMean(baseLayerByPyramid_seq_extent), 0.01)
  }
}
