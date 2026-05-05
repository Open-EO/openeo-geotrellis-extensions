package org.openeo.geotrellis.file

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.opensearch.OpenSearchResponses.{Feature, FeatureCollection, Link}

import java.net.{URI, URL}
import java.time.{LocalDate, ZonedDateTime}
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.util.Collections.{emptyMap, singletonList}
// import org.openeo.geotrellis.TestImplicits._

object Sentinel5PPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[2]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel5PPyramidFactoryTest {

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testSentinel5P(): Unit = {
    val bbox = ProjectedExtent(Extent(-5.52612, 51.2654, -2.31262, 52.5864), LatLng)
    val srs = s"EPSG:${bbox.crs.epsgCode.get}"

    assertTrue(bbox.extent.width > 0.05, s"${bbox.extent.width}")
    assertTrue(bbox.extent.height > 0.05, s"${bbox.extent.height}")

    val date = LocalDate.of(2020, 1, 1).atStartOfDay(UTC)

    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
    val to_date = from_date
    val metadata_properties = emptyMap[String, Any]()
    val correlation_id = "testSentinel5P"

    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, srs)

    val openSearchEndpoint = "https://services.terrascope.be/catalogue"
//    val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = false)

    val openSearchClient = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """
        |{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1:S5P_L3_CO_TD_20200101_V100",
        |            "geometry": {"coordinates":[[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]]],"type":"MultiPolygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2020-01-01T00:00:00Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1:S5P_L3_CO_TD_20200101_V100","available":"2021-02-08T11:19:31Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2021-02-08T11:18:19.644Z","processingMode":"OFFL","productType":"CO_TD","availabilityTime":"2021-02-08T11:19:31Z"},"links":{"related":[{"length":1257340,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_CO_TD_V1/2020/01/S5P_OFFL_L3_CO_TD_20200101_V100/S5P_CO_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":4679776,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_CO_TD_V1/2020/01/S5P_OFFL_L3_CO_TD_20200101_V100/S5P_CO_TD_20200101_CO_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"CO","bandNames":["CO"]}],"previews":[],"alternates":[]},"published":"2021-02-08T11:19:31Z","title":"S5P_L3_CO_TD_20200101_V100","updated":"2021-02-08T11:18:19.644Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:00:00Z","endingDateTime":"2020-01-01T23:59:59Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__CO_____20200101T005246_20200101T023416_11487_01_010302_20200102T143721"},{"title":"S5P_OFFL_L2__CO_____20200101T023416_20200101T041546_11488_01_010302_20200102T161725"},{"title":"S5P_OFFL_L2__CO_____20200101T041546_20200101T055716_11489_01_010302_20200102T175739"},{"title":"S5P_OFFL_L2__CO_____20200101T055716_20200101T073846_11490_01_010302_20200102T194736"},{"title":"S5P_OFFL_L2__CO_____20200101T073846_20200101T092016_11491_01_010302_20200102T212738"},{"title":"S5P_OFFL_L2__CO_____20200101T092016_20200101T110146_11492_01_010302_20200102T230740"},{"title":"S5P_OFFL_L2__CO_____20200101T110146_20200101T124316_11493_01_010302_20200103T004856"},{"title":"S5P_OFFL_L2__CO_____20200101T124316_20200101T142446_11494_01_010302_20200103T022748"},{"title":"S5P_OFFL_L2__CO_____20200101T142446_20200101T160616_11495_01_010302_20200103T040909"},{"title":"S5P_OFFL_L2__CO_____20200101T160616_20200101T174746_11496_01_010302_20200103T054754"},{"title":"S5P_OFFL_L2__CO_____20200101T174746_20200101T192916_11497_01_010302_20200103T073756"},{"title":"S5P_OFFL_L2__CO_____20200101T192916_20200101T211046_11498_01_010302_20200103T091800"},{"title":"S5P_OFFL_L2__CO_____20200101T211046_20200101T225216_11499_01_010302_20200103T105803"},{"title":"S5P_OFFL_L2__CO_____20200101T225216_20200102T003346_11500_01_010302_20200103T123927"}]}}
        |         }
        |    ]
        |  }
        |""".stripMargin).features.foreach(feature => openSearchClient.addFeature(feature))
//    FeatureCollection.parse(
//    """{
//      |    "features": [
//      |        {
//      |            "type": "Feature",
//      |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1:S5P_L3_CO_TD_20200101_V100",
//      |            "geometry": {"coordinates":[[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]]],"type":"MultiPolygon"},
//      |            "bbox": [-180.0,-89.0,180.0,89.0],
//      |            "properties":
//      |            	{"date":"2020-01-01T00:00:00Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1:S5P_L3_CO_TD_20200101_V100","available":"2021-02-08T11:19:31Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_CO_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2021-02-08T11:18:19.644Z","processingMode":"OFFL","productType":"CO_TD","availabilityTime":"2021-02-08T11:19:31Z"},"links":{"related":[{"length":1257340,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_CO_TD_V1/2020/01/S5P_OFFL_L3_CO_TD_20200101_V100/S5P_CO_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","category":"QUALITY"}],"data":[{"length":4679776,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_CO_TD_V1/2020/01/S5P_OFFL_L3_CO_TD_20200101_V100/S5P_CO_TD_20200101_CO_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"CO","bandNames":["WEIGHT"]}],"previews":[],"alternates":[]},"published":"2021-02-08T11:19:31Z","title":"S5P_L3_CO_TD_20200101_V100","updated":"2021-02-08T11:18:19.644Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:00:00Z","endingDateTime":"2020-01-01T23:59:59Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__CO_____20200101T005246_20200101T023416_11487_01_010302_20200102T143721"},{"title":"S5P_OFFL_L2__CO_____20200101T023416_20200101T041546_11488_01_010302_20200102T161725"},{"title":"S5P_OFFL_L2__CO_____20200101T041546_20200101T055716_11489_01_010302_20200102T175739"},{"title":"S5P_OFFL_L2__CO_____20200101T055716_20200101T073846_11490_01_010302_20200102T194736"},{"title":"S5P_OFFL_L2__CO_____20200101T073846_20200101T092016_11491_01_010302_20200102T212738"},{"title":"S5P_OFFL_L2__CO_____20200101T092016_20200101T110146_11492_01_010302_20200102T230740"},{"title":"S5P_OFFL_L2__CO_____20200101T110146_20200101T124316_11493_01_010302_20200103T004856"},{"title":"S5P_OFFL_L2__CO_____20200101T124316_20200101T142446_11494_01_010302_20200103T022748"},{"title":"S5P_OFFL_L2__CO_____20200101T142446_20200101T160616_11495_01_010302_20200103T040909"},{"title":"S5P_OFFL_L2__CO_____20200101T160616_20200101T174746_11496_01_010302_20200103T054754"},{"title":"S5P_OFFL_L2__CO_____20200101T174746_20200101T192916_11497_01_010302_20200103T073756"},{"title":"S5P_OFFL_L2__CO_____20200101T192916_20200101T211046_11498_01_010302_20200103T091800"},{"title":"S5P_OFFL_L2__CO_____20200101T211046_20200101T225216_11499_01_010302_20200103T105803"},{"title":"S5P_OFFL_L2__CO_____20200101T225216_20200102T003346_11500_01_010302_20200103T123927"}]}}
//      |         }
//      |    ]
//      |  }""".stripMargin).features.foreach(feature => openSearchClient.addFeature(feature))

    val dailyCOPyramidFactory = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "whatever",
      openSearchLinkTitles = singletonList("CO"),
      rootPath = "/whatever",
      maxSpatialResolution = CellSize(0.05,0.05)
    )
    dailyCOPyramidFactory.crs = LatLng

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
