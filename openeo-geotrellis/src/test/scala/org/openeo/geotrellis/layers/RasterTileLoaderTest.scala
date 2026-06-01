package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, LayoutTileSource, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{CellSize, MultibandTile, RasterSource}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.EnabledIf
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis._
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.layers.FileLayerProvider.rasterSourceRDD
import org.openeo.geotrelliscommon.DatacubeSupport._
import org.openeo.geotrelliscommon.{DataCubeParameters, NoCloudFilterStrategy, SyntheticDataOverride}
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.io.{BufferedSource, Source}

object RasterTileLoaderTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[RasterTileLoaderTest])
  private var _sc: Option[SparkContext] = None

  private def sc: SparkContext = {
    if (_sc.isEmpty) {
      println("Creating SparkContext")

      val conf = new SparkConf()
        .set("spark.ui.enabled", "true")
      val sc = SparkUtils.createLocalSparkContext(
        "local[1]",
        appName = classOf[RasterTileLoaderTest].getName,
        conf,
      )
      if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
      _sc = Some(sc)
    }
    _sc.get
  }

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = {
    sc
  }

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    if (_sc.isDefined) {
      _sc.get.stop()
      _sc = None
    }
  }
}

class RasterTileLoaderTest extends RasterMatchers {

  import RasterTileLoaderTest._

  private def sentinel5PMaxSpatialResolution = CellSize(0.05, 0.05)

  private def sentinel5PLayoutScheme = FloatingLayoutScheme(64)

  private def sentinel5PCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1"

  private def sentinel5PJsonStringFileLayerProvider(jsonFeaturesString: String) = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(jsonFeaturesString).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = sentinel5PCollectionId,
      NonEmptyList.one("NO2"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
      maxSpatialResolution = sentinel5PMaxSpatialResolution,
      new Sentinel5PPathDateExtractor(maxDepth = 3),
      layoutScheme = sentinel5PLayoutScheme
    )
  }


  private def sentinel5PFileLayerProvider = {
    val client = new FixedFeaturesOpenSearchClient
    val source: BufferedSource = Source.fromResource("org/openeo/geotrellis/sentinel5PFileLayerProvider_features.json")
    FeatureCollection.parse(
      source.getLines().mkString("")
    ).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = sentinel5PCollectionId,
      NonEmptyList.one("NO2"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
      maxSpatialResolution = sentinel5PMaxSpatialResolution,
      new Sentinel5PPathDateExtractor(maxDepth = 3),
      layoutScheme = sentinel5PLayoutScheme
    )
  }

  private def _getSentinel5PRasterSources(bbox: ProjectedExtent, date: ZonedDateTime, zoom: Int, featuresJsonString: Option[String] = None): (RDD[LayoutTileSource[SpaceTimeKey]], TileLayerMetadata[SpaceTimeKey]) = {
    val fileLayerProvider = if (featuresJsonString.isDefined) sentinel5PJsonStringFileLayerProvider(featuresJsonString.get) else sentinel5PFileLayerProvider

    val overlappingRasterSources: Seq[RasterSource] = fileLayerProvider.loadRasterSourceRDD(bbox, date, date, zoom).map(_._1)
    val commonCellType = overlappingRasterSources.head.cellType
    val metadata = layerMetadata(bbox, date, date, zoom min zoom, commonCellType, sentinel5PLayoutScheme, sentinel5PMaxSpatialResolution)

    val rasterSources = rasterSourceRDD(overlappingRasterSources, metadata, sentinel5PMaxSpatialResolution, sentinel5PCollectionId)(sc)
    (rasterSources, metadata)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def retainNoDataTilesTest(): Unit = {
    val jsonFeaturesString =
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2019-12-31T01:11:47.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100","available":"2021-02-08T10:47:24Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:10:39.950Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:24Z"},"links":{"related":[{"length":2218326,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5454742,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:24Z","title":"S5P_L3_NO2_TD_20191231_V100","bandNames":["S5P_L3_NO2_TD_20191231_V100"],"updated":"2023-03-02T10:10:39.950Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2019-12-31T01:11:47.000Z","endingDateTime":"2020-01-01T00:52:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20191231T025317_20191231T043447_11474_01_010302_20200101T192226"},{"title":"S5P_OFFL_L2__NO2____20191231T130217_20191231T144347_11480_01_010302_20200102T060402"},{"title":"S5P_OFFL_L2__NO2____20191231T061617_20191231T075747_11476_01_010302_20200101T232513"},{"title":"S5P_OFFL_L2__NO2____20191231T075747_20191231T093917_11477_01_010302_20200102T004148"},{"title":"S5P_OFFL_L2__NO2____20191231T011147_20191231T025317_11473_01_010302_20200101T180133"},{"title":"S5P_OFFL_L2__NO2____20191231T112047_20191231T130217_11479_01_010302_20200102T043947"},{"title":"S5P_OFFL_L2__NO2____20191231T180646_20191231T194816_11483_01_010302_20200102T112434"},{"title":"S5P_OFFL_L2__NO2____20191231T093917_20191231T112047_11478_01_010302_20200102T023550"},{"title":"S5P_OFFL_L2__NO2____20191231T212946_20191231T231116_11485_01_010302_20200102T140733"},{"title":"S5P_OFFL_L2__NO2____20191231T231116_20200101T005246_11486_01_010302_20200102T155014"},{"title":"S5P_OFFL_L2__NO2____20191231T194816_20191231T212946_11484_01_010302_20200102T123941"},{"title":"S5P_OFFL_L2__NO2____20191231T162516_20191231T180646_11482_01_010302_20200102T084547"},{"title":"S5P_OFFL_L2__NO2____20191231T144347_20191231T162516_11481_01_010302_20200102T070221"},{"title":"S5P_OFFL_L2__NO2____20191231T043447_20191231T061617_11475_01_010302_20200101T212406"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2020-01-01T00:52:46.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100","available":"2021-02-08T10:47:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:08:00.205Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:38Z"},"links":{"related":[{"length":2259627,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5431118,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:38Z","title":"S5P_L3_NO2_TD_20200101_V100","bandNames":["S5P_L3_NO2_TD_20200101_V100"],"updated":"2023-03-02T10:08:00.205Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:52:46.000Z","endingDateTime":"2020-01-02T00:33:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20200101T174746_20200101T192916_11497_01_010302_20200103T104405"},{"title":"S5P_OFFL_L2__NO2____20200101T225216_20200102T003346_11500_01_010302_20200103T153646"},{"title":"S5P_OFFL_L2__NO2____20200101T073846_20200101T092016_11491_01_010302_20200103T003802"},{"title":"S5P_OFFL_L2__NO2____20200101T192916_20200101T211046_11498_01_010302_20200103T121312"},{"title":"S5P_OFFL_L2__NO2____20200101T211046_20200101T225216_11499_01_010302_20200103T140339"},{"title":"S5P_OFFL_L2__NO2____20200101T142446_20200101T160616_11495_01_010302_20200103T065547"},{"title":"S5P_OFFL_L2__NO2____20200101T160616_20200101T174746_11496_01_010302_20200103T083756"},{"title":"S5P_OFFL_L2__NO2____20200101T110146_20200101T124316_11493_01_010302_20200103T041218"},{"title":"S5P_OFFL_L2__NO2____20200101T092016_20200101T110146_11492_01_010302_20200103T021108"},{"title":"S5P_OFFL_L2__NO2____20200101T055716_20200101T073846_11490_01_010302_20200102T225627"},{"title":"S5P_OFFL_L2__NO2____20200101T124316_20200101T142446_11494_01_010302_20200103T054233"},{"title":"S5P_OFFL_L2__NO2____20200101T041546_20200101T055716_11489_01_010302_20200102T210644"},{"title":"S5P_OFFL_L2__NO2____20200101T005246_20200101T023416_11487_01_010302_20200102T172632"},{"title":"S5P_OFFL_L2__NO2____20200101T023416_20200101T041546_11488_01_010302_20200102T190100"}]}}
        |         }
        |    ]
        |  }""".stripMargin

    val bbox1 = ProjectedExtent(Extent(xmin = 0.0, ymin = 0.0, xmax = 30.0, ymax = 10.0), LatLng)
    val bbox2 = ProjectedExtent(Extent(xmin = 50.0, ymin = 20.0, xmax = 60.0, ymax = 40.0), LatLng)
    val fullBbox = ProjectedExtent(bbox1.extent.combine(bbox2.extent), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))

    val params = new DataCubeParameters()
    params.layoutScheme = "FloatingLayoutScheme"
    params.globalExtent = Some(fullBbox)
    params.tileSize = 64
    params.retainNoDataTiles = true

    val polygons1 = MultiPolygon(fullBbox.extent.toPolygon())
    val (rasterSources1, metadata1) = _getSentinel5PRasterSources(fullBbox, date, 0, featuresJsonString = Some(jsonFeaturesString))
    val resultRetainNoDatatiles = RasterTileLoader.readMultibandTileLayer(rasterSources1, metadata1, Array(polygons1),
      fullBbox.crs, sc, NoCloudFilterStrategy, datacubeParams = Some(params))
    val resultRetainNoDatatilesColl = resultRetainNoDatatiles.collect()
    assertEquals(1, resultRetainNoDatatilesColl.count(_._2.isInstanceOf[EmptyMultibandTile]))
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def sparsePartitionerMergeTest(): Unit = {
    val zoom = 6
    // Create the first RDD.
    val bbox1 = ProjectedExtent(Extent(xmin = 55.0, ymin = 20.0, xmax = 60.0, ymax = 25.0), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))
    val polygons1 = MultiPolygon(bbox1.extent.toPolygon())
    val featuresJsonString1 =
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2019-12-31T01:11:47.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100","available":"2021-02-08T10:47:24Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:10:39.950Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:24Z"},"links":{"related":[{"length":2218326,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5454742,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:24Z","title":"S5P_L3_NO2_TD_20191231_V100","bandNames":["S5P_L3_NO2_TD_20191231_V100"],"updated":"2023-03-02T10:10:39.950Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2019-12-31T01:11:47.000Z","endingDateTime":"2020-01-01T00:52:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20191231T025317_20191231T043447_11474_01_010302_20200101T192226"},{"title":"S5P_OFFL_L2__NO2____20191231T130217_20191231T144347_11480_01_010302_20200102T060402"},{"title":"S5P_OFFL_L2__NO2____20191231T061617_20191231T075747_11476_01_010302_20200101T232513"},{"title":"S5P_OFFL_L2__NO2____20191231T075747_20191231T093917_11477_01_010302_20200102T004148"},{"title":"S5P_OFFL_L2__NO2____20191231T011147_20191231T025317_11473_01_010302_20200101T180133"},{"title":"S5P_OFFL_L2__NO2____20191231T112047_20191231T130217_11479_01_010302_20200102T043947"},{"title":"S5P_OFFL_L2__NO2____20191231T180646_20191231T194816_11483_01_010302_20200102T112434"},{"title":"S5P_OFFL_L2__NO2____20191231T093917_20191231T112047_11478_01_010302_20200102T023550"},{"title":"S5P_OFFL_L2__NO2____20191231T212946_20191231T231116_11485_01_010302_20200102T140733"},{"title":"S5P_OFFL_L2__NO2____20191231T231116_20200101T005246_11486_01_010302_20200102T155014"},{"title":"S5P_OFFL_L2__NO2____20191231T194816_20191231T212946_11484_01_010302_20200102T123941"},{"title":"S5P_OFFL_L2__NO2____20191231T162516_20191231T180646_11482_01_010302_20200102T084547"},{"title":"S5P_OFFL_L2__NO2____20191231T144347_20191231T162516_11481_01_010302_20200102T070221"},{"title":"S5P_OFFL_L2__NO2____20191231T043447_20191231T061617_11475_01_010302_20200101T212406"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2020-01-01T00:52:46.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100","available":"2021-02-08T10:47:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:08:00.205Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:38Z"},"links":{"related":[{"length":2259627,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5431118,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:38Z","title":"S5P_L3_NO2_TD_20200101_V100","bandNames":["S5P_L3_NO2_TD_20200101_V100"],"updated":"2023-03-02T10:08:00.205Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:52:46.000Z","endingDateTime":"2020-01-02T00:33:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20200101T174746_20200101T192916_11497_01_010302_20200103T104405"},{"title":"S5P_OFFL_L2__NO2____20200101T225216_20200102T003346_11500_01_010302_20200103T153646"},{"title":"S5P_OFFL_L2__NO2____20200101T073846_20200101T092016_11491_01_010302_20200103T003802"},{"title":"S5P_OFFL_L2__NO2____20200101T192916_20200101T211046_11498_01_010302_20200103T121312"},{"title":"S5P_OFFL_L2__NO2____20200101T211046_20200101T225216_11499_01_010302_20200103T140339"},{"title":"S5P_OFFL_L2__NO2____20200101T142446_20200101T160616_11495_01_010302_20200103T065547"},{"title":"S5P_OFFL_L2__NO2____20200101T160616_20200101T174746_11496_01_010302_20200103T083756"},{"title":"S5P_OFFL_L2__NO2____20200101T110146_20200101T124316_11493_01_010302_20200103T041218"},{"title":"S5P_OFFL_L2__NO2____20200101T092016_20200101T110146_11492_01_010302_20200103T021108"},{"title":"S5P_OFFL_L2__NO2____20200101T055716_20200101T073846_11490_01_010302_20200102T225627"},{"title":"S5P_OFFL_L2__NO2____20200101T124316_20200101T142446_11494_01_010302_20200103T054233"},{"title":"S5P_OFFL_L2__NO2____20200101T041546_20200101T055716_11489_01_010302_20200102T210644"},{"title":"S5P_OFFL_L2__NO2____20200101T005246_20200101T023416_11487_01_010302_20200102T172632"},{"title":"S5P_OFFL_L2__NO2____20200101T023416_20200101T041546_11488_01_010302_20200102T190100"}]}}
        |         }
        |    ]
        |  }""".stripMargin
    val (rasterSources1, metadata1) = _getSentinel5PRasterSources(bbox1, date, zoom, Some(featuresJsonString1))
    val sparseBaseLayer = RasterTileLoader.readMultibandTileLayer(rasterSources1, metadata1, Array(polygons1),
      bbox1.crs, sc,
      NoCloudFilterStrategy)
    val defaultBaseLayer = RasterTileLoader.readMultibandTileLayer(rasterSources1, metadata1, Array(polygons1),
      bbox1.crs, sc,
      NoCloudFilterStrategy,
      useSparsePartitioner = false)

    // Create the second RDD.
    val bbox2 = ProjectedExtent(Extent(xmin = 58.0, ymin = 20.0, xmax = 62.0, ymax = 25.0), LatLng)
    val polygons2 = MultiPolygon(bbox2.extent.toPolygon())
    val featuresJsonString2 =
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2019-12-31T01:11:47.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100","available":"2021-02-08T10:47:24Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:10:39.950Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:24Z"},"links":{"related":[{"length":2218326,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5454742,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:24Z","title":"S5P_L3_NO2_TD_20191231_V100","bandNames":["S5P_L3_NO2_TD_20191231_V100"],"updated":"2023-03-02T10:10:39.950Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2019-12-31T01:11:47.000Z","endingDateTime":"2020-01-01T00:52:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20191231T025317_20191231T043447_11474_01_010302_20200101T192226"},{"title":"S5P_OFFL_L2__NO2____20191231T130217_20191231T144347_11480_01_010302_20200102T060402"},{"title":"S5P_OFFL_L2__NO2____20191231T061617_20191231T075747_11476_01_010302_20200101T232513"},{"title":"S5P_OFFL_L2__NO2____20191231T075747_20191231T093917_11477_01_010302_20200102T004148"},{"title":"S5P_OFFL_L2__NO2____20191231T011147_20191231T025317_11473_01_010302_20200101T180133"},{"title":"S5P_OFFL_L2__NO2____20191231T112047_20191231T130217_11479_01_010302_20200102T043947"},{"title":"S5P_OFFL_L2__NO2____20191231T180646_20191231T194816_11483_01_010302_20200102T112434"},{"title":"S5P_OFFL_L2__NO2____20191231T093917_20191231T112047_11478_01_010302_20200102T023550"},{"title":"S5P_OFFL_L2__NO2____20191231T212946_20191231T231116_11485_01_010302_20200102T140733"},{"title":"S5P_OFFL_L2__NO2____20191231T231116_20200101T005246_11486_01_010302_20200102T155014"},{"title":"S5P_OFFL_L2__NO2____20191231T194816_20191231T212946_11484_01_010302_20200102T123941"},{"title":"S5P_OFFL_L2__NO2____20191231T162516_20191231T180646_11482_01_010302_20200102T084547"},{"title":"S5P_OFFL_L2__NO2____20191231T144347_20191231T162516_11481_01_010302_20200102T070221"},{"title":"S5P_OFFL_L2__NO2____20191231T043447_20191231T061617_11475_01_010302_20200101T212406"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2020-01-01T00:52:46.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100","available":"2021-02-08T10:47:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:08:00.205Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:38Z"},"links":{"related":[{"length":2259627,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5431118,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:38Z","title":"S5P_L3_NO2_TD_20200101_V100","bandNames":["S5P_L3_NO2_TD_20200101_V100"],"updated":"2023-03-02T10:08:00.205Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:52:46.000Z","endingDateTime":"2020-01-02T00:33:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20200101T174746_20200101T192916_11497_01_010302_20200103T104405"},{"title":"S5P_OFFL_L2__NO2____20200101T225216_20200102T003346_11500_01_010302_20200103T153646"},{"title":"S5P_OFFL_L2__NO2____20200101T073846_20200101T092016_11491_01_010302_20200103T003802"},{"title":"S5P_OFFL_L2__NO2____20200101T192916_20200101T211046_11498_01_010302_20200103T121312"},{"title":"S5P_OFFL_L2__NO2____20200101T211046_20200101T225216_11499_01_010302_20200103T140339"},{"title":"S5P_OFFL_L2__NO2____20200101T142446_20200101T160616_11495_01_010302_20200103T065547"},{"title":"S5P_OFFL_L2__NO2____20200101T160616_20200101T174746_11496_01_010302_20200103T083756"},{"title":"S5P_OFFL_L2__NO2____20200101T110146_20200101T124316_11493_01_010302_20200103T041218"},{"title":"S5P_OFFL_L2__NO2____20200101T092016_20200101T110146_11492_01_010302_20200103T021108"},{"title":"S5P_OFFL_L2__NO2____20200101T055716_20200101T073846_11490_01_010302_20200102T225627"},{"title":"S5P_OFFL_L2__NO2____20200101T124316_20200101T142446_11494_01_010302_20200103T054233"},{"title":"S5P_OFFL_L2__NO2____20200101T041546_20200101T055716_11489_01_010302_20200102T210644"},{"title":"S5P_OFFL_L2__NO2____20200101T005246_20200101T023416_11487_01_010302_20200102T172632"},{"title":"S5P_OFFL_L2__NO2____20200101T023416_20200101T041546_11488_01_010302_20200102T190100"}]}}
        |         }
        |    ]
        |  }""".stripMargin
    val (rasterSources2, metadata2) = _getSentinel5PRasterSources(bbox1, date, zoom, Some(featuresJsonString2))
    val sparseBaseLayer2 = RasterTileLoader.readMultibandTileLayer(rasterSources2, metadata2, Array(polygons2),
      bbox2.crs, sc,
      NoCloudFilterStrategy)
    val defaultBaseLayer2 = RasterTileLoader.readMultibandTileLayer(rasterSources2, metadata2, Array(polygons2),
      bbox2.crs, sc,
      NoCloudFilterStrategy,
      useSparsePartitioner = false)

    // Merge both RDDs.
    val defaultMergedLayer = defaultBaseLayer.merge(defaultBaseLayer2)
    val defaultMergedLayerKeys = defaultMergedLayer.keys.collect().toSet
    val sparseMergedLayer = sparseBaseLayer.merge(sparseBaseLayer2)
    val sparseMergedLayerKeys = sparseMergedLayer.keys.collect().toSet

    assertTrue(defaultMergedLayerKeys.nonEmpty)
    assertEquals(defaultMergedLayerKeys, sparseMergedLayerKeys)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def sparsePartitionerMaskTest(): Unit = {
    // Create the base layers.
    val bbox = ProjectedExtent(Extent(xmin = 55.0, ymin = 30.0, xmax = 60.0, ymax = 35.0), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))
    val polygons = MultiPolygon(bbox.extent.toPolygon())
    val (rasterSources, metadata) = _getSentinel5PRasterSources(bbox, date, 8, Some(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2019-12-31T01:11:47.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20191231_V100","available":"2021-02-08T10:47:24Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:10:39.950Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:24Z"},"links":{"related":[{"length":2218326,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5454742,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2019/12/S5P_OFFL_L3_NO2_TD_20191231_V100/S5P_NO2_TD_20191231_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:24Z","title":"S5P_L3_NO2_TD_20191231_V100","bandNames":["S5P_L3_NO2_TD_20191231_V100"],"updated":"2023-03-02T10:10:39.950Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2019-12-31T01:11:47.000Z","endingDateTime":"2020-01-01T00:52:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20191231T025317_20191231T043447_11474_01_010302_20200101T192226"},{"title":"S5P_OFFL_L2__NO2____20191231T130217_20191231T144347_11480_01_010302_20200102T060402"},{"title":"S5P_OFFL_L2__NO2____20191231T061617_20191231T075747_11476_01_010302_20200101T232513"},{"title":"S5P_OFFL_L2__NO2____20191231T075747_20191231T093917_11477_01_010302_20200102T004148"},{"title":"S5P_OFFL_L2__NO2____20191231T011147_20191231T025317_11473_01_010302_20200101T180133"},{"title":"S5P_OFFL_L2__NO2____20191231T112047_20191231T130217_11479_01_010302_20200102T043947"},{"title":"S5P_OFFL_L2__NO2____20191231T180646_20191231T194816_11483_01_010302_20200102T112434"},{"title":"S5P_OFFL_L2__NO2____20191231T093917_20191231T112047_11478_01_010302_20200102T023550"},{"title":"S5P_OFFL_L2__NO2____20191231T212946_20191231T231116_11485_01_010302_20200102T140733"},{"title":"S5P_OFFL_L2__NO2____20191231T231116_20200101T005246_11486_01_010302_20200102T155014"},{"title":"S5P_OFFL_L2__NO2____20191231T194816_20191231T212946_11484_01_010302_20200102T123941"},{"title":"S5P_OFFL_L2__NO2____20191231T162516_20191231T180646_11482_01_010302_20200102T084547"},{"title":"S5P_OFFL_L2__NO2____20191231T144347_20191231T162516_11481_01_010302_20200102T070221"},{"title":"S5P_OFFL_L2__NO2____20191231T043447_20191231T061617_11475_01_010302_20200101T212406"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100",
        |            "geometry": {"coordinates":[[[-180.0,89.0],[-180.0,-89.0],[180.0,-89.0],[180.0,89.0],[-180.0,89.0]]],"type":"Polygon"},
        |            "bbox": [-180.0,-89.0,180.0,89.0],
        |            "properties":
        |            	{"date":"2020-01-01T00:52:46.000Z","identifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1:S5P_L3_NO2_TD_20200101_V100","available":"2021-02-08T10:47:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","processingDate":"2023-03-02T10:08:00.205Z","processingMode":"OFFL","productType":"NO2_TD","availabilityTime":"2021-02-08T10:47:38Z"},"links":{"related":[{"length":2259627,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_WEIGHT_V100.tif","type":"image/tiff","title":"WEIGHT","bandNames":["WEIGHT"],"category":"QUALITY"}],"data":[{"length":5431118,"href":"file:///data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1/2020/01/S5P_OFFL_L3_NO2_TD_20200101_V100/S5P_NO2_TD_20200101_NO2_V100.tif","conformsTo":"http://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"NO2","bandNames":["NO2"]}],"previews":[],"alternates":[]},"published":"2021-02-08T10:47:38Z","title":"S5P_L3_NO2_TD_20200101_V100","bandNames":["S5P_L3_NO2_TD_20200101_V100"],"updated":"2023-03-02T10:08:00.205Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","beginningDateTime":"2020-01-01T00:52:46.000Z","endingDateTime":"2020-01-02T00:33:46.000Z"},"platform":{"platformShortName":"Sentinel-5P","platformSerialIdentifier":"S5P"}}],"status":"ARCHIVED","additionalAttributes":{"sourceData":[{"title":"S5P_OFFL_L2__NO2____20200101T174746_20200101T192916_11497_01_010302_20200103T104405"},{"title":"S5P_OFFL_L2__NO2____20200101T225216_20200102T003346_11500_01_010302_20200103T153646"},{"title":"S5P_OFFL_L2__NO2____20200101T073846_20200101T092016_11491_01_010302_20200103T003802"},{"title":"S5P_OFFL_L2__NO2____20200101T192916_20200101T211046_11498_01_010302_20200103T121312"},{"title":"S5P_OFFL_L2__NO2____20200101T211046_20200101T225216_11499_01_010302_20200103T140339"},{"title":"S5P_OFFL_L2__NO2____20200101T142446_20200101T160616_11495_01_010302_20200103T065547"},{"title":"S5P_OFFL_L2__NO2____20200101T160616_20200101T174746_11496_01_010302_20200103T083756"},{"title":"S5P_OFFL_L2__NO2____20200101T110146_20200101T124316_11493_01_010302_20200103T041218"},{"title":"S5P_OFFL_L2__NO2____20200101T092016_20200101T110146_11492_01_010302_20200103T021108"},{"title":"S5P_OFFL_L2__NO2____20200101T055716_20200101T073846_11490_01_010302_20200102T225627"},{"title":"S5P_OFFL_L2__NO2____20200101T124316_20200101T142446_11494_01_010302_20200103T054233"},{"title":"S5P_OFFL_L2__NO2____20200101T041546_20200101T055716_11489_01_010302_20200102T210644"},{"title":"S5P_OFFL_L2__NO2____20200101T005246_20200101T023416_11487_01_010302_20200102T172632"},{"title":"S5P_OFFL_L2__NO2____20200101T023416_20200101T041546_11488_01_010302_20200102T190100"}]}}
        |         }
        |    ]
        |  }""".stripMargin))
    val sparseBaseLayer = RasterTileLoader.readMultibandTileLayer(rasterSources, metadata, Array(polygons),
      bbox.crs, sc,
      NoCloudFilterStrategy)
    val defaultBaseLayer = RasterTileLoader.readMultibandTileLayer(rasterSources, metadata, Array(polygons),
      bbox.crs, sc,
      NoCloudFilterStrategy,
      useSparsePartitioner = false)

    // Create the masked layers.
    val maskBbox = ProjectedExtent(Extent(xmin = 57.0, ymin = 30.0, xmax = 58.0, ymax = 35.0), LatLng)
    val maskPolygons = MultiPolygon(maskBbox.extent.toPolygon())
    val defaultMaskedLayer = defaultBaseLayer.mask(maskPolygons)
    val sparseMaskedLayer = sparseBaseLayer.mask(maskPolygons)

    val defaultMaskedLayerKeys = defaultMaskedLayer.keys.collect().toSet
    val sparseMaskedLayerKeys = sparseMaskedLayer.keys.collect().toSet

    assertTrue(defaultMaskedLayerKeys.nonEmpty)
    assertEquals(defaultMaskedLayerKeys, sparseMaskedLayerKeys)
  }

  @Test
  def multipleMultibandAssetsPerFeature(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2024, 4, 7), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(
      20.6,
      45.5,
      20.7,
      45.6
    ), LatLng)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.setLoadPerProduct(true)
    dataCubeParameters.setSyntheticDataOverride(new SyntheticDataOverride("uint16raw", None))

    val client = new FixedFeaturesOpenSearchClient

    val source: BufferedSource = Source.fromResource("org/openeo/geotrellis/multipleMultibandAssetsPerFeature_features.json")
    FeatureCollection.parse(
      source.getLines().mkString("")
    ).features.foreach(feature => client.addFeature(feature))

    val layerProvider = FileLayerProvider(
      client,
      openSearchCollectionId = "multimulti",
      openSearchLinkTitles = NonEmptyList.of("B1", "B2", "B3", "B4"),
      rootPath = "/tmp",
      CellSize(10, 10),
      SplitYearMonthDayPathDateExtractor, layoutScheme = FloatingLayoutScheme(256)
    )

    val layer: MultibandTileLayerRDD[SpaceTimeKey] = layerProvider.readMultibandTileLayer(from = date, to = date, bbox, Array(MultiPolygon(bbox.extent.toPolygon())), bbox.crs, layerProvider.maxZoom, sc, datacubeParams = Some(dataCubeParameters))
    val spatialLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = layer
      .toSpatial(date)
      .cache()
    spatialLayer.writeGeoTiff("/tmp/multipleMultibandAssetsPerFeature.tif", bbox)
  }
}
