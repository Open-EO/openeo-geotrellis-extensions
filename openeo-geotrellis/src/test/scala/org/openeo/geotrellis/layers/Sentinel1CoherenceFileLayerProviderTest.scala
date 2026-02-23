package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.types.MeanValue
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection

import java.net.URL
import java.nio.file.{Files, Paths}
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import scala.reflect.io.Directory

object Sentinel1CoherenceFileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[2]",
    appName = Sentinel1CoherenceFileLayerProviderTest.getClass.getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel1CoherenceFileLayerProviderTest {
  import Sentinel1CoherenceFileLayerProviderTest._

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def polygonalMean(): Unit = {
    val outDir = Paths.get("tmp/Sentinel1CoherenceFileLayerProviderTest/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val layer = coherenceLayerProvider().readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    val tiffPath = outDir + "/polygonalMean.tiff" // Band 1: VH, Band 2: VV
    org.openeo.geotrellis.geotiff.saveRDD(spatialLayer, -1, tiffPath, 6, None)

    val reprojected = polygon.reproject(LatLng, spatialLayer.metadata.crs)

    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(reprojected, MeanVisitor)

    val qgisZonalStatisticsPluginResult = Array(149.25818472185253, 100.46179813143196)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    assertEquals(2, meanList.length)
    assertArrayEquals(qgisZonalStatisticsPluginResult, meanList.map(_.mean), 0.2)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def filterByAttributeValue(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val layer = coherenceLayerProvider(Map("relativeOrbitNumber" -> 161)).readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)

    assertFalse(spatialLayer.isEmpty())
  }

  @Test
  def emptyFilterByAttributeValue(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    try{

      val layer = coherenceLayerProvider(Map("relativeOrbitNumber" -> 99)).readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
      val spatialLayer = layer
        .toSpatial(date)
      fail()
    } catch{
      case e: IllegalArgumentException => return
      case t: Exception => throw t
    }


  }

  private def coherenceLayerProvider(attributeValues: Map[String, Any] = Map()) = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100",
        |            "geometry": {"coordinates":[[[5.6529464,51.4156136],[4.5426663,51.3052781],[4.5570557,51.2471741],[3.3176126,51.1108854],[3.3329141,51.0534713],[2.1484719,50.9100174],[2.3117352,50.4085565],[3.4942511,50.5528967],[3.4787426,50.6105181],[4.6786407,50.7438466],[4.7095982,50.6363709],[5.8174751,50.7481452],[5.6529464,51.4156136]]],"type":"Polygon"},
        |            "bbox": [2.1484719,50.4085565,5.8174751,51.4156136],
        |            "properties":
        |            	{"date":"2020-04-05T17:32:21Z","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100","available":"2020-09-16T08:43:39Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","timeliness":"Fast-24h","processingDate":"2020-06-16T23:12:37.016Z","productType":"COHERENCE","availabilityTime":"2020-09-16T08:43:39Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":41196451,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":40366910,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-05&BBOX=239166.79789165698,6517333.586517985,647598.3658345483,6695142.861943698&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":35902,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-16T08:43:39Z","title":"S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V100","updated":"2020-06-16T23:12:37.016Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":161,"polarisationChannels":"VV, VH","beginningDateTime":"2020-03-30T17:32:47.943Z","orbitDirection":"ASCENDING","endingDateTime":"2020-04-05T17:32:48.906Z","orbitNumber":21012},"platform":{"platformShortName":"Sentinel-1"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[5.6529464,51.4156136],[4.5426663,51.3052781],[4.5570557,51.2471741],[3.3176126,51.1108854],[3.3329141,51.0534713],[2.1484719,50.9100174],[2.3117352,50.4085565],[3.4942511,50.5528967],[3.4787426,50.6105181],[4.6786407,50.7438466],[4.7095982,50.6363709],[5.8174751,50.7481452],[5.6529464,51.4156136]]]},
        |            "bbox": [2.1484719,50.4085565,5.8174751,51.4156136],
        |            "properties":
        |            	{"date":"2020-04-05T17:32:21Z","updated":"2025-04-15T12:10:46.459Z","available":"2025-04-15T12:10:47Z","published":"2025-04-15T12:10:47Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"ASCENDING","orbitNumber":21012,"relativeOrbitNumber":161,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-03-30T17:32:47.943Z","endingDateTime":"2020-04-05T17:32:48.906Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-15T12:10:47Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32631","processingCenter":"VITO","processingDate":"2025-04-15T12:10:46.459Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-05&BBOX=239166.79789165698,6517333.586517985,647598.3658345483,6695142.861943698&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110.xml","type":"application/vnd.iso.19139+xml","length":35906,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110_VH.tif","type":"image/tiff","length":40385837,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110_VV.tif","type":"image/tiff","length":41210347,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100",
        |            "geometry": {"coordinates":[[[5.6491303,51.4241983],[4.5222341,51.3120643],[4.5372362,51.2543146],[3.2965159,51.1176392],[3.3129885,51.0604047],[2.1395065,50.9180892],[2.3017728,50.4162756],[3.4742748,50.559499],[3.4581727,50.61694],[4.6866437,50.7534883],[4.7187198,50.6459941],[5.8145781,50.7565347],[5.6491303,51.4241983]]],"type":"Polygon"},
        |            "bbox": [2.1395065,50.4162756,5.8145781,51.4241983],
        |            "properties":
        |            	{"date":"2020-04-11T17:32:48Z","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100","available":"2020-09-16T08:43:37Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","timeliness":"Fast-24h","processingDate":"2020-06-16T22:25:18.159Z","productType":"COHERENCE","availabilityTime":"2020-09-16T08:43:37Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":41247651,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":40209914,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-11&BBOX=238168.77412889895,6518682.000546814,647275.8732697202,6696675.306003614&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":35906,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-16T08:43:37Z","title":"S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V100","updated":"2020-06-16T22:25:18.159Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":161,"polarisationChannels":"VV, VH","beginningDateTime":"2020-04-05T17:32:21.937Z","orbitDirection":"ASCENDING","endingDateTime":"2020-04-11T17:33:15.302Z","orbitNumber":32083},"platform":{"platformShortName":"Sentinel-1"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[5.6491303,51.4241983],[4.5222341,51.3120643],[4.5334939,51.2538242],[3.2965159,51.1176392],[3.3129885,51.0604047],[2.1395065,50.9180892],[2.3017728,50.4162756],[3.4742748,50.559499],[3.4581727,50.61694],[4.6866437,50.7534883],[4.7187198,50.6459941],[5.8145781,50.7565347],[5.6491303,51.4241983]]]},
        |            "bbox": [2.1395065,50.4162756,5.8145781,51.4241983],
        |            "properties":
        |            	{"date":"2020-04-11T17:32:48Z","updated":"2025-04-16T23:14:07.576Z","available":"2025-04-16T23:14:09Z","published":"2025-04-16T23:14:09Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"ASCENDING","orbitNumber":32083,"relativeOrbitNumber":161,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-04-05T17:32:21.937Z","endingDateTime":"2020-04-11T17:33:15.302Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-16T23:14:09Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32631","processingCenter":"VITO","processingDate":"2025-04-16T23:14:07.576Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-11&BBOX=238168.77412889895,6518682.000546814,647275.8732697202,6696675.306003614&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110.xml","type":"application/vnd.iso.19139+xml","length":35904,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110_VH.tif","type":"image/tiff","length":40246765,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/11/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110/S1B_S1A_Coherence_20200405T173221_20200411T173248_ASC_161_V110_VV.tif","type":"image/tiff","length":41275526,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}]}}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))


    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1",
      openSearchLinkTitles = NonEmptyList.of("VH", "VV"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor,
      attributeValues
      )
  }
}
