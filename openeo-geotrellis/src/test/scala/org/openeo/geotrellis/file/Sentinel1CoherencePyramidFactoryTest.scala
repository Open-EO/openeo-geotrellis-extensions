package org.openeo.geotrellis.file

import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.PolygonalSummaryResult
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.types.MeanValue
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection

import java.net.URL
import java.nio.file.{Files, Paths}
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Arrays.asList
import scala.reflect.io.Directory

object Sentinel1CoherencePyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel1CoherencePyramidFactoryTest {

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def polygonalMean(): Unit = {
    val outDir = Paths.get("tmp/Sentinel1CoherencePyramidFactoryTest/")
    new Directory(outDir.toFile).deepFiles.foreach(_.delete())
    Files.createDirectories(outDir)

    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygon = Polygon((5.333628277543832, 51.125675727017786), (5.275056319942021, 51.120766442610417), (5.271964011621427, 51.148274537190268), (5.329808367265453, 51.150784833330405), (5.333628277543832, 51.125675727017786))
    val bbox = ProjectedExtent(polygon.extent, LatLng)

    val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
    val to_date = from_date

    val (_, baseLayer) = sentinel1CoherencePyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date, correlationId = "")
      .maxBy { case (zoom, _) => zoom }

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    val tiffPath = outDir + "/polygonalMean.tiff" // Band 1: VH, Band 2: VV
    org.openeo.geotrellis.geotiff.saveRDD(spatialLayer, -1, tiffPath, 6, None)

    val reprojected = polygon.reproject(LatLng, spatialLayer.metadata.crs)

    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(reprojected, MeanVisitor)

    val qgisZonalStatisticsPluginResult = Array(100.09070278555602, 149.25818472185253)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    assertEquals(2, meanList.length)
    assertArrayEquals(qgisZonalStatisticsPluginResult, meanList.map(_.mean), 0.2)
    println(meanList.map(_.count))
  }

  private def sentinel1CoherencePyramidFactory = {
    val openSearchClient = new FixedFeaturesOpenSearchClient

    FeatureCollection.parse(
      """{   
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[5.6529464,51.4156136],[4.5426663,51.3052781],[4.5570557,51.2471741],[3.3176126,51.1108854],[3.3329141,51.0534713],[2.1484719,50.9100174],[2.3117352,50.4085565],[3.4942511,50.5528967],[3.4787426,50.6105181],[4.6786407,50.7438466],[4.7095982,50.6363709],[5.8174751,50.7481452],[5.6529464,51.4156136]]]},
        |            "bbox": [2.1484719,50.4085565,5.8174751,51.4156136],
        |            "properties":
        |            	{"date":"2020-04-05T17:32:21Z","updated":"2025-04-15T12:10:46.459Z","available":"2025-04-15T12:10:47Z","published":"2025-04-15T12:10:47Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"ASCENDING","orbitNumber":21012,"relativeOrbitNumber":161,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-03-30T17:32:47.943Z","endingDateTime":"2020-04-05T17:32:48.906Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-15T12:10:47Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32631","processingCenter":"VITO","processingDate":"2025-04-15T12:10:46.459Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-05&BBOX=239166.79789165698,6517333.586517985,647598.3658345483,6695142.861943698&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110.xml","type":"application/vnd.iso.19139+xml","length":35906,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110_VH.tif","type":"image/tiff","length":40385837,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/05/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110/S1A_S1B_Coherence_20200330T173247_20200405T173221_ASC_161_V110_VV.tif","type":"image/tiff","length":41210347,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}]}}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => openSearchClient.addFeature(feature))


    new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1",
      openSearchLinkTitles = asList("VH", "VV"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE",
      maxSpatialResolution = CellSize(10, 10)
    )
  }
}
