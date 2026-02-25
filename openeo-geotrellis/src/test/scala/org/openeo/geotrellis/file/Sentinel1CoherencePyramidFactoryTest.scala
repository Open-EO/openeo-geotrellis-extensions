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

    val qgisZonalStatisticsPluginResult = Array(149.25818472185253, 100.09070278555602)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    assertEquals(2, meanList.length)
    assertArrayEquals(qgisZonalStatisticsPluginResult, meanList.map(_.mean), 0.5)
    println(meanList.map(_.count))
  }

  private def sentinel1CoherencePyramidFactory = {
    val openSearchClient = new FixedFeaturesOpenSearchClient

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
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100",
        |            "geometry": {"coordinates":[[[3.9525367,51.5654311],[3.8297204,51.0622599],[4.932282,50.9517404],[4.9500723,51.0093634],[6.1802671,50.8731301],[6.2006568,50.9299637],[7.3815848,50.7864651],[7.5390051,51.2893814],[6.3379801,51.4341801],[6.3185781,51.3769822],[5.0704153,51.5137986],[5.0528444,51.4563722],[3.9525367,51.5654311]]],"type":"Polygon"},
        |            "bbox": [3.8297204,50.7864651,7.5390051,51.5654311],
        |            "properties": 
        |            	{"date":"2020-04-09T05:49:49Z","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100","available":"2020-09-16T08:43:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","timeliness":"Fast-24h","processingDate":"2020-06-09T01:00:34.563Z","productType":"COHERENCE","availabilityTime":"2020-09-16T08:43:38Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":37277574,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":36380655,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-09&BBOX=426322.524808612,6583608.437682343,839238.2088198925,6721928.031826272&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":35894,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-16T08:43:38Z","title":"S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V100","updated":"2020-06-09T01:00:34.563Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":37,"polarisationChannels":"VV, VH","beginningDateTime":"2020-04-03T05:50:14.846Z","orbitDirection":"DESCENDING","endingDateTime":"2020-04-09T05:50:16.67Z","orbitNumber":21063},"platform":{"platformShortName":"Sentinel-1"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[3.9525367,51.5654311],[3.8297204,51.0622599],[4.932282,50.9517404],[4.9500723,51.0093634],[6.1802671,50.8731301],[6.2006568,50.9299637],[7.3815848,50.7864651],[7.5390051,51.2893815],[6.3379801,51.4341801],[6.3185781,51.3769822],[5.0724233,51.5135832],[5.0528444,51.4563722],[3.9525367,51.5654311]]]},
        |            "bbox": [3.8297204,50.7864651,7.5390051,51.5654311],
        |            "properties": 
        |            	{"date":"2020-04-09T05:49:49Z","updated":"2025-04-15T12:05:49.871Z","available":"2025-04-15T12:05:50Z","published":"2025-04-15T12:05:50Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"DESCENDING","orbitNumber":21063,"relativeOrbitNumber":37,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-04-03T05:50:14.846Z","endingDateTime":"2020-04-09T05:50:16.67Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-15T12:05:50Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32631","processingCenter":"VITO","processingDate":"2025-04-15T12:05:49.871Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-09&BBOX=426322.524808612,6583608.437682343,839238.2088198925,6721928.031826272&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110.xml","type":"application/vnd.iso.19139+xml","length":35896,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110_VH.tif","type":"image/tiff","length":36397726,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/09/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110/S1A_S1B_Coherence_20200403T055014_20200409T054949_DSC_37_V110_VV.tif","type":"image/tiff","length":37290780,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100",
        |            "geometry": {"coordinates":[[[7.7250313,51.3732034],[6.6132033,51.2626488],[6.6262265,51.2046339],[5.3900195,51.0684597],[5.3566929,51.1762781],[4.1671034,51.0323083],[4.5448132,49.871965],[5.7167153,50.0160306],[5.7013237,50.0735316],[6.9123197,50.2088638],[6.898564,50.266722],[7.9663117,50.3750205],[7.7250313,51.3732034]]],"type":"Polygon"},
        |            "bbox": [4.1671034,49.871965,7.9663117,51.3732034],
        |            "properties": 
        |            	{"date":"2020-04-06T17:24:41Z","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100","available":"2020-09-16T08:43:39Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","timeliness":"Fast-24h","processingDate":"2020-06-16T23:15:36.641Z","productType":"COHERENCE","availabilityTime":"2020-09-16T08:43:39Z","referenceSystemIdentifier":"EPSG:32632"},"links":{"related":[],"data":[{"length":76993272,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":75043600,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-06&BBOX=463879.828570919,6424131.88908919,886805.7619444976,6687576.489706625&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":35896,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-16T08:43:39Z","title":"S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V100","updated":"2020-06-16T23:15:36.641Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":88,"polarisationChannels":"VV, VH","beginningDateTime":"2020-03-31T17:23:52.637Z","orbitDirection":"ASCENDING","endingDateTime":"2020-04-06T17:25:08.954Z","orbitNumber":32010},"platform":{"platformShortName":"Sentinel-1"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[7.7250313,51.3732034],[6.6132033,51.2626488],[6.6262265,51.2046339],[5.3900195,51.0684597],[5.3566929,51.1762781],[4.1671034,51.0323083],[4.5448132,49.871965],[5.7167153,50.0160306],[5.7013237,50.0735316],[6.9123197,50.2088638],[6.898564,50.266722],[7.9663117,50.3750205],[7.7250313,51.3732034]]]},
        |            "bbox": [4.1671034,49.871965,7.9663117,51.3732034],
        |            "properties": 
        |            	{"date":"2020-04-06T17:24:41Z","updated":"2025-04-17T00:17:33.574Z","available":"2025-04-17T00:17:35Z","published":"2025-04-17T00:17:35Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"ASCENDING","orbitNumber":32010,"relativeOrbitNumber":88,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-03-31T17:23:52.637Z","endingDateTime":"2020-04-06T17:25:08.954Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-17T00:17:35Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32632","processingCenter":"VITO","processingDate":"2025-04-17T00:17:33.574Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-06&BBOX=463879.828570919,6424131.88908919,886805.7619444976,6687576.489706625&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110.xml","type":"application/vnd.iso.19139+xml","length":35898,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110_VH.tif","type":"image/tiff","length":75099530,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/06/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110/S1B_S1A_Coherence_20200331T172352_20200406T172441_ASC_88_V110_VV.tif","type":"image/tiff","length":77037672,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32632"}]}}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100",
        |            "geometry": {"coordinates":[[[2.0418912,52.173565],[1.8368274,51.3405226],[2.9581912,51.2288537],[2.9762073,51.2856888],[4.215277,51.1489916],[4.2336736,51.2061722],[5.4073698,51.064148],[5.6818523,51.896047],[4.4717269,52.0405954],[4.4520935,51.9836523],[3.1890104,52.1206624],[3.1709713,52.0631487],[2.0418912,52.173565]]],"type":"Polygon"},
        |            "bbox": [1.8368274,51.064148,5.6818523,52.173565],
        |            "properties": 
        |            	{"date":"2020-04-08T05:58:23Z","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100","available":"2020-09-16T08:43:38Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","productInformation":{"processingCenter":"VITO","productVersion":"V100","timeliness":"Fast-24h","processingDate":"2020-06-09T01:22:05.837Z","productType":"COHERENCE","availabilityTime":"2020-09-16T08:43:38Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":56740168,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":57838901,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-08&BBOX=204474.69084313262,6632648.614947929,632500.9047985902,6831569.278639721&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":35906,"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-16T08:43:38Z","title":"S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V100","updated":"2020-06-09T01:22:05.837Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":110,"polarisationChannels":"VV, VH","beginningDateTime":"2020-04-02T05:57:52.961Z","orbitDirection":"DESCENDING","endingDateTime":"2020-04-08T05:58:50.855Z","orbitNumber":32032},"platform":{"platformShortName":"Sentinel-1"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110",
        |            "geometry": {"type":"Polygon","coordinates":[[[3.187842,52.1206643],[2.9615799,51.2871237],[4.215277,51.1489916],[4.2336736,51.2061722],[5.4073698,51.064148],[5.6818523,51.896047],[4.4717269,52.0405954],[4.4520935,51.9836523],[3.187842,52.1206643]]]},
        |            "bbox": [2.9615799,51.064148,5.6818523,52.1206643],
        |            "properties": 
        |            	{"date":"2020-04-08T05:58:23Z","updated":"2025-04-16T23:49:26.970Z","available":"2025-04-16T23:49:28Z","published":"2025-04-16T23:49:28Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1","title":"S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110","identifier":"urn:eop:VITO:TERRASCOPE_S1_SLC_COHERENCE_V1:S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-1"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"DESCENDING","orbitNumber":32032,"relativeOrbitNumber":110,"polarisationMode":"D","polarisationChannels":"VV, VH","operationalMode":"IW","beginningDateTime":"2020-04-02T05:57:52.961Z","endingDateTime":"2020-04-08T05:58:50.855Z"}}],"productInformation":{"productType":"COHERENCE","availabilityTime":"2025-04-16T23:49:28Z","productVersion":"V110","timeliness":"Fast-24h","referenceSystemIdentifier":"EPSG:32631","processingCenter":"VITO","processingDate":"2025-04-16T23:49:26.970Z"},"links":{"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_COHERENCE&TIME=2020-04-08&BBOX=329681.56641159405,6632648.614947929,632500.9047985902,6821972.581741962&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110.xml","type":"application/vnd.iso.19139+xml","length":35768,"title":"Inspire metadata"}],"related":[],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110_VH.tif","type":"image/tiff","length":40172660,"title":"VH","bandNames":["VH"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/2020/04/08/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110/S1B_S1A_Coherence_20200402T055752_20200408T055823_DSC_110_V110_VV.tif","type":"image/tiff","length":41079645,"title":"VV","bandNames":["VV"],"conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631"}]}}
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
