package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.SpatialKey
import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

object S1GrdSigma0FileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]",
    appName = S1GrdSigma0FileLayerProviderTest.getClass.getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class S1GrdSigma0FileLayerProviderTest {
  import S1GrdSigma0FileLayerProviderTest._

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def polygonalMeanMultiband(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(682463.2469290665, 5706687.916789337, 685321.364715595, 5708951.4296454685), CRS.fromEpsgCode(32631))

    val layer = sigma0LayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
    val spatialLayer: MultibandTileLayerRDD[SpatialKey] = layer.toSpatial(date).cache()

    // spatialLayer.writeGeoTiff(bbox, "/tmp/sigma0__.tif")

    val polygon = bbox.reprojectAsPolygon(spatialLayer.metadata.crs)

    val Summary(Array(vhMean, vvMean, angleMean)) = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor)

    // all derived with the QGIS Zonal Statistics plugin
    assertEquals(0.025856676236086, vhMean.mean, 0.001)
    assertEquals(0.0895277254625895, vvMean.mean, 0.001)
    assertEquals(33460.7361532273, angleMean.mean, 1)
  }

  private def sigma0LayerProvider = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110",
        |            "geometry": {"coordinates":[[[2.238402,50.41465],[5.840686,50.817055],[5.470199,52.312359],[1.74933,51.907749],[2.238402,50.41465]]],"type":"Polygon"},
        |            "bbox": [1.74933,50.41465,5.840686,52.312359],
        |            "properties":
        |            	{"date":"2020-04-05T17:32:23.078Z","identifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110","available":"2020-09-09T14:07:46Z","parentIdentifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1","productInformation":{"processingCenter":"VITO","productVersion":"V110","timeliness":"Fast-24h","processingDate":"2020-04-07T07:35:33.562Z","productType":"SIGMA0","availabilityTime":"2020-09-09T14:07:46Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":1640023244,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2020/04/05/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":1642610085,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2020/04/05/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]},{"length":82653980,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2020/04/05/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110_angle.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"angle","bandNames":["angle"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_GRD_SIGMA0&TIME=2020-04-05&BBOX=194734.52482939727,6518398.013707578,650182.1914034018,6856802.220669911&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":38266,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2020/04/05/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110/S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-09T14:07:46Z","title":"S1B_IW_GRDH_SIGMA0_DV_20200405T173223_ASCENDING_161_32CD_V110","updated":"2020-04-07T07:35:33.562Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":161,"polarisationChannels":"VV, VH","beginningDateTime":"2020-04-05T17:32:23.078Z","orbitDirection":"ASCENDING","endingDateTime":"2020-04-05T17:32:48.076Z","orbitNumber":21012},"platform":{"platformShortName":"Sentinel-1","platformSerialIdentifier":"S1B"}}],"status":"ARCHIVED"}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VH", "VV", "angle"),
      rootPath = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )
  }
}
