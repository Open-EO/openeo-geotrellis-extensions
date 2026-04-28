package org.openeo.geotrellis.integrationtests

import geotrellis.layer.SpatialKey
import geotrellis.raster.CellSize
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Geometry
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.file.{FixedFeaturesOpenSearchClient, PyramidFactory}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection

import java.net.URL
import java.util

object MergeCubesTest {
  private var sc: SparkContext = _
  private val openSearchEndpoint = "https://services.terrascope.be/catalogue"

  @BeforeAll
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", classOf[MergeCubesTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class MergeCubesTest {
  import MergeCubesTest._

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testMergeSigma0AscendingAndFapar(): Unit = {
    val vector_file = getClass.getResource("/org/openeo/geotrellis/integrationtests/Field_test.geojson").getFile
    val projected_polygons = ProjectedPolygons.reproject(ProjectedPolygons.fromVectorFile(vector_file), epsg_code = 32631)
    val from_date = "2019-03-07T00:00:00Z"
    val to_date = from_date

    val datacubeParams = new DataCubeParameters()
    datacubeParams.setLoadPerProduct(true)
    datacubeParams.setRetainNoDataTiles(true)
    datacubeParams.layoutScheme="FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(projected_polygons.extent)
    val Seq((_, fapar)) = faparPyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.singletonMap[String, Any]("resolution", "10"),
      "correlationid",
      datacubeParams
    )

    val Seq((_, sigma0Asc)) = sigma0PyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.emptyMap[String, Any](),
      "correlationid",
      datacubeParams
    )


    //global bounds mechanism ensures that keys are aligned
    assertEquals(fapar.metadata.bounds.get.minKey.col, sigma0Asc.metadata.bounds.get.minKey.col)
    assertEquals(fapar.metadata.bounds.get.minKey.row, sigma0Asc.metadata.bounds.get.minKey.row)
    assertEquals(fapar.metadata.bounds.get.maxKey.col, sigma0Asc.metadata.bounds.get.maxKey.col)
    assertEquals(fapar.metadata.bounds.get.maxKey.row, sigma0Asc.metadata.bounds.get.maxKey.row)

    val merged = new OpenEOProcesses().mergeCubes(sigma0Asc, fapar, operator = null)

    //saveRDD(merged.toSpatial(),1,"out.tiff",formatOptions = new GTiffOptions())

    val sigma0Means = meanValues(sigma0Asc.toSpatial, projected_polygons.polygons.head)
    val faparMeans = meanValues(fapar.toSpatial, projected_polygons.polygons.head)
    val mergedMeans = meanValues(merged.toSpatial, projected_polygons.polygons.head)

    assertArrayEquals(sigma0Means ++ faparMeans, mergedMeans, 0.00001)
  }

  private def meanValues(spatialLayer: MultibandTileLayerRDD[SpatialKey], geometry: Geometry): Array[Double] = {
    val Summary(bandMeans) = spatialLayer.polygonalSummaryValue(geometry, MeanVisitor)
    bandMeans.map(_.mean)
  }

  private def sigma0PyramidFactory = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110",
        |            "geometry": {"coordinates":[[[4.449606,49.906914],[8.033141,50.310795],[7.659013,51.805588],[3.95955,51.399448],[4.449606,49.906914]]],"type":"Polygon"},
        |            "bbox": [3.95955,49.906914,8.033141,51.805588],
        |            "properties":
        |            	{"date":"2019-03-07T17:24:35.968Z","identifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110","available":"2020-09-09T14:02:50Z","parentIdentifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1","productInformation":{"processingCenter":"VITO","productVersion":"V110","timeliness":"Fast-24h","processingDate":"2019-03-08T00:36:42Z","productType":"SIGMA0","availabilityTime":"2020-09-09T14:02:50Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":1649573967,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2019/03/07/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":1645498983,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2019/03/07/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]},{"length":116056233,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2019/03/07/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110_angle.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"angle","bandNames":["angle"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_GRD_SIGMA0&TIME=2019-03-07&BBOX=440775.0897705064,6430170.56493712,894245.1655905686,6765049.317120887&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":38267,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2019/03/07/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110/S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2020-09-09T14:02:50Z","title":"S1A_IW_GRDH_SIGMA0_DV_20190307T172435_ASCENDING_88_B705_V110","updated":"2019-03-08T00:36:42Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":88,"polarisationChannels":"VV, VH","beginningDateTime":"2019-03-07T17:24:35.968Z","orbitDirection":"ASCENDING","endingDateTime":"2019-03-07T17:25:00.966Z","orbitNumber":26235},"platform":{"platformShortName":"Sentinel-1","platformSerialIdentifier":"S1A"}}],"status":"ARCHIVED"}
        |         }
        |    ]
        |  }""".stripMargin
    ).features.foreach(feature => client.addFeature(feature))

    new PyramidFactory(
      client,
      openSearchCollectionId = "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = util.Arrays.asList("VH", "VV", "angle"),
      rootPath = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1",
      maxSpatialResolution = CellSize(10, 10)
    )
  }

  private def faparPyramidFactory = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2:S2A_20190307T105021_31UFS_FAPAR_10M_V210",
        |            "geometry": {"type":"Polygon","coordinates":[[[5.9661361,50.6178099],[6.017025,51.4123427],[4.4388768,51.4423523],[4.4087151,50.4552722],[5.8706478,50.4278544],[5.9209417,50.5282026],[5.9661361,50.6178099]]]},
        |            "bbox": [4.4087151,50.4278544,6.017025,51.4423523],
        |            "properties":
        |            	{"date":"2019-03-07T10:50:21.024Z","updated":"2024-08-29T20:56:18.311Z","available":"2024-08-29T20:56:22Z","published":"2024-08-29T20:56:22Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2","title":"S2A_20190307T105021_31UFS_FAPAR_10M_V210","identifier":"urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2:S2A_20190307T105021_31UFS_FAPAR_10M_V210","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2A"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitNumber":19353,"relativeOrbitNumber":51,"beginningDateTime":"2019-03-07T10:50:21.024Z","endingDateTime":"2019-03-07T10:50:21.024Z","tileId":"31UFS"}}],"additionalAttributes":{"resolution":10},"productInformation":{"cloudCover":65.155,"productType":"FAPAR","availabilityTime":"2024-08-29T20:56:22Z","productVersion":"V210","processingCenter":"VITO","processingDate":"2024-08-29T20:56:18.311Z"},"links":{"previews":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/03/07/S2A_20190307T105021_31UFS_FAPAR_V210/10M/S2A_20190307T105021_31UFS_FAPAR_QUICKLOOK_V210.tif","type":"image/tiff","length":172591,"category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_FAPAR&TIME=2019-03-07&BBOX=490775.91998461616,6520705.059840585,669812.159090397,6699916.902160429&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/03/07/S2A_20190307T105021_31UFS_FAPAR_V210/10M/S2A_20190307T105021_31UFS_FAPAR_10M_V210.xml","type":"application/vnd.iso.19139+xml","length":32546,"title":"Inspire metadata"}],"related":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/03/07/S2A_20190307T105021_31UFS_FAPAR_V210/10M/S2A_20190307T105021_31UFS_SCENECLASSIFICATION_20M_V210.tif","type":"image/tiff","length":3564702,"title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"}],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/03/07/S2A_20190307T105021_31UFS_FAPAR_V210/10M/S2A_20190307T105021_31UFS_FAPAR_10M_V210.tif","type":"image/tiff","length":17289629,"title":"FAPAR_10M","bandNames":["FAPAR_10M"]}]}}
        |         }
        |    ]
        |  }""".stripMargin
    ).features.foreach(feature => client.addFeature(feature))

    new PyramidFactory(
      client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = util.Collections.singletonList("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
  }
}
