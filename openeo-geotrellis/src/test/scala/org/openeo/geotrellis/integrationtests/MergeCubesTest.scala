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
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.file.PyramidFactory
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.util

object MergeCubesTest {
  private var sc: SparkContext = _
  private val openSearchEndpoint = "https://services.terrascope.be/catalogue"

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", classOf[MergeCubesTest].getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class MergeCubesTest {
  import MergeCubesTest._

  @Test
  def testMergeSigma0AscendingAndFapar(): Unit = {
    val vector_file = getClass.getResource("/org/openeo/geotrellis/integrationtests/Field_test.geojson").getFile
    val projected_polygons = ProjectedPolygons.reproject(ProjectedPolygons.fromVectorFile(vector_file), epsg_code = 32631)
    val from_date = "2019-03-07T00:00:00Z"
    val to_date = from_date

    val datacubeParams = new DataCubeParameters()
    datacubeParams.layoutScheme="FloatingLayoutScheme"
    datacubeParams.globalExtent = Some(projected_polygons.extent)
    val Seq((_, sigma0Asc)) = sigma0PyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.emptyMap[String, Any](),
      "correlationid",
      datacubeParams
    )

    val Seq((_, fapar)) = faparPyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.singletonMap[String, Any]("resolution", "10"),
      "correlationid",
      datacubeParams
    )

    //global bounds mechanism ensures that keys are aligned
    assertEquals(0, fapar.metadata.bounds.get.minKey.col)
    assertEquals(0, fapar.metadata.bounds.get.minKey.row)
    assertEquals(0, sigma0Asc.metadata.bounds.get.maxKey.col)
    assertEquals(0, sigma0Asc.metadata.bounds.get.maxKey.row)

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
    val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = true)
    new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = util.Arrays.asList("VH", "VV", "angle"),
      rootPath = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1",
      maxSpatialResolution = CellSize(10, 10)
    )
  }

  private def faparPyramidFactory = {
    val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = true)
    new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = util.Collections.singletonList("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
  }
}
