package org.openeo.geotrellis.integrationtests

import java.util

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
import org.openeo.geotrellis.file.Sentinel2PyramidFactory
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}

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

    val Seq((_, sigma0Asc)) = sigma0PyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.emptyMap[String, Any]()
    )

    val Seq((_, fapar)) = faparPyramidFactory.datacube_seq(
      projected_polygons,
      from_date,
      to_date,
      util.Collections.emptyMap[String, Any]()
    )

    val merged = new OpenEOProcesses().mergeCubes(sigma0Asc, fapar, operator = null)

    val sigma0Means = meanValues(sigma0Asc.toSpatial, projected_polygons.polygons.head)
    val faparMeans = meanValues(fapar.toSpatial, projected_polygons.polygons.head)
    val mergedMeans = meanValues(merged.toSpatial, projected_polygons.polygons.head)

    assertArrayEquals(sigma0Means ++ faparMeans, mergedMeans, 0.00001)
  }

  private def meanValues(spatialLayer: MultibandTileLayerRDD[SpatialKey], geometry: Geometry): Array[Double] = {
    val Summary(bandMeans) = spatialLayer.polygonalSummaryValue(geometry, MeanVisitor)
    bandMeans.map(_.mean)
  }

  private def sigma0PyramidFactory = new Sentinel2PyramidFactory(
    openSearchEndpoint,
    openSearchCollectionId = "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
    openSearchLinkTitles = util.Arrays.asList("VH", "VV", "angle"),
    rootPath = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1",
    maxSpatialResolution = CellSize(10, 10)
  )

  private def faparPyramidFactory = new Sentinel2PyramidFactory(
    openSearchEndpoint,
    openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
    openSearchLinkTitles = util.Collections.singletonList("FAPAR_10M"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
    maxSpatialResolution = CellSize(10, 10)
  )
}
