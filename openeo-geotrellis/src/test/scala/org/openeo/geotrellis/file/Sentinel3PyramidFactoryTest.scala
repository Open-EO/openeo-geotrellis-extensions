package org.openeo.geotrellis.file

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.junit.jupiter.api._
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.OscarsClient

import java.net.URL
import java.util

object Sentinel3PyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[Sentinel3PyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel3PyramidFactoryTest {

  @Test
  def testTOA_NDVI(): Unit = {
    val includeAllBands = true

    val openSearchClient = new OscarsClient(new URL("https://services.terrascope.be/catalogue"), isUTM = false)

    val date = "2020-07-01T00:00:00Z"
    val bands =
      if (includeAllBands) util.Arrays.asList("B0", "B2", "B3", "MIR", "og", "ag", "saa", "sm", "sza", "vaa", "vza", "wvg", "tg", "NDVI", "TOA_NDVI")
      else util.Arrays.asList("NDVI")

    val pyramidFactory = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S3_SY_2_VG1_V1",
      openSearchLinkTitles = bands,
      rootPath = null,
      maxSpatialResolution = CellSize(0.008928571428571, 0.008928571428571)
    )

    //val requestedExtent = Extent(30.8074028236180268, 29.8337715247058242, 31.3766799320516760, 30.2494341753081706)
    val requestedExtent = Extent(9.603818023478027, 57.26817706195379, 10.732128254309856, 57.80985456431523)
    val targetCrs = LatLng

    val projectedPolygons = ProjectedPolygons.reproject(ProjectedPolygons.fromExtent(requestedExtent, "EPSG:4326"),
      targetCrs)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"

    val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(projectedPolygons, from_date = date, to_date = date,
      metadata_properties = util.Collections.emptyMap(), correlationId = "", dataCubeParameters)

    val raster = baseLayer
      .toSpatial()
      .stitch()
      .crop(requestedExtent)

    MultibandGeoTiff(raster, targetCrs).write(s"/tmp/testTOA_NDVI_${String.join("_", bands)}.tif")
  }
}
