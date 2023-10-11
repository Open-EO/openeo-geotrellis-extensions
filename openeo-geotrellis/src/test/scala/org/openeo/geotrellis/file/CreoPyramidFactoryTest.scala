package org.openeo.geotrellis.file

import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.CreodiasClient

import java.nio.file.{Path, Paths}
import java.util

object CreoPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[CreoPyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class CreoPyramidFactoryTest {

  @Disabled("requires some environment variables (see https://github.com/Open-EO/openeo-opensearch-client/issues/25")
  @Test
  def testSentinel2L2a(): Unit = {
    val boundingBox = ProjectedExtent(
      Extent(4.912844218500582, 51.02816932187383, 4.918160603369832, 51.029815337603594), LatLng)

    val date = "2023-09-24T00:00:00Z"

    val openSearchClient = new CreodiasClient

    // see creo_layercatalog.json
    val openSearchLinkTitlesSets = Seq(
      util.Arrays.asList(
        "S2_Level-2A_Tile1_Metadata##0",
        //"IMG_DATA_Band_B04_10m_Tile1_Data",
        //"IMG_DATA_Band_B03_10m_Tile1_Data",
        //"IMG_DATA_Band_B02_10m_Tile1_Data",
        "S2_Level-2A_Tile1_Metadata##2",
        //"S2_Level-2A_Tile1_Metadata##3",
      ),
      util.Collections.singletonList("S2_Level-2A_Tile1_Metadata##0"),
      util.Collections.singletonList("S2_Level-2A_Tile1_Metadata##2"),
      util.Collections.singletonList("S2_Level-2A_Tile1_Metadata##3"),
    )

    def writeGeoTiff(openSearchLinkTitles: util.List[String], outputFile: Path): Unit = {
      val pyramidFactory = new PyramidFactory(
        openSearchClient,
        openSearchCollectionId = "Sentinel2",
        openSearchLinkTitles,
        rootPath = "/eodata",
        maxSpatialResolution = CellSize(10, 10),
      )

      val projectedPolygons = ProjectedPolygons.fromExtent(boundingBox.extent, s"EPSG:${boundingBox.crs.epsgCode.get}")
      val projectedPolygonsNativeCrs = ProjectedPolygons.reproject(projectedPolygons, 32631)
      val dataCubeParameters = new DataCubeParameters
      dataCubeParameters.layoutScheme = "FloatingLayoutScheme"

      val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(
        projectedPolygonsNativeCrs,
        from_date = date, to_date = date,
        metadata_properties = util.Collections.singletonMap("productType", "L2A"),
        correlationId = "",
        dataCubeParameters,
      )

      val crs = baseLayer.metadata.crs

      val raster = baseLayer
        .toSpatial()
        .stitch()
        .crop(boundingBox.reproject(crs))

      MultibandGeoTiff(raster, crs).write(outputFile.toString)
    }

    for (openSearchLinkTitles <- openSearchLinkTitlesSets.slice(0, 1)) {
      val fileName = s"testSentinel2L2a_${String.join("_", openSearchLinkTitles)}.tif"
      // TODO: visual inspection only
      writeGeoTiff(openSearchLinkTitles, Paths.get("/tmp").resolve(fileName))
    }
  }
}
