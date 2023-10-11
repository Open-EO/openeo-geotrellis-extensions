package org.openeo.geotrellis.file

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{ArrayMultibandTile, CellSize, MultibandTile, Raster}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.CreodiasClient

import java.nio.file.{Path, Paths}
import java.util

object CreoPyramidFactoryTest {
  private final val B03 = "IMG_DATA_Band_B03_10m_Tile1_Data"
  private final val Saa = "S2_Level-2A_Tile1_Metadata##0"
  private final val Sza = "S2_Level-2A_Tile1_Metadata##1"
  private final val Vaa = "S2_Level-2A_Tile1_Metadata##2"
  private final val Vza = "S2_Level-2A_Tile1_Metadata##3"

  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[CreoPyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

@Disabled("requires some environment variables (see https://github.com/Open-EO/openeo-opensearch-client/issues/25")
class CreoPyramidFactoryTest {
  import CreoPyramidFactoryTest._

  @Test
  def testSentinel2L2a(): Unit = {
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

    for (openSearchLinkTitles <- openSearchLinkTitlesSets.slice(0, 1)) {
      val fileName = s"testSentinel2L2a_${String.join("_", openSearchLinkTitles)}.tif"
      val outputFile = Paths.get("/tmp").resolve(fileName)

      val (raster, crs) = sentinel2L2aRaster(openSearchLinkTitles)
      MultibandGeoTiff(raster, crs).write(outputFile.toString)
    }
  }

  private def sentinel2L2aRaster(openSearchLinkTitles: util.List[String]): (Raster[MultibandTile], CRS) = {
    val boundingBox = ProjectedExtent(
      Extent(4.912844218500582, 51.02816932187383, 4.918160603369832, 51.029815337603594), LatLng)

    val date = "2023-09-24T00:00:00Z"

    val pyramidFactory = new PyramidFactory(
      openSearchClient = new CreodiasClient,
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

    (raster, crs)
  }

  @Test
  def testResultReflectsBandsOrder(): Unit = {
    val (saaRaster, _) = sentinel2L2aRaster(openSearchLinkTitles = util.Collections.singletonList(Saa))
    val (vaaRaster, _) = sentinel2L2aRaster(openSearchLinkTitles = util.Collections.singletonList(Vaa))
    val (combination, _) = sentinel2L2aRaster(openSearchLinkTitles = util.Arrays.asList(Saa, Vaa))

    assertEquals(saaRaster.tile.band(0), combination.tile.band(0))
    assertEquals(vaaRaster.tile.band(0), combination.tile.band(1))
  }

  @Test
  def compareS2L2aReferenceImage(@TempDir tempDir: Path): Unit = {
    val bandMix = util.Arrays.asList(Saa, Vaa, B03, Vza)
    // expected SAA: 165.931952115363
    // expected VAA: 107.973307847137
    // TODO: B03 seems right but every pixel has an offset of -1000 wrt/ the source asset, is this expected?
    // expected VZA: 6.85674497180878

    val (actualRaster, actualCrs) = sentinel2L2aRaster(bandMix)
    val outputFile = tempDir.resolve("actual.tif")
    MultibandGeoTiff(actualRaster, actualCrs).write(outputFile.toString)
    val actualGeoTiff = MultibandGeoTiff(outputFile.toString)

    val (referenceRaster, referenceCrs) = this.referenceRaster("creo_S2L2A_2023-09-24.tif")

    assertEquals(referenceRaster, actualGeoTiff.raster.mapTile(_.toArrayTile()))
    assertEquals(referenceCrs, actualGeoTiff.crs)
  }

  private def referenceRaster(name: String): (Raster[ArrayMultibandTile], CRS) = {
    // TODO: get it from Artifactory instead?
    val referenceGeoTiff = MultibandGeoTiff(s"/data/projects/OpenEO/automated_test_files/$name")
    (referenceGeoTiff.raster.mapTile(_.toArrayTile()), referenceGeoTiff.crs)
  }
}
