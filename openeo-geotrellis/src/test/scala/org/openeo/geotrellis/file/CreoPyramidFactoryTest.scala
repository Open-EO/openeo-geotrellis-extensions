package org.openeo.geotrellis.file

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.CreodiasClient

import java.nio.file.Paths
import java.util

object CreoPyramidFactoryTest {
  // see creo_layercatalog.json
  private final val B04 = "IMG_DATA_Band_B04_10m_Tile1_Data"
  private final val B03 = "IMG_DATA_Band_B03_10m_Tile1_Data"
  private final val B02 = "IMG_DATA_Band_B02_10m_Tile1_Data"
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
class CreoPyramidFactoryTest extends RasterMatchers {
  import CreoPyramidFactoryTest._

  // TODO: these are to become the first alias in creo_layercatalog.json
  private val allLandsat8OpenSearchLinkTitles = util.Arrays.asList(
    "SR_B1",
    "SR_B2",
    "SR_B3",
    "SR_B4",
    "SR_B5",
    "SR_B6",
    "SR_B7",
    "ST_B10",
    "QA_PIXEL",
    "QA_RADSAT",
    "SR_QA_AEROSOL",
    "ST_QA",
    "ST_TRAD",
    "ST_URAD",
    "ST_DRAD",
    "ST_ATRAN",
    "ST_EMIS",
    "ST_EMSD",
    "ST_CDIST",
  )

  @Disabled("visual inspection only")
  @Test
  def testSentinel2L2a(): Unit = {
    val openSearchLinkTitlesSets = Seq(
      util.Arrays.asList(
        Saa,
        //B04,
        //B03,
        //B02,
        Vaa,
        //Vza,
      ),
      util.Collections.singletonList(Saa),
      util.Collections.singletonList(Vaa),
      util.Collections.singletonList(Vza),
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
  def compareS2L2aReferenceImage(): Unit = {
    val (referenceRaster, referenceCrs) = this.referenceRaster("creo_S2L2A_2023-09-24.tif")

    val bandMix = util.Arrays.asList(Saa, Vaa, B03, Vza)
    // expected SAA: 165.931952115363 everywhere
    // expected VAA: 107.973307847137 everywhere
    // expected B03: every value is 1000 less than the value in the jp2 asset
    // expected VZA: 6.85674497180878 everywhere
    val (actualRaster, actualCrs) = sentinel2L2aRaster(bandMix)

    assertEqual(referenceRaster, actualRaster)
    assertEquals(referenceCrs, actualCrs)
  }

  private def referenceRaster(name: String): (Raster[MultibandTile], CRS) = {
    // TODO: get it from Artifactory instead?
    val referenceGeoTiff = MultibandGeoTiff(s"/data/projects/OpenEO/automated_test_files/$name")
    (referenceGeoTiff.raster, referenceGeoTiff.crs)
  }

  @Disabled("visual inspection only")
  @Test
  def testLandsat8(): Unit = {
    val (raster, crs) = this.landsat8l2Raster(allLandsat8OpenSearchLinkTitles)

    val fileName = s"testLandsat8_${String.join("_", allLandsat8OpenSearchLinkTitles)}.tif"
    val outputFile = Paths.get("/tmp").resolve(fileName)
    MultibandGeoTiff(raster, crs).write(outputFile.toString)
  }

  @Test
  def compareLandsat8ReferenceImage(): Unit = {
    val (referenceRaster, referenceCrs) = this.referenceRaster("creo_Landsat8L2_2022-01-17.tif")

    val (actualRaster, actualCrs) = landsat8l2Raster(allLandsat8OpenSearchLinkTitles)

    assertEqual(referenceRaster, actualRaster)
    assertEquals(referenceCrs, actualCrs)
  }

  private def landsat8l2Raster(openSearchLinkTitles: util.List[String]): (Raster[MultibandTile], CRS) = {
    val boundingBox = ProjectedExtent(Extent(4.123803680535843, 51.38393982450626, 4.21525120682341, 51.44770087550853), LatLng)
    println(boundingBox.extent.toGeoJson())

    val date = "2022-01-17T00:00:00Z"

    val pyramidFactory = new PyramidFactory(
      openSearchClient = new CreodiasClient,
      openSearchCollectionId = "Landsat8",
      openSearchLinkTitles,
      rootPath = "/eodata",
      maxSpatialResolution = CellSize(30, 30),
    )

    val projectedPolygons = ProjectedPolygons.fromExtent(boundingBox.extent, s"EPSG:${boundingBox.crs.epsgCode.get}")
    val projectedPolygonsNativeCrs = ProjectedPolygons.reproject(projectedPolygons, 32631)
    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"

    val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(
      projectedPolygonsNativeCrs,
      from_date = date, to_date = date,
      metadata_properties = util.Collections.singletonMap("productType", "L2SP"),
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
}
