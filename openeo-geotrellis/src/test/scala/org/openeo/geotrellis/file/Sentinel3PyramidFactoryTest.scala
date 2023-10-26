package org.openeo.geotrellis.file

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, MultibandTile, Raster, ShortUserDefinedNoDataCellType}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.OscarsClient

import java.net.URL
import java.util
import java.util.stream.{Stream => JStream}

object Sentinel3PyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[Sentinel3PyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()

  def testIntroducingEmptyBandRetainsCellTypeArgumentsProvider: JStream[Arguments] = JStream.of(
    Arguments.of(util.Arrays.asList("vaa", "B0", "NDVI"), java.lang.Boolean.FALSE),
    Arguments.of(util.Arrays.asList("vaa", "B0", "NDVI", "TOA_NDVI"), java.lang.Boolean.TRUE),
  )
}

class Sentinel3PyramidFactoryTest extends RasterMatchers {

  @Disabled("only visual inspection")
  @Test
  def testTOA_NDVI(): Unit = {
    val bands = util.Arrays.asList("B0", "B2", "B3", "MIR", "og", "ag", "saa", "sm", "sza", "vaa", "vza", "wvg", "tg",
      "NDVI", "TOA_NDVI")
    val (raster, crs) = sentinel3Raster(bands)
    MultibandGeoTiff(raster, crs).write(s"/tmp/testTOA_NDVI_${String.join("_", bands)}.tif")
  }

  @Test
  def compareReferenceImage(): Unit = {
    val referenceGeoTiff = MultibandGeoTiff("/data/projects/OpenEO/automated_test_files/Sentinel3_2020-07-01.tif")
    val referenceCrs = referenceGeoTiff.crs
    val referenceRaster = referenceGeoTiff.raster

    val bandMix = util.Arrays.asList("MIR", "TOA_NDVI", "B2", "tg")
    val (actualRaster, actualCrs) = sentinel3Raster(bandMix)

    assertEqual(referenceRaster, actualRaster)
    assertEquals(referenceCrs, actualCrs)
  }

  private def sentinel3Raster(bands: util.List[String]): (Raster[MultibandTile], CRS) = {
    val openSearchClient = new OscarsClient(new URL("https://services.terrascope.be/catalogue"), isUTM = false)

    val date = "2020-07-01T00:00:00Z"

    val pyramidFactory = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S3_SY_2_VG1_V1",
      openSearchLinkTitles = bands,
      rootPath = null,
      maxSpatialResolution = CellSize(0.008928571428571, 0.008928571428571)
    )

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

    (raster, targetCrs)
  }

  @ParameterizedTest
  @MethodSource(Array("testIntroducingEmptyBandRetainsCellTypeArgumentsProvider"))
  def testIntroducingEmptyBandRetainsCellType(bands: util.List[String], resultContainsNoDataBand: Boolean): Unit = {
    val (raster, _) = sentinel3Raster(bands)
    assertEquals(ShortUserDefinedNoDataCellType(-10000), raster.cellType) // B0's cell type, the broadest, is int16 with NODATA -10000
    assertEquals(resultContainsNoDataBand, raster.tile.bands.exists(_.isNoDataTile))
  }
}
