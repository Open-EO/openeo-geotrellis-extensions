package org.openeo.geotrellis.file

import geotrellis.proj4.LatLng
import geotrellis.proj4.util.UTM
import geotrellis.raster.{CellSize, isData}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import java.time.ZonedDateTime
import java.util.Collections

object LoadStacPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[Sentinel3PyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class LoadStacPyramidFactoryTest {

  @Test
  def testMissingDataInAdjacentTiles(): Unit = {
    val boundingBox = ProjectedExtent(
      Extent(11.1427023295687, 47.22033843316067, 11.821519349155245, 47.628952581107114), LatLng)

    val topFeature = Feature(
      id = "TCD_2018_010m_E44N27_03035_v020",
      bbox = Extent(11.064548187608006, 47.38783029804821, 12.36948893966052, 48.3083796083107),
      nominalDate = ZonedDateTime.parse("2018-01-01T00:00:00+00:00"),
      links = Array(Link(
        URI.create("file:/data/projects/OpenEO/automated_test_files/load_stac_TCD_2018_010m_E44N27_03035_v020.tif"),
        title = None,
        bandNames = Some(Seq("TCD")),
      )),
      resolution = None,
    )

    val bottomFeature = Feature(
      id = "TCD_2018_010m_E44N26_03035_v020",
      bbox = Extent(11.046005504476401, 46.48802651961088, 12.329336972791394, 47.40858427433351),
      nominalDate = ZonedDateTime.parse("2018-01-01T00:00:00+00:00"),
      links = Array(Link(
        URI.create("file:/data/projects/OpenEO/automated_test_files/load_stac_TCD_2018_010m_E44N26_03035_v020.tif"),
        title = None,
        bandNames = Some(Seq("TCD")),
      )),
      resolution = None,
    )

    val openSearchClient = new FixedFeaturesOpenSearchClient
    openSearchClient.addFeature(topFeature)
    openSearchClient.addFeature(bottomFeature)

    val bandNames = Collections.singletonList("TCD")

    val pyramidFactory = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "doesnotmatter",
      openSearchLinkTitles = bandNames,
      rootPath = "/doesnotmatter",
      maxSpatialResolution = CellSize(10, 10),
      experimental = false,
    )

    val utmCrs = UTM.getZoneCrs(lon = boundingBox.extent.center.getX, lat = boundingBox.extent.center.getY)
    val utmBoundingBox = ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)

    val projectedPolygons = ProjectedPolygons(Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmCrs)

    val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(
      projectedPolygons,
      from_date = "1970-01-01T00:00:00Z",
      to_date = "2070-01-01T00:00:00Z",
      metadata_properties = Collections.emptyMap(),
      correlationId = "doesnotmatter",
      new DataCubeParameters,
    )

    val spatialLayer = baseLayer
      .withContext(_.mapValues(_.band(0)))
      .toSpatial()
      .crop(utmBoundingBox.extent)
      .cache()

    geotrellis.raster.io.geotiff.GeoTiff(spatialLayer.stitch(), spatialLayer.metadata.crs)
      .write("/tmp/testMissingDataInAdjacentTiles.tif")

    for {
      tile <- spatialLayer.values
      value <- tile
    } assertTrue(isData(value))
  }
}
