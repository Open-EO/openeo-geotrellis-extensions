package org.openeo.geotrellis.file

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{CellSize, isData}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import java.time.ZonedDateTime
import java.util.Collections
import scala.collection.JavaConverters._

object LoadStacPyramidFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[LoadStacPyramidFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class LoadStacPyramidFactoryTest {

  @Test
  def testMissingDataInAdjacentTiles(): Unit = {
    // mimics a load_stac from https://stac.openeo.vito.be/collections/tree_cover_density_2018 with assets in EPSG:3035

    val boundingBox = ProjectedExtent(
      Extent(11.1427023295687, 47.22033843316067, 11.821519349155245, 47.628952581107114), LatLng)

    val bandNames = Seq("TCD")

    val topFeature = Feature(
      id = "TCD_2018_010m_E44N27_03035_v020",
      bbox = Extent(11.064548187608006, 47.38783029804821, 12.36948893966052, 48.3083796083107),
      nominalDate = ZonedDateTime.parse("2018-01-01T00:00:00+00:00"),
      links = Array(Link(
        URI.create("file:/data/projects/OpenEO/automated_test_files/load_stac_TCD_2018_010m_E44N27_03035_v020.tif"),
        title = None,
        bandNames = Some(bandNames),
      )),
      resolution = None,
      geometry = Some(GeoJson.parse[Geometry]("""{"type":"Polygon","coordinates":[[[11.046005504476401,47.40858428037738],[11.707867449704809,47.40021736186508],[12.36948893966052,47.38783030409527],[12.390240820693707,47.837566260620925],[12.411462626880093,48.28720072607632],[11.738134164531402,48.29984134090657],[11.064548187608006,48.30837961418922],[11.055172953154765,47.85853023272656],[11.046005504476401,47.40858428037738]]]}""")),
    )

    val bottomFeature = Feature(
      id = "TCD_2018_010m_E44N26_03035_v020",
      bbox = Extent(11.046005504476401, 46.48802651961088, 12.329336972791394, 47.40858427433351),
      nominalDate = ZonedDateTime.parse("2018-01-01T00:00:00+00:00"),
      links = Array(Link(
        URI.create("file:/data/projects/OpenEO/automated_test_files/load_stac_TCD_2018_010m_E44N26_03035_v020.tif"),
        title = None,
        bandNames = Some(bandNames),
      )),
      resolution = None,
      geometry = Some(GeoJson.parse[Geometry]("""{"type":"Polygon","coordinates":[[[11.028268294907988,46.508374655560594],[11.67891503074726,46.50017140047362],[12.329336972791394,46.48802652576836],[12.349192334635823,46.93798601154326],[12.36948893966052,47.38783030409527],[11.707867449704809,47.40021736186508],[11.046005504476401,47.40858428037738],[11.037039355869382,46.958534756699635],[11.028268294907988,46.508374655560594]]]}""")),
    )

    val openSearchClient = new FixedFeaturesOpenSearchClient
    openSearchClient.addFeature(topFeature)
    openSearchClient.addFeature(bottomFeature)

    val pyramidFactory = new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "doesnotmatter",
      openSearchLinkTitles = bandNames.asJava,
      rootPath = "/doesnotmatter",
      maxSpatialResolution = CellSize(10, 10),
      experimental = false,
    )

    val targetCrs = CRS.fromEpsgCode(3035)
    val targetCrsBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)

    val projectedPolygons = ProjectedPolygons(Array(MultiPolygon(targetCrsBoundingBox.extent.toPolygon())), targetCrs)

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
      .crop(targetCrsBoundingBox.extent)
      .cache()

    geotrellis.raster.io.geotiff.GeoTiff(spatialLayer.stitch(), spatialLayer.metadata.crs)
      .write("/tmp/testMissingDataInAdjacentTiles.tif")

    for {
      tile <- spatialLayer.values
      value <- tile
    } assertTrue(isData(value))
  }


  @Disabled("won't run during automated tests")
  @Test
  def rasterSourceFromCorruptTile(): Unit = {
    /* Make sure to set:
     - AWS_HTTPS=TRUE
     - AWS_VIRTUAL_HOSTING=FALSE
     - AWS_S3_ENDPOINT=eodata.cloudferro.com
     - AWS_ACCESS_KEY_ID=...
     - AWS_SECRET_ACCESS_KEY=...
     */
    val rs = GDALRasterSource("/vsis3/EODATA/Sentinel-2/MSI/L2A_N0500/2018/03/27/S2A_MSIL2A_20180327T114351_N0500_R123_T29UMV_20230828T122340.SAFE/GRANULE/L2A_T29UMV_A014420_20180327T114351/IMG_DATA/R10m/T29UMV_20180327T114351_B04_10m.jp2")

    val Some(raster) = rs.read()
    MultibandGeoTiff(raster, rs.crs).write("/tmp/rasterSourceFromCorruptTile.tif")
  }
}
