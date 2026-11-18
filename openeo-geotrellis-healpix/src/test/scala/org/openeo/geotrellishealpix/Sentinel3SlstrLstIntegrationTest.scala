package org.openeo.geotrellishealpix

import geotrellis.vector.Extent
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.geotiff.saveRDDTemporal
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.STACClient

import java.net.URI
import java.nio.file.Files
import java.util.Collections

/**
 * Integration test that queries 5 Sentinel-3 SL-2-LST products from the
 * Copernicus Data Space STAC catalogue, reads them from S3 via the
 * netcdf-java `cdm-s3` module, bins into HEALPix, and renders to
 * GeoTrellis RDD / GeoTiff.
 *
 * Requires network access to:
 *  - `stac.dataspace.copernicus.eu` (catalogue)
 *  - `eodata.dataspace.copernicus.eu` (S3 object storage)
 *
 * Disabled by default; set `RUN_INTEGRATION_TESTS=true` to enable.
 *
 * Sentinel-3 SLSTR Level-2 LST product layout (.SEN3 directory):
 *  - `LST_in.nc` : variable `LST` (Land Surface Temperature in K)
 *  - `geodetic_in.nc` : variables `latitude_in`, `longitude_in`
 */
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
@TestInstance(Lifecycle.PER_CLASS)
class Sentinel3SlstrLstIntegrationTest {

  private var spark: SparkSession = _

  @BeforeAll
  def setup(): Unit = {
    spark = SparkSession.builder()
      .master("local[3]")
      .appName("Sentinel3SlstrLstIntegrationTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = if (spark != null) spark.stop()

  @Test
  def loadSentinel3LstViaDacubeSeq(): Unit = {
    // STACClient is an OpenSearchClient — use it directly with datacube_seq
    val stacClient = new STACClient(
      new URI("https://stac.dataspace.copernicus.eu/v1").toURL,
      s3URLS = true)

    val collectionId = "sentinel-3-sl-2-lst-ntc"
    val bbox = Extent(3.0, 50.0, 7.0, 52.0)
    val polygons = ProjectedPolygons.fromExtent(bbox, "EPSG:4326")

    val config = Sentinel3BinningReader.ProductConfig(
      latVariable = "latitude_in",
      lonVariable = "longitude_in",
      assetVariables = Map("LST_in" -> Seq("LST")),
      geoFileSuffix = Some("geodetic_in.nc")
    )

    val cube = Sentinel3BinningReader.datacube_seq(
      openSearchClient = stacClient,
      openSearchCollectionId = collectionId,
      polygons = polygons,
      from_date = "2024-06-01T00:00:00Z",
      to_date = "2024-06-10T00:00:00Z",
      metadata_properties = Collections.emptyMap(),
      correlationId = "s3-lst-inttest",
      dataCubeParameters = new DataCubeParameters(),
      nside = 256,
      config = config
    )

    val cellCount = cube.df.count()
    println(s"HEALPix datacube: $cellCount cells, nside=${cube.nside}")
    assertTrue(cellCount > 0, "HEALPix datacube is empty")

    cube.df.select("cell_id", "LST").show(10, truncate = false)


    val rdd = cube.resampleSpatial(3857, 500.0)
    val collected = rdd.collect()
    println(s"GeoTrellis RDD: ${collected.length} tiles")
    assertTrue(collected.nonEmpty, "Rendered RDD is empty")

    // Write GeoTiff
    val outDir = Files.createTempDirectory("s3-lst-integration-").toFile
    val written = saveRDDTemporal(rdd, outDir.getAbsolutePath)
    assertFalse(written.isEmpty, "No GeoTiff files written")
    written.forEach { case (path, _, _) =>
      println(s"  Written: $path (${new java.io.File(path).length()} bytes)")
    }
    println(s"Output: ${outDir.getAbsolutePath}")
  }
}

