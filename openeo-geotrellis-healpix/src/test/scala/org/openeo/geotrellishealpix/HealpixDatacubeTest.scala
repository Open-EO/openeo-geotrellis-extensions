package org.openeo.geotrellishealpix

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

import java.sql.Timestamp
import java.time.Instant
import java.util

@TestInstance(Lifecycle.PER_CLASS)
class HealpixDatacubeTest {

  private var spark: SparkSession = _

  @BeforeAll
  def setup(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("HealpixDatacubeTest")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = if (spark != null) spark.stop()

  /** Build a script: out = data * 10, applied to each band tile (legacy `data` style). */
  private def multiplyByTenScript(): OpenEOProcessScriptBuilder = {
    val builder = new OpenEOProcessScriptBuilder
    val args = new util.HashMap[String, AnyRef]
    args.put("x", "dummy")
    args.put("y", "dummy")
    builder.expressionStart("multiply", args)
    builder.argumentStart("x")
    builder.fromParameter("data")
    builder.argumentEnd()
    builder.constantArgument("y", 10)
    builder.expressionEnd("multiply", args)
    builder
  }

  private val ts = Seq(Timestamp.from(Instant.parse("2024-01-01T00:00:00Z")))

  @Test
  def initScalarAndApply(): Unit = {
    val cube = HealpixDataGenerator.randomScalar(spark, nside = 2, ts, Seq("B1"))
    assertEquals(12L * 2 * 2, cube.df.count())

    val sumBefore = cube.df.selectExpr("sum(B1) as s").collect().head.getDouble(0)

    val applied = cube
      .applyProcess(multiplyByTenScript(), new util.HashMap[String, Any]())
      .asInstanceOf[ScalarHealpixDatacube]

    val sumAfter = applied.df.selectExpr("sum(B1) as s").collect().head.getDouble(0)

    assertEquals(sumBefore * 10.0, sumAfter, 1e-6)
    assertEquals(cube.df.count(), applied.df.count())
  }

  @Test
  def initPackedAndApply(): Unit = {
    val cube = HealpixDataGenerator.fractalPacked(spark, nside = 2, chunkSize = 4, ts, Seq("B1"))
    assertTrue(cube.df.count() > 0)

    def packedSum(c: HealpixDatacube): Double =
      c.df.rdd.map { r =>
        r.getAs[scala.collection.Seq[Any]]("B1")
          .map(v => if (v == null) 0.0 else v.asInstanceOf[Number].doubleValue()).sum
      }.sum()

    val sumBefore = packedSum(cube)
    val applied = cube.applyProcess(multiplyByTenScript(), new util.HashMap[String, Any]())
    assertEquals(cube.df.count(), applied.df.count())
    assertEquals(sumBefore * 10.0, packedSum(applied), 1e-6)
  }

  @Test
  def renderToGeotrellisRdd(): Unit = {
    import geotrellis.layer.LayoutDefinition
    import geotrellis.proj4.LatLng
    import geotrellis.raster.TileLayout
    import geotrellis.vector.Extent
    import org.openeo.geotrellis.geotiff.saveRDDTemporal

    val cube = HealpixDataGenerator.latitudeStripesScalar(spark, nside = 8, ts)

    val extent = Extent(-180, -90, 180, 90)
    val tileLayout = TileLayout(layoutCols = 4, layoutRows = 2, tileCols = 256, tileRows = 256)
    val layout = LayoutDefinition(extent, tileLayout)

    val rdd = cube.toMultibandTileLayerRDD(LatLng, layout, extent)
    val collected = rdd.collect()
    assertEquals(8, collected.length) // 2 spatial keys * 1 timestamp

    collected.foreach { case (_, mb) =>
      assertEquals(1, mb.bandCount)
      val t = mb.band(0)
      assertEquals(256, t.cols)
      assertEquals(256, t.rows)
      // Latitude stripes => top row (north) should be larger than bottom row (south).
      assertTrue(t.getDouble(0, 0) > t.getDouble(0, t.rows - 1))
    }

    val outDir = java.nio.file.Files.createTempDirectory("healpix-render-").toFile
    val written = saveRDDTemporal(rdd, outDir.getAbsolutePath)
    assertFalse(written.isEmpty, "saveRDDTemporal did not produce any GeoTiff")
    written.forEach { case (path, _, _) =>
      assertTrue(new java.io.File(path).length() > 0L, s"GeoTiff not written: $path")
    }
    println(s"Rendered HEALPix GeoTiffs written to: ${outDir.getAbsolutePath}")
  }

  @Test
  def writeZarrV3Store(): Unit = {
    val cube = HealpixDataGenerator.latitudeStripesScalar(spark, nside = 8, ts)
    val outDir = java.nio.file.Files.createTempDirectory("healpix-zarr-").toString

    HealpixZarrWriter.write(cube, outDir, parentLevels = 2)

    // Verify store structure
    val root = java.nio.file.Paths.get(outDir)
    assertTrue(java.nio.file.Files.exists(root.resolve("zarr.json")),
      "Root zarr.json missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/zarr.json")),
      "Level zarr.json missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/cell_id/zarr.json")),
      "cell_id array metadata missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/cell_id/c/0")),
      "cell_id chunk 0 missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/parent_offsets/zarr.json")),
      "parent_offsets metadata missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/bands/lat/zarr.json")),
      "band 'lat' array metadata missing")
    assertTrue(java.nio.file.Files.exists(root.resolve("nside_8/bands/lat/c/0")),
      "band 'lat' chunk 0 missing")

    // Verify root metadata content
    val rootJson = new String(java.nio.file.Files.readAllBytes(root.resolve("zarr.json")))
    assertTrue(rootJson.contains("\"zarr_format\": 3"))
    assertTrue(rootJson.contains("\"base_nside\": 8"))
    assertTrue(rootJson.contains("\"parent_levels\": 2"))
    assertTrue(rootJson.contains("\"lat\""))

    // Verify level metadata
    val levelJson = new String(java.nio.file.Files.readAllBytes(
      root.resolve("nside_8/zarr.json")))
    assertTrue(levelJson.contains("\"nside\": 8"))
    assertTrue(levelJson.contains("\"nside_parent\": 2"))
    // nside=8, nside_parent=2 → children_per_parent = (8/2)^2 = 16
    assertTrue(levelJson.contains("\"children_per_parent\": 16"))
    // n_parents = 12 * 2^2 = 48
    assertTrue(levelJson.contains("\"n_parents\": 48"))

    println(s"HEALPix Zarr v3 store written to: $outDir")
  }
}
