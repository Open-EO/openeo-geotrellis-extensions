package org.openeo.geotrellishealpix

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.openeo.geotrellis.geotiff.saveRDDTemporal

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant

/**
 * Tests for [[Sentinel3BinningReader]], [[HealpixBinner]], and the end-to-end
 * pipeline that reads swath NetCDF data into a [[HealpixDatacube]].
 */
@TestInstance(Lifecycle.PER_CLASS)
class Sentinel3BinningTest {

  private var spark: SparkSession = _

  @BeforeAll
  def setup(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("Sentinel3BinningTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  @AfterAll
  def teardown(): Unit = if (spark != null) spark.stop()

  // ---------- helper: generate a small synthetic NetCDF product ---------------

  private val ts = Timestamp.from(Instant.parse("2024-06-15T10:30:00Z"))

  /**
   * Creates a tiny NetCDF with `nPixels` points distributed along a latitude
   * strip. Band "B1" = sin(lat), band "B2" = cos(lon).
   */
  private def createSyntheticProduct(nPixels: Int): (String, Sentinel3BinningReader.ProductRef) = {
    val tmpFile = Files.createTempFile("s3-binning-test-", ".nc").toString

    val latitudes  = new Array[Float](nPixels)
    val longitudes = new Array[Float](nPixels)
    val b1         = new Array[Float](nPixels)
    val b2         = new Array[Float](nPixels)

    // Distribute pixels along a diagonal from (-30, -60) to (60, 120)
    for (i <- 0 until nPixels) {
      val frac = i.toDouble / (nPixels - 1).max(1)
      latitudes(i)  = (-30.0 + 90.0 * frac).toFloat
      longitudes(i) = (-60.0 + 180.0 * frac).toFloat
      b1(i) = math.sin(math.toRadians(latitudes(i))).toFloat
      b2(i) = math.cos(math.toRadians(longitudes(i))).toFloat
    }

    Sentinel3BinningReader.createSyntheticNetCDF(
      tmpFile, nPixels, latitudes, longitudes,
      Map("B1" -> b1, "B2" -> b2))

    val ref = Sentinel3BinningReader.ProductRef(tmpFile, ts)
    (tmpFile, ref)
  }

  // ---------- tests ----------------------------------------------------------

  @Test
  def readRawProducesCorrectRowCount(): Unit = {
    val nPixels = 100
    val (_, ref) = createSyntheticProduct(nPixels)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1", "B2"))

    val raw = Sentinel3BinningReader.readRaw(spark, Seq(ref), nside = 4, config)

    // All 100 pixels have valid lat/lon, so we expect 100 raw rows
    assertEquals(nPixels.toLong, raw.df.count())
    assertEquals(2, raw.bands.size)
    assertEquals("B1", raw.bands.head._1)
  }

  @Test
  def aggregateMeanReducesDuplicates(): Unit = {
    val nPixels = 200
    val (_, ref) = createSyntheticProduct(nPixels)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1"))

    // NSIDE = 2 -> only 48 cells total, so 200 pixels will have many
    // duplicates per cell.
    val raw = Sentinel3BinningReader.readRaw(spark, Seq(ref), nside = 2, config)
    assertEquals(nPixels.toLong, raw.df.count())

    val aggregated = HealpixBinner.aggregate(raw, HealpixBinner.Aggregation.Mean)
    val aggCount = aggregated.df.count()

    // After aggregation, we should have fewer rows (each cell_id appears once)
    assertTrue(aggCount < nPixels, s"Expected fewer rows after aggregation, got $aggCount")
    assertTrue(aggCount > 0, "Aggregated datacube is empty")

    // The mean of sin(lat) should be somewhere in [-1, 1]
    val meanB1 = aggregated.df.selectExpr("avg(B1)").collect().head.getDouble(0)
    assertTrue(meanB1 >= -1.0 && meanB1 <= 1.0,
      s"Mean of B1 ($meanB1) outside expected range [-1, 1]")
  }

  @Test
  def aggregateCountReturnsPositiveCounts(): Unit = {
    val nPixels = 50
    val (_, ref) = createSyntheticProduct(nPixels)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1"))

    val raw = Sentinel3BinningReader.readRaw(spark, Seq(ref), nside = 2, config)
    val counted = HealpixBinner.aggregate(raw, HealpixBinner.Aggregation.Count)

    // Sum of counts should equal the total number of raw pixels
    val totalCount = counted.df.selectExpr("sum(B1)").collect().head.getLong(0)
    assertEquals(nPixels.toLong, totalCount)
  }

  @Test
  def loadCollectionEndToEnd(): Unit = {
    val nPixels = 150
    val (_, ref) = createSyntheticProduct(nPixels)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1", "B2"))

    val cube = Sentinel3BinningReader.loadCollection(
      spark, Seq(ref), nside = 4, config)

    assertTrue(cube.df.count() > 0, "loadCollection produced empty datacube")
    assertEquals(4, cube.nside)
    assertEquals(2, cube.bands.size)
  }

  @Test
  def loadCollectionRenderToGeoTiff(): Unit = {
    import geotrellis.layer.LayoutDefinition
    import geotrellis.proj4.LatLng
    import geotrellis.raster.TileLayout
    import geotrellis.vector.Extent

    val nPixels = 500
    val (_, ref) = createSyntheticProduct(nPixels)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1"))

    val cube = Sentinel3BinningReader.loadCollection(
      spark, Seq(ref), nside = 8, config)

    // Render to GeoTrellis RDD
    val extent = Extent(-180, -90, 180, 90)
    val tileLayout = TileLayout(
      layoutCols = 2, layoutRows = 1, tileCols = 360, tileRows = 180)
    val layout = LayoutDefinition(extent, tileLayout)

    val rdd = cube.toMultibandTileLayerRDD(LatLng, layout, extent)
    val collected = rdd.collect()
    assertTrue(collected.nonEmpty, "RDD is empty after rendering")

    // Write to GeoTiff
    val outDir = Files.createTempDirectory("s3-binning-geotiff-").toFile
    val written = saveRDDTemporal(rdd, outDir.getAbsolutePath)

    assertFalse(written.isEmpty, "saveRDDTemporal produced no GeoTiff files")
    written.forEach { case (path, _, _) =>
      assertTrue(new java.io.File(path).length() > 0L,
        s"GeoTiff file is empty: $path")
    }
    println(s"Sentinel-3 binned GeoTiffs written to: ${outDir.getAbsolutePath}")
  }

  @Test
  def multipleProductsMerge(): Unit = {
    // Two products with the same timestamp – simulates overlapping orbits
    val (_, ref1) = createSyntheticProduct(80)
    val (_, ref2) = createSyntheticProduct(80)

    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1"))

    val raw = Sentinel3BinningReader.readRaw(
      spark, Seq(ref1, ref2), nside = 4, config)

    // Raw should be the sum of both products' pixels
    assertEquals(160L, raw.df.count())

    // After aggregation with mean, duplicates are merged
    val aggregated = HealpixBinner.aggregate(raw, HealpixBinner.Aggregation.Mean)
    assertTrue(aggregated.df.count() < 160L)
    assertTrue(aggregated.df.count() > 0L)
  }

  @Test
  def toPackedConversion(): Unit = {
    val (_, ref) = createSyntheticProduct(100)
    val config = Sentinel3BinningReader.ProductConfig(
      bandVariables = Seq("B1"))

    val raw = Sentinel3BinningReader.readRaw(spark, Seq(ref), nside = 4, config)
    val aggregated = HealpixBinner.aggregate(raw, HealpixBinner.Aggregation.Mean)
    val packed = HealpixBinner.toPacked(aggregated, childrenPerParent = 16)

    assertTrue(packed.df.count() > 0)
    assertTrue(packed.childrenPerParent == 16)
    // Packed should have fewer rows than scalar (cells are grouped)
    assertTrue(packed.df.count() <= aggregated.df.count())
  }
}

