package org.openeo.geotrellis

import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.crop.Crop
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{ByteArrayTile, IntArrayTile, MultibandTile, Raster, RasterExtent, ReadingSource, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import io.circe.Json
import geotrellis.vector._
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.operation.union.UnaryUnionOp

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

object VectorizeSpec{

  var sc: SparkContext = _

  @BeforeAll
  def setupSpark() = {
    sc = {
      val conf = new SparkConf().setMaster("local[2]")//.set("spark.driver.bindAddress", "127.0.0.1")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }
  }

  /**
   * Builds a single-band SpatialKey cube (UTM 31N, 10m cells) from a tile, split into tiles of tileSize x tileSize pixels.
   */
  def cubeFromTile(tile: Tile, tileSize: Int): MultibandTileLayerRDD[SpatialKey] = {
    val extent = Extent(500000, 5600000, 500000 + tile.cols * 10, 5600000 + tile.rows * 10)
    val tileLayout = TileLayout(tile.cols / tileSize, tile.rows / tileSize, tileSize, tileSize)
    val layout = LayoutDefinition(extent, tileLayout)
    val tiles = tile.split(tileLayout)
    val keyedTiles = for {
      row <- 0 until tileLayout.layoutRows
      col <- 0 until tileLayout.layoutCols
    } yield (SpatialKey(col, row), MultibandTile(tiles(row * tileLayout.layoutCols + col)): MultibandTile)
    val metadata = TileLayerMetadata(tile.cellType, layout, extent, CRS.fromEpsgCode(32631),
      KeyBounds(SpatialKey(0, 0), SpatialKey(tileLayout.layoutCols - 1, tileLayout.layoutRows - 1)))
    ContextRDD(sc.parallelize(keyedTiles), metadata)
  }

  /**
   * 512x512 raster with several shapes crossing the borders of 256x256 tiles.
   */
  def multiValueTile(): IntArrayTile = {
    val tile = IntArrayTile.fill(1, 512, 512)
    // Square crossing all four tile borders.
    for (c <- 200 until 312; r <- 200 until 312) tile.set(c, r, 2)
    // L-shape crossing both the vertical and the horizontal tile border.
    for (c <- 50 until 350; r <- 400 until 420) tile.set(c, r, 3)
    for (c <- 330 until 350; r <- 100 until 420) tile.set(c, r, 3)
    // Small square inside a single tile.
    for (c <- 10 until 20; r <- 10 until 20) tile.set(c, r, 4)
    // Diagonal line of pixels (only touching at corners) crossing the vertical tile border.
    for (i <- 0 until 32) tile.set(240 + i, 100 + i, 5)
    // Ring crossing the horizontal tile border, with a separate area of value 1 inside.
    for (c <- 400 until 480; r <- 230 until 290) tile.set(c, r, 6)
    for (c <- 420 until 460; r <- 245 until 275) tile.set(c, r, 1)
    tile
  }

  /**
   * 1024x1024 raster: multiValueTile() in the top-left quadrant, plus shapes crossing the 512 and 768 pixel borders,
   * so that every tileSizeFactor (1 to 4) has polygons crossing its tile borders.
   */
  def largeMultiValueTile(): IntArrayTile = {
    val small = multiValueTile()
    val tile = IntArrayTile.fill(1, 1024, 1024)
    for (c <- 0 until small.cols; r <- 0 until small.rows) tile.set(c, r, small.get(c, r))
    // Rectangle crossing the vertical 512 and 768 borders and the horizontal 768 border.
    for (c <- 480 until 800; r <- 600 until 800) tile.set(c, r, 7)
    // Diagonal line of pixels crossing the vertical 768 border.
    for (i <- 0 until 20) tile.set(760 + i, 500 + i, 5)
    // Ring crossing the horizontal 512 border, with a separate area of value 1 inside.
    for (c <- 600 until 700; r <- 470 until 560) tile.set(c, r, 6)
    for (c <- 620 until 680; r <- 490 until 540) tile.set(c, r, 1)
    tile
  }
}

class VectorizeSpec {

  @Test
  def vectorize(): Unit = {
    val largeExtent = Extent(xmin = 3.248235121238894, ymin = 49.9753557675801, xmax = 4.256396825072918, ymax = 50.98003212949561)
    val cube: (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = LayerFixtures.aSpacetimeTileLayerRdd(21,21,3, largeExtent)

    val res = cube._1.metadata.cellSize
    val theExtent = cube._1.metadata.extent
    val newExtent = theExtent.copy(theExtent.xmin, theExtent.ymin, theExtent.xmax - 50 * res.width, theExtent.ymax - 60 * res.height)
    println(newExtent)
    val croppedCube = cube._1.crop(newExtent, Options(force = true, clamp = true))

    GeoTiff(Raster(cube._2,theExtent).crop(newExtent,Crop.Options(force=true)),cube._1.metadata.crs).write("vectorize_reference.tiff")
    val outputPath = Paths.get("polygons.geojson")
    new OpenEOProcesses().vectorize(ContextRDD(croppedCube,croppedCube.metadata.copy(extent = newExtent)),outputPath.toString)

    val json: JsonFeatureCollection = GeoJson.fromFile[JsonFeatureCollection](outputPath.toString)
    val features: Seq[Json] = json.asJson.hcursor.downField("features").focus.flatMap(_.asArray).getOrElse(Vector.empty)
    val featureIds: Seq[String] = features.flatMap { feature => feature.hcursor.downField("id").focus.flatMap(_.asString) }
    val expectedIds: Seq[String] = (0 to 10).map(i => s"20170102_band0_$i") ++ (0 to 10).map(i => s"20170103_band0_$i") ++ (0 to 10).map(i => s"20170104_band0_$i")
    assertEquals(expectedIds, featureIds)
    val polygons = json.getAllPolygons()
    assertEquals(33,polygons.size)

    for (elem <- polygons) {
      assertTrue(newExtent.toPolygon().buffer(0.00000001).covers(elem),f"Polygon $elem not in extent")
    }
  }

  @Test
  def vectorizeGeojsonProperties(): Unit = {
    val cube: (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = LayerFixtures.aSpacetimeTileLayerRdd(10, 10, 1)
    val res = cube._1.metadata.cellSize
    val theExtent = cube._1.metadata.extent
    val newExtent = theExtent.copy(theExtent.xmin, theExtent.ymin, theExtent.xmax - 50 * res.width, theExtent.ymax - 60 * res.height)
    println(newExtent)
    val croppedCube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = cube._1.crop(newExtent, Options(force = true, clamp = true))

    val openEOProcesses = new OpenEOProcesses()
    val (features: Array[(String, Seq[PolygonFeature[Int]])], crs: CRS) = openEOProcesses.vectorize(ContextRDD(croppedCube, croppedCube.metadata.copy(extent = newExtent)))
    val geojson = openEOProcesses.featuresToGeojson(features, crs)

    // assert that geojson["features"][0]["properties"] is a Map
    val hcursor = geojson.hcursor.downField("features").downArray.downField("properties")
    assertTrue(hcursor.focus.exists(_.isObject))

    // assert that geojson["features"][0]["properties"]["value"] == 0
    val value = hcursor.downField("value").focus.flatMap(_.asNumber).flatMap(_.toInt)
    assertEquals(Some(0), value)
  }


  /**
   * Polygons crossing (overlapping) tile borders must be merged into a single feature.
   */
  @Test
  def vectorizeMergesPolygonsOverTileBorders(): Unit = {
    val size = 512
    val tileSize = 256
    val tile = IntArrayTile.fill(1, size, size)
    // Rectangle of value 2 crossing the borders of all four 256x256 tiles.
    for (c <- 200 until 312; r <- 200 until 312) tile.set(c, r, 2)

    val extent = Extent(500000, 5600000, 500000 + size * 10, 5600000 + size * 10)
    val tileLayout = TileLayout(size / tileSize, size / tileSize, tileSize, tileSize)
    val layout = LayoutDefinition(extent, tileLayout)
    val tiles = tile.split(tileLayout)
    val keyedTiles = for {
      row <- 0 until tileLayout.layoutRows
      col <- 0 until tileLayout.layoutCols
    } yield (SpatialKey(col, row), MultibandTile(tiles(row * tileLayout.layoutCols + col)): MultibandTile)
    val metadata = TileLayerMetadata(tile.cellType, layout, extent, CRS.fromEpsgCode(32631),
      KeyBounds(SpatialKey(0, 0), SpatialKey(tileLayout.layoutCols - 1, tileLayout.layoutRows - 1)))
    val cube = ContextRDD(VectorizeSpec.sc.parallelize(keyedTiles), metadata)

    val (features, _) = new OpenEOProcesses().vectorize(cube, tileSizeFactor = 1)
    val allFeatures = features.flatMap(_._2)

    assertTrue(allFeatures.forall(_.geom.isValid), "All polygons should be valid")
    assertEquals(1, allFeatures.count(_.data == 2), "Value 2 should be a single merged polygon")
    assertEquals(1, allFeatures.count(_.data == 1), "Value 1 should be a single merged polygon")
    assertEquals(112 * 112 * 100.0, allFeatures.find(_.data == 2).get.geom.getArea, 1e-3)
    assertEquals(1, allFeatures.find(_.data == 1).get.geom.getNumInteriorRing)
  }

  /**
   * Vectorizing with small (overlapping) tiles must give the same polygons as vectorizing the whole raster at once.
   * On a 1024x1024 raster: factor 1 = 4x4 tiles, 2 = 2x2 tiles, 3 = tiles of 768 that don't divide the raster evenly,
   * 4 = a single tile.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(1, 2, 3, 4))
  def vectorizeSameResultForDifferentTileSizes(tileSizeFactor: Int): Unit = {
    val tile = VectorizeSpec.largeMultiValueTile()
    val cube = VectorizeSpec.cubeFromTile(tile, 256)

    // Reference: GeoTrellis vectorization of the complete raster in one go, without any tiling.
    val reference: Seq[PolygonFeature[Int]] = Raster(tile, cube.metadata.extent).toVector()
    val (features, _) = new OpenEOProcesses().vectorize(cube, tileSizeFactor = tileSizeFactor)
    val tiled: Seq[PolygonFeature[Int]] = features.flatMap(_._2).toSeq

    assertTrue(tiled.forall(_.geom.isValid), "All polygons should be valid")
    val values = reference.map(_.data).distinct.sorted
    assertEquals(values, tiled.map(_.data).distinct.sorted)
    for (value <- values) {
      val expected = reference.filter(_.data == value).map(_.geom)
      val actual = tiled.filter(_.data == value).map(_.geom)
      assertEquals(expected.size, actual.size, s"Number of polygons for value $value")
      assertEquals(expected.map(_.getArea).sum, actual.map(_.getArea).sum, 1e-3, s"Total area for value $value")
      val difference = UnaryUnionOp.union(expected.asJava).symDifference(UnaryUnionOp.union(actual.asJava)).getArea
      assertEquals(0.0, difference, 1e-3, s"Geometry difference for value $value")
    }
  }

  /**
   * Round trip: rasterizing the vectorized polygons must give back the original raster,
   * with every pixel covered by exactly one polygon (no gaps or overlaps at the tile seams).
   */
  @Test
  def vectorizeRoundTrip(): Unit = {
    val tile = VectorizeSpec.multiValueTile()
    val cube = VectorizeSpec.cubeFromTile(tile, 256)
    val (features, _) = new OpenEOProcesses().vectorize(cube, tileSizeFactor = 1)

    val rasterExtent = RasterExtent(cube.metadata.extent, tile.cols, tile.rows)
    val rasterized = IntArrayTile.empty(tile.cols, tile.rows)
    val hits = IntArrayTile.fill(0, tile.cols, tile.rows)
    for (feature <- features.flatMap(_._2)) {
      Rasterizer.foreachCellByGeometry(feature.geom, rasterExtent) { (col, row) =>
        rasterized.set(col, row, feature.data)
        hits.set(col, row, hits.get(col, row) + 1)
      }
    }

    val pixels = for (col <- 0 until tile.cols; row <- 0 until tile.rows) yield (col, row)
    val notCoveredOnce = pixels.filter { case (col, row) => hits.get(col, row) != 1 }
    assertTrue(notCoveredOnce.isEmpty, s"${notCoveredOnce.size} pixels not covered by exactly one polygon, e.g. ${notCoveredOnce.take(5)}")
    val wrongValue = pixels.filter { case (col, row) => rasterized.get(col, row) != tile.get(col, row) }
    assertTrue(wrongValue.isEmpty, s"${wrongValue.size} pixels have a different value, e.g. ${wrongValue.take(5)}")
  }

  private def square(xmin: Double, ymin: Double, xmax: Double, ymax: Double, value: Int): PolygonFeature[Int] =
    Feature(Extent(xmin, ymin, xmax, ymax).toPolygon(), value)

  @Test
  def mergeVectorizedFeaturesMergesEdgeTouchingPolygons(): Unit = {
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(square(0, 0, 1, 1, 1), square(1, 0, 2, 1, 1)))
    assertEquals(1, merged.size)
    assertEquals(2.0, merged.head.geom.getArea, 1e-9)
  }

  @Test
  def mergeVectorizedFeaturesMergesOverlappingPolygons(): Unit = {
    // Same situation as polygons coming from neighbouring tiles with an overlap buffer.
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(square(0, 0, 1.5, 1, 1), square(1, 0, 2, 1, 1)))
    assertEquals(1, merged.size)
    assertEquals(2.0, merged.head.geom.getArea, 1e-9)
  }

  @Test
  def mergeVectorizedFeaturesKeepsDisjointPolygonsSeparate(): Unit = {
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(square(0, 0, 1, 1, 1), square(2, 0, 3, 1, 1)))
    assertEquals(2, merged.size)
  }

  @Test
  def mergeVectorizedFeaturesKeepsCornerTouchingPolygonsSeparate(): Unit = {
    // A valid polygon can't touch itself in a single point, so these stay two separate polygons.
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(square(0, 0, 1, 1, 1), square(1, 1, 2, 2, 1)))
    assertEquals(2, merged.size)
    assertTrue(merged.forall(_.geom.isValid))
  }

  @Test
  def mergeVectorizedFeaturesDoesNotMergeDifferentValues(): Unit = {
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(square(0, 0, 1, 1, 1), square(1, 0, 2, 1, 2)))
    assertEquals(2, merged.size)
    assertEquals(Set(1, 2), merged.map(_.data).toSet)
  }

  @Test
  def mergeVectorizedFeaturesOrdersByPositionThenValue(): Unit = {
    val bottomLeft = square(0, 0, 1, 1, 1)
    val topRight = square(2, 2, 3, 3, 2)
    val topLeft = square(0, 2, 1, 3, 3)
    val topLeftSameEnvelope = square(0, 2, 1, 3, 0)
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(bottomLeft, topRight, topLeft, topLeftSameEnvelope))
    // Top-to-bottom, then left-to-right, then by value.
    assertEquals(Seq(0, 3, 2, 1), merged.map(_.data))
  }

  @Test
  def mergeVectorizedFeaturesReturnsIndexedSeq(): Unit = {
    // A List would be serialized recursively by Kryo, causing a StackOverflowError for many features (issue #312).
    val features = (0 until 100).map(i => square(2 * i, 0, 2 * i + 1, 1, 1))
    val merged = OpenEOProcesses.mergeVectorizedFeatures(features)
    assertTrue(merged.isInstanceOf[IndexedSeq[_]], s"Expected an IndexedSeq but got ${merged.getClass}")
    assertEquals(100, merged.size)
  }

  @Test
  def mergeVectorizedFeaturesFixesInvalidGeometry(): Unit = {
    // Self-intersecting "bowtie" polygon.
    val bowtie = GeomFactory.factory.createPolygon(Array(
      new Coordinate(0, 0), new Coordinate(1, 1), new Coordinate(1, 0), new Coordinate(0, 1), new Coordinate(0, 0)))
    assertTrue(!bowtie.isValid)
    val merged = OpenEOProcesses.mergeVectorizedFeatures(Seq(Feature(bowtie, 1)))
    assertTrue(merged.nonEmpty)
    assertTrue(merged.forall(_.geom.isValid), "All polygons should be valid")
    assertEquals(0.5, merged.map(_.geom.getArea).sum, 1e-9)
  }

  /**
   * Test vectorization of a complex real-world raster resulting in ~3000 features.
   * https://github.com/Open-EO/openeo-geotrellis-extensions/issues/312
   *
   */
  @Test
  def vectorizeOverflow():Unit = {
    val inputFile = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/chl_masked.tif").getPath
    val extent = GeoTiff.readMultiband(inputFile).rasterExtent
    val rsRDD = RasterSourceRDD.read(Seq(ReadingSource(GeoTiffRasterSource(inputFile),1,0)), LayoutDefinition(extent,128))(VectorizeSpec.sc)

    new OpenEOProcesses().vectorize(rsRDD,"/tmp/out.geojson")

  }
}
