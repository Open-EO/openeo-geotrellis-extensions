package org.openeo.geotrellis

import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.crop.Crop
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteArrayTile, IntArrayTile, MultibandTile, Raster, ReadingSource, TileLayout}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import io.circe.Json
import geotrellis.vector._
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeAll, Test}

import java.nio.file.Paths

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
}

class VectorizeSpec {
  //TODO better test would be a round-trip: rasterize then vectorize

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
