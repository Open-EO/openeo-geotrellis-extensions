package org.openeo.geotrellis

import io.circe.Json
import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.crop.Crop
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{ByteArrayTile, MultibandTile, Raster}

import java.nio.file.Paths
import geotrellis.spark.util.SparkUtils
import geotrellis.spark._
import geotrellis.vector._
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.{BeforeClass, Test}

object VectorizeSpec{

  var sc: SparkContext = _

  @BeforeClass
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
    //val layer = LayerFixtures.ClearNDVILayerForSingleDate()(VectorizeSpec.sc)

    val cube: (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = LayerFixtures.aSpacetimeTileLayerRdd(10,10,1)

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
    val expectedIds: Seq[String] = (0 to 10).map(i => s"20170102_band0_$i")
    assertEquals(expectedIds, featureIds)
    val polygons = json.getAllPolygons()
    assertEquals(11,polygons.size)

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
    val (features: Array[(String, List[PolygonFeature[Int]])], crs: CRS) = openEOProcesses.vectorize(ContextRDD(croppedCube, croppedCube.metadata.copy(extent = newExtent)))
    val geojson = openEOProcesses.featuresToGeojson(features, crs)

    // assert that geojson["features"][0]["properties"] is a Map
    val hcursor = geojson.hcursor.downField("features").downArray.downField("properties")
    assertTrue(hcursor.focus.exists(_.isObject))

    // assert that geojson["features"][0]["properties"]["value"] == 0
    val value = hcursor.downField("value").focus.flatMap(_.asNumber).flatMap(_.toInt)
    assertEquals(Some(0), value)
  }
}
