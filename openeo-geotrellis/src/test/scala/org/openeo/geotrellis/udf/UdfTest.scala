package org.openeo.geotrellis.udf

import geotrellis.layer.{Bounds, KeyBounds, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, CellSize, FloatArrayTile, MultibandTile, Tile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, _}
import geotrellis.vector.{Extent, MultiPolygon, Polygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}

import java.time.ZonedDateTime
import java.util
import scala.collection.mutable.ListBuffer
import scala.io.Source

object UdfTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[1]", classOf[UdfTest].getName, sparkConf)
  }

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

/*
Note: Ensure that the python environment has all the required modules installed.
Numpy should be installed before Jep for off-heap memory tiles to work!

Note: In order to run these tests you need to set several environment variables.
If you use the virtual environment venv (with JEP and Numpy installed):
1. LD_LIBRARY_PATH = .../venv/lib/python3.6/site-packages/jep
  This will look for the shared library 'jep.so'. This is the compiled C code that binds Java and Python.
2. PATH = .../venv/bin:$PATH
  This will ensure your virtual environment is used instead of your default python interpreter.
*/
class UdfTest extends RasterMatchers {

  /*
    Supported CellTypes: Float
    Unsupported CellTypes: Bit, Byte, Ubyte, Short, UShort, Int, Float, Double
   */
  //@Test
  def testSimpleDatacubeOperationsFloat(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/simple_datacube_operations.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val dates = _getDates()
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = _createChunkPolygonDatacube(dates)
    datacube.values.first().bands(0).foreach(e => assert(e == 0))
    val resultRDD = Udf.runUserCode(code, datacube, new util.ArrayList[String](), new util.HashMap[String, Any]())
    resultRDD.values.first().bands(0).foreach(e => assert(e == 60))
  }

  //@Test
  def testSuperResolution(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/modify_spatial_dimensions.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val dates = _getDates()
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = _createChunkPolygonDatacube(dates)
    val currentRes = datacube.metadata.cellSize
    datacube.values.first().bands(0).foreach(e => assert(e == 0))
    val resultRDD = Udf.runUserCode(code, datacube, new util.ArrayList[String](), new util.HashMap[String, Any]())
    assertEquals(resultRDD.metadata.layout.cellSize,CellSize(currentRes.width/2.0,currentRes.height/2.0))
    assertEquals(datacube.metadata.cols*2,resultRDD.metadata.cols)
    resultRDD.values.first().bands(0).foreach(e => assert(e == 60))

  }

  def _getDates(): Seq[ZonedDateTime] = {
    val dates = Seq(
      "2017-01-15T00:01:00Z","2017-01-16T00:01:00Z", "2017-02-01T00:03:00Z",
      "2017-02-04T00:04:00Z", "2017-02-06T00:02:00Z", "2017-02-07T00:04:00Z", "2017-02-10T00:00:00Z",
      "2017-02-11T00:05:00Z", "2017-03-10T00:00:00Z", "2020-01-31T00:05:00Z")
    dates.map(ZonedDateTime.parse(_))
  }

  def _createChunkPolygonDatacube(times: Seq[ZonedDateTime]): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val constantFloats: Array[Float] = Array.fill(512*512)(0f)
    val constantBands = List.fill(2)(FloatArrayTile(constantFloats, 512, 512))
    val bands = ListBuffer[FloatArrayTile]()
    // Give a different value to every tile in tileLayout.
    // This makes it easier to identify stitching or merging errors.
    for (constantBand <- constantBands) {
      bands += constantBand.map((col: Int, row: Int, value: Int) => {
        val layoutCol = math.floor(col / 32).toInt
        val layoutRow = math.floor(row / 32).toInt
        (16*layoutCol + layoutRow)
      }).asInstanceOf[FloatArrayTile]
    }

    val arrayMultibandTile = new ArrayMultibandTile(bands.toArray)
    // Chop each large tile into a 16x16 array (with 32x32 pixels each).
    val tileLayout = new TileLayout(16, 16, 32, 32)

    // Create cube with (x, y, bands) dimensions.
    val cubeXYB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] =
      TileLayerRDDBuilders.createMultibandTileLayerRDD(
        SparkContext.getOrCreate, arrayMultibandTile, tileLayout
      ).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]
    // Add time dimension.
    val cubeXYTB: RDD[(SpaceTimeKey, MultibandTile)] = cubeXYB.flatMap((pair: Tuple2[SpatialKey, MultibandTile]) => {
      times.zipWithIndex.map(timePair => {
        (SpaceTimeKey(pair._1, TemporalKey(timePair._1)), pair._2.map((_b, c) => c + timePair._2))
      })
    })
    // Combine cube with metadata to create the final datacube.
    val md: TileLayerMetadata[SpatialKey] = cubeXYB.metadata
    val bounds: Bounds[SpatialKey] = md.bounds
    val minKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.minKey, TemporalKey(times.head))
    val maxKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.maxKey, TemporalKey(times.last))
    val metadata: TileLayerMetadata[SpaceTimeKey] = new TileLayerMetadata[SpaceTimeKey](md.cellType, md.layout, md.extent, md.crs, new KeyBounds[SpaceTimeKey](minKey, maxKey))
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(cubeXYTB, metadata)
    datacube
  }

  def _createChunkPolygonProjectedPolygons(): ProjectedPolygons = {
    val projectedPolygons1 = ProjectedPolygons.fromExtent(
      new Extent(-180, -80, -156, -72), "EPSG:4326"
    )
    val projectedPolygons2 = ProjectedPolygons.fromExtent(
      new Extent(-170, -80, -130, -70), "EPSG:4326"
    )
    val projectedPolygons3 = ProjectedPolygons.fromExtent(
      new Extent(-140, -80, -100, -70), "EPSG:4326"
    )
    val projectedPolygons4 = ProjectedPolygons.fromExtent(
      new Extent(170, 70, 175, 80), "EPSG:4326"
    )
    val projectedPolygons5 = ProjectedPolygons.fromExtent(
      new Extent(160, 70, 165, 80), "EPSG:4326"
    )
    val projectedPolygons6 = ProjectedPolygons.fromExtent(
      new Extent(150, 70, 155, 80), "EPSG:4326"
    )
    val projectedPolygonsTriangle = ProjectedPolygons(List(Polygon((-20.0,-70.0), (-50.0,-70.0), (-20.0, -80.0), (-20.0,-70.0))), "EPSG:4326")
    new ProjectedPolygons(
      projectedPolygons1.polygons ++ projectedPolygons2.polygons ++ projectedPolygons3.polygons ++
        projectedPolygons4.polygons ++ projectedPolygons5.polygons ++ projectedPolygons6.polygons ++
        projectedPolygonsTriangle.polygons,
      projectedPolygons1.crs
    )
  }

//  @Test
  def testChunkPolygonInPlaceModify(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/chunk_polygon_test.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    // Datacube extent: (-180.0, -89.99999, 179.99999, 89.99999).
    val dates = _getDates()
    val datacube = _createChunkPolygonDatacube(dates)
    val projectedPolygons = _createChunkPolygonProjectedPolygons()
    val bandNames = new util.ArrayList[String]()
    bandNames.add("B01")
    bandNames.add("B02")

    val context = new util.HashMap[String, Any]()
    val context_inner = new util.HashMap[String, Any]()
    context_inner.put("innerkey", "innervalue")
    context.put("soil_type", context_inner)
    val resultCube: MultibandTileLayerRDD[SpaceTimeKey] = Udf.runChunkPolygonUserCode(
      code, datacube, projectedPolygons, bandNames, context, -9000.0f
    )

    // Compare bands.
    val resultArray: Array[(SpaceTimeKey, MultibandTile)] = resultCube.collect()
    val tilesBand0: Iterable[(SpatialKey, Tile)] = resultArray.map(tile => (tile._1.spatialKey, tile._2.band(0)))
    val tilesBand1: Iterable[(SpatialKey, Tile)] = resultArray.map(tile => (tile._1.spatialKey, tile._2.band(1)))
    tilesBand0.zip(tilesBand1).foreach( x =>
      assert(x._1._2.asInstanceOf[FloatArrayTile]
        .toArray().filter(_ > 0).map(_+1) sameElements x._2._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0)
      )
    )

    // Compare dates.
    //resultArray.foreach (key_and_tile => {
    //  val keyToCheck = key_and_tile._1
    //  val resultTileToCheck = key_and_tile._2.band(0)
    //  val inputTileToCheck = datacube.collect().toMap.get(keyToCheck).get.band(0)
    //  val inputArray = inputTileToCheck.map(_ + 1001).toArray()
    //  inputArray.zip(resultTileToCheck.toArray()).foreach(pair => {
    //    if (pair._2 > 0) assert(pair._1 == pair._2)
    //  })
    //})

    // Assert UDF effects.
    val processes = new OpenEOProcesses
    val groupedAndMaskedByGeometry: RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])] =
      processes.groupAndMaskByGeometry(datacube, projectedPolygons, -1.0f)
    val resultCubeNoUdf: MultibandTileLayerRDD[SpaceTimeKey] = processes.mergeGroupedByGeometry(groupedAndMaskedByGeometry, datacube.metadata)
    val resultArrayNoUdf: Array[(SpaceTimeKey, MultibandTile)] = resultCubeNoUdf.collect()
    val tilesBand0NoUdf: Iterable[(SpatialKey, Tile)] = resultArrayNoUdf.map(tile => (tile._1.spatialKey, tile._2.band(0)))
    val tilesBand1NoUdf: Iterable[(SpatialKey, Tile)] = resultArrayNoUdf.map(tile => (tile._1.spatialKey, tile._2.band(1)))
    tilesBand0.zip(tilesBand0NoUdf).map(x =>
      x._1._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0).map(_ + 1001) sameElements  x._2._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0)
    )
    tilesBand0.zip(tilesBand1NoUdf).map(x =>
      x._1._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0).map(_ + 1002) sameElements  x._2._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0)
    )

    // Compare to reference tile.
    saveRDD(resultCube.toSpatial(resultArray(0)._1.time),2, "chunkPolygon_actual.tif", 6, Some(datacube.metadata.extent))
    val actualRaster = GeoTiffRasterSource("chunkPolygon_actual.tif").read().get
    val referenceRaster = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/chunkPolygon_reference.tif").read().get
    assertRastersEqual(referenceRaster, actualRaster)

    // Visualize RDD.
    // val fullRaster: Raster[Tile] = org.openeo.geotrellis.netcdf.NetCDFRDDWriter.ContextSeq(tilesBand0, datacube.metadata.layout).stitch()
    // fullRaster.tile.renderPng(colorRamp).write("udf_fullTile_band0")
  }

//  @Test
  def testChunkPolygonEditDatesAndBands(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/chunk_polygon_edit_dates_and_bands_test.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val dates = _getDates()
    val datacube = _createChunkPolygonDatacube(dates)
    val projectedPolygons = _createChunkPolygonProjectedPolygons()
    val bandNames = new util.ArrayList[String]()
    bandNames.add("B01")
    bandNames.add("B02")

    val resultCube: MultibandTileLayerRDD[SpaceTimeKey] = Udf.runChunkPolygonUserCode(
      code, datacube, projectedPolygons, bandNames, new util.HashMap[String, Any](), -1.0f
    )

    val originalArray: Array[(SpaceTimeKey, MultibandTile)] = datacube.collect()
    val resultArray: Array[(SpaceTimeKey, MultibandTile)] = resultCube.collect()
    val resultDates = resultArray.map(_._1.instant).toSet

    assert(resultDates.size == 1)
    assert(resultArray.head._2.bandCount == 1)
    assert(resultArray.head._2.cols == originalArray.head._2.cols)
    assert(resultArray.head._2.rows == originalArray.head._2.rows)

    saveRDD(resultCube.toSpatial(
      resultArray(0)._1.time),
      1,
      "chunkPolygon_edit_dates_and_bands_actual.tif",
      6,
      Some(datacube.metadata.extent)
    )
  }

}

