package org.openeo.geotrellis.udf

import geotrellis.layer.{Bounds, KeyBounds, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, FloatArrayTile, MultibandTile, MutableArrayTile, Raster, Tile, TileLayout}
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, _}
import geotrellis.vector.{Extent, MultiPolygon}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{AfterClass, BeforeClass}
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}

import java.time.ZonedDateTime
import java.util
import scala.collection.mutable.ListBuffer
import scala.io.Source

object UdfTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext("local[*]", classOf[UdfTest].getName, sparkConf)
  }

  @AfterClass
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
//  @Test
  def testSimpleDatacubeOperationsFloat(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/simple_datacube_operations.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val zeroTile: MutableArrayTile = FloatArrayTile.fill(0, 256, 256)
    val multibandTile: MultibandTile = new ArrayMultibandTile(Array(zeroTile).asInstanceOf[Array[Tile]])
    val extent: Extent = new Extent(0,0,10,10)
    val tileLayout = new TileLayout(1, 1, zeroTile.cols.asInstanceOf[Integer], zeroTile.rows.asInstanceOf[Integer])

    val tileLayerRDD: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, multibandTile, tileLayout).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]

    tileLayerRDD.values.first().bands(0).foreach(e => assert(e == 0))
    val resultRDD = Udf.runUserCode(code, tileLayerRDD, new util.ArrayList[String](), new util.HashMap[String, Any]())
    resultRDD.values.first().bands(0).foreach(e => assert(e == 60))
  }

//  @Test
  def testRunChunkPolygonUserCode(): Unit = {
    val filename = "/org/openeo/geotrellis/udf/chunk_polygon_test.py"
    val source = Source.fromURL(getClass.getResource(filename))
    val code = source.getLines.mkString("\n")
    source.close()

    val dates = Seq("2017-01-15T00:00:00Z","2017-01-16T00:00:00Z")
    val constantFloats: Array[Float] = Array.fill(512*512)(0f)
    val constantBands = List.fill(2)(FloatArrayTile(constantFloats, 512, 512))
    val bands = ListBuffer[FloatArrayTile]()
    for (constantBand <- constantBands) {
      bands += constantBand.map((col: Int, row: Int, value: Int) => {
        val layoutCol = math.floor(col / 32).toInt
        val layoutRow = math.floor(row / 32).toInt
        (16*layoutCol + layoutRow)
      }).asInstanceOf[FloatArrayTile]
    }

    val arrayMultibandTile = new ArrayMultibandTile(bands.toArray)
    // Chop each large tile in bands into a 16x16 array (with 32x32 pixels each).
    val tileLayout = new TileLayout(16, 16, 32, 32)

    // Create cube with (x, y, bands) dimensions.
    val cubeXYB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] =
      TileLayerRDDBuilders.createMultibandTileLayerRDD(
        SparkContext.getOrCreate, arrayMultibandTile, tileLayout
      ).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]
    // Add time dimension.
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val cubeXYTB: RDD[(SpaceTimeKey, MultibandTile)] = cubeXYB.flatMap((pair: Tuple2[SpatialKey, MultibandTile]) => {
      times.map((time: ZonedDateTime) => (SpaceTimeKey(pair._1, TemporalKey(time)), pair._2))
    })
    // Combine cube with metadata to create the final datacube.
    val md: TileLayerMetadata[SpatialKey] = cubeXYB.metadata
    val bounds: Bounds[SpatialKey] = md.bounds
    val minKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.minKey, TemporalKey(times.head))
    val maxKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.maxKey, TemporalKey(times.last))
    val metadata: TileLayerMetadata[SpaceTimeKey] = new TileLayerMetadata[SpaceTimeKey](md.cellType, md.layout, md.extent, md.crs, new KeyBounds[SpaceTimeKey](minKey, maxKey))
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(cubeXYTB, metadata)

    // Visualize original RDD.
    val colorRamp =
      ColorRamp(0xFF0000FF, 0x0000FFFF)
        .stops(512*512)
        .setAlphaGradient(0xFF, 0xAA)
    val tiles1: Iterable[(SpatialKey, Tile)] = datacube.collect().map(tile => (tile._1.spatialKey, tile._2.band(0)))
    val fullRaster2: Raster[Tile] = org.openeo.geotrellis.netcdf.NetCDFRDDWriter.ContextSeq(tiles1, datacube.metadata.layout).stitch()
    fullRaster2.tile.renderPng(colorRamp).write("orig_udf_fullTile")

    // Datacube extent(-180.0, -89.99999, 179.99999, 89.99999).
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
    val projectedPolygons = new ProjectedPolygons(
      projectedPolygons1.polygons ++ projectedPolygons2.polygons ++ projectedPolygons3.polygons ++
        projectedPolygons4.polygons ++ projectedPolygons5.polygons ++ projectedPolygons6.polygons,
      projectedPolygons1.crs
    )
    val bandNames = new util.ArrayList[String]()
    bandNames.add("B01")
    bandNames.add("B02")

    val resultCube: MultibandTileLayerRDD[SpaceTimeKey] = Udf.runChunkPolygonUserCode(code, datacube, projectedPolygons, bandNames, new util.HashMap[String, Any](), -1.0f)

    // Handle results

    // Compare bands.
    val resultArray: Array[(SpaceTimeKey, MultibandTile)] = resultCube.collect()
    val tilesBand0: Iterable[(SpatialKey, Tile)] = resultArray.map(tile => (tile._1.spatialKey, tile._2.band(0)))
    val tilesBand1: Iterable[(SpatialKey, Tile)] = resultArray.map(tile => (tile._1.spatialKey, tile._2.band(1)))
    tilesBand0.zip(tilesBand1).foreach( x =>
      assert(x._1._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0).map(_+1) sameElements x._2._2.asInstanceOf[FloatArrayTile].toArray().filter(_ > 0))
    )

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
    saveRDD(resultCube.toSpatial(times.head),2, "chunkPolygon_actual.tif", 6, Some(datacube.metadata.extent))
    val actualRaster = GeoTiffRasterSource("chunkPolygon_actual.tif").read().get
    val referenceRaster = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/chunkPolygon_reference.tif").read().get
    assertRastersEqual(referenceRaster, actualRaster)

    // Visualize RDD.
    // val fullRaster: Raster[Tile] = org.openeo.geotrellis.netcdf.NetCDFRDDWriter.ContextSeq(tilesBand0, datacube.metadata.layout).stitch()
    // fullRaster.tile.renderPng(colorRamp).write("udf_fullTile_band0")
  }

}

