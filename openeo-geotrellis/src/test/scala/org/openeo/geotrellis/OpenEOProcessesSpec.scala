package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import geotrellis.layer._
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.io.geotiff._
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, ByteConstantTile, DoubleArrayTile, GridBounds, IntConstantNoDataCellType, MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory
import org.openeo.geotrellis.geotiff.ContextSeq
import org.openeo.geotrellisaccumulo.PyramidFactory

import scala.collection.JavaConverters._

object OpenEOProcessesSpec{

  private var sc: SparkContext = _

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().setMaster("local[2]")//.set("spark.driver.bindAddress", "127.0.0.1")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }

  def getPixel(layer:MultibandTileLayerRDD[SpaceTimeKey]): Array[Int] = {
    new OpenEOProcesses().filterEmptyTile(layer).groupBy(_._1).mapValues(values => {
      val raster: Raster[MultibandTile] = ContextSeq(values.map(v => (v._1.spatialKey, v._2)).seq, layer.metadata).stitch()
      raster.tile.band(0).get(0,0)
    }).collect().sortBy(_._1.instant).map(_._2)
  }

}

class OpenEOProcessesSpec extends RasterMatchers {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  private lazy val accumuloPyramidFactory = {new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")}
  private val pyramidFactory = new Sentinel2RadiometryPyramidFactory

  private def dataCube(minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid = pyramidFactory.pyramid_seq(bbox, srs, minDateString, maxDateString, java.util.Arrays.asList(1, 2))
    System.out.println("pyramid = " + pyramid)

    val pyramidAsMap = pyramid.toMap
    val maxZoom = pyramidAsMap.keys.max
    val datacube = pyramidAsMap.get(maxZoom).get
    datacube
  }

  private def accumuloDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid = accumuloPyramidFactory.pyramid_seq(layer,bbox, srs, minDateString, maxDateString)
    System.out.println("pyramid = " + pyramid)

    val pyramidAsMap = pyramid.toMap
    val maxZoom = pyramidAsMap.keys.max
    val datacube = pyramidAsMap.get(maxZoom).get
    datacube
  }

  /**
    * Test created in the frame of:
    * https://github.com/locationtech/geotrellis/issues/3168
    */
  @Test
  def applyMask() = {
    val date = "2018-05-06T00:00:00Z"

    val extent = Extent(3.4, 51.0, 3.5, 51.05)
    val datacube= dataCube( date, date, extent, "EPSG:4326")

    val selectedBands = datacube.withContext(_.mapValues(_.subsetBands(1)))

    val mask = accumuloDataCube("S2_SCENECLASSIFICATION_PYRAMID_20200407", date, date, extent, "EPSG:4326")
    val binaryMask = mask.withContext(_.mapValues( _.map(0)(pixel => if ( pixel == 5) 0 else 1)))

    print(binaryMask.partitioner)

    val maskedCube: MultibandTileLayerRDD[SpaceTimeKey] = new OpenEOProcesses().rasterMask(selectedBands, binaryMask, Double.NaN)
    val stitched = maskedCube.toSpatial().stitch()
    print(stitched)
  }

  @Test
  def applyMask_spacetime_spatial() = {
    val date = "2018-05-06T00:00:00Z"

    val extent = Extent(3.4, 51.0, 3.5, 51.05)
    val datacube= dataCube( date, date, extent, "EPSG:4326")

    val selectedBands = LayerFixtures.sentinel2B04Layer

    val maskTile = new ByteConstantTile(0.toByte, 256, 256).mutable
    maskTile.set(0, 0, 0)
    maskTile.set(0, 1, 1)
    maskTile.set(0, 2, 1)

    val mask = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new Raster(new ArrayMultibandTile(Array[Tile](maskTile)),selectedBands.metadata.extent), selectedBands.metadata.tileLayout,selectedBands.metadata.crs)
                  .withContext(_.mapValues(t => MultibandTile(maskTile)))

    val maskedCube: MultibandTileLayerRDD[SpaceTimeKey] = new OpenEOProcesses().rasterMask_spacetime_spatial(selectedBands, mask, 123)
    val tiles = maskedCube.collectAsMap()

    import scala.collection.JavaConversions._
    for (tileEntry <- tiles.entrySet) {
      val tile = tileEntry.getValue.band(0)

      //get method applies a conversion to int, also nodata is converted
      val value = tile.get(0, 1)
      assertTrue(123 == value || IntConstantNoDataCellType.noDataValue == value)
      val value2 = tile.get(0, 2)
      assertTrue(123 == value || IntConstantNoDataCellType.noDataValue == value)

    }


  }


  @Test
  def applyMaskFFT(): Unit = {
    val tile: Tile = DoubleArrayTile.fill(1.0,1280, 1280)
    val tileSize = 256
    val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new ArrayMultibandTile(Array[Tile](tile)), new TileLayout(1 + tile.cols / tileSize, 1 + tile.rows / tileSize, tileSize, tileSize))
    val kernel: Tile = DoubleArrayTile.fill(1.0,61, 61)

    val resultCube = new OpenEOProcesses().apply_kernel_spatial(datacube, kernel)

    val theResultTile = time{ resultCube.stitch().tile.band(0) }
    val expectedConvolution = time{Convolve.apply(tile, new Kernel(kernel), Option.empty, TargetCell.All)}
    assertEqual(expectedConvolution,theResultTile)
  }

  @Test
  def makeSquareTile(): Unit = {
    val tile: MultibandTile = MultibandTile( DoubleArrayTile.fill(1.0,144, 160))
    var squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(tile,new GridBounds[Int](0,16,128,144)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(4,20),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(20,4),0.0)

    squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(MultibandTile( DoubleArrayTile.fill(1.0,160, 144)),new GridBounds[Int](16,0,144,128)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(1.0,squareTile.band(0).getDouble(4,20),0.0)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(20,4),0.0)

    squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(MultibandTile( DoubleArrayTile.fill(1.0,160, 144)),new GridBounds[Int](16,16,144,144)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(20,150),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(20,140),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(4,20),0.0)
  }

  @Test
  def medianComposite():Unit = {

    val layer:MultibandTileLayerRDD[SpaceTimeKey] = LayerFixtures.sentinel2B04Layer

    val startDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val intervals = Range(0,20).flatMap{r => Seq(startDate.plusDays(10L*r),startDate.plusDays(10L*(r+1)))}.map(DateTimeFormatter.ISO_INSTANT.format(_))
    val labels = Range(0,20).map{r => DateTimeFormatter.ISO_INSTANT.format(startDate.plusDays(10L*r))}

    val resultTiles: Array[MultibandTile] = new OpenEOProcesses().aggregateTemporal(layer,intervals.asJava,labels.asJava,TestOpenEOProcessScriptBuilder.createMedian(true), java.util.Collections.emptyMap()).values.collect()
    val validTile = resultTiles.find(_ !=null).get
    val emptyTile = ArrayMultibandTile.empty(validTile.cellType,validTile.bandCount,validTile.cols,validTile.rows)
    val filledResult = resultTiles.map{t => if(t != null) t.band(0) else emptyTile.band(0)}
    GeoTiff(Raster(MultibandTile(filledResult),layer.metadata.extent),layer.metadata.crs).write("result.tiff",true)


  }

}
