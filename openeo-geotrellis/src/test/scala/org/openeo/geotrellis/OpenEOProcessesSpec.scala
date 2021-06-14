package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import geotrellis.layer._
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff._
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, DoubleArrayTile, GridBounds, MultibandTile, Raster, Tile, TileLayout}
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

    val rs = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/S2_B04_timeseries.tiff")
    val raster = rs.read().get
    val tiles = raster.tile.bands
    val timesteps = Array(0,25,35,37,55,60,67,70,80,82,85,87,90,110,112,117,122,137,140,147,152,157,160,165,167,177,180,185,190,195,210,212,215,217,222,230,232,237,240,242,265,275,280,292,302,305,312,317,325,342,350,357,360,362,367,370,372,380,382,422,425,427,430,432,435,440,442,445,447,450,452,455,457,460,462,470,472,480,482,485,490,492,495,497,515,517,520,522,532,545,547,550,552,555,557,562,565,570,572,575,587,590,600,602,605,607,610,617,637,652,667,670,697)
    val startDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val dates = timesteps.map(startDate.plusDays(_))

    val timeseries = dates.zip(tiles).map({date_tile=>{
      (SpaceTimeKey(0,0,date_tile._1),MultibandTile(date_tile._2.withNoData(Some(32767))))
    }})

    val rdd = SparkContext.getOrCreate().parallelize(timeseries)
    val layer = ContextRDD(rdd,TileLayerMetadata(tiles.head.cellType,LayoutDefinition(raster.rasterExtent,tiles.head.size),raster.extent,rs.crs,KeyBounds[SpaceTimeKey](timeseries.head._1,timeseries.last._1)))

    val intervals = Range(0,20).flatMap{r => Seq(startDate.plusDays(10L*r),startDate.plusDays(10L*(r+1)))}.map(DateTimeFormatter.ISO_INSTANT.format(_))
    val labels = Range(0,20).map{r => DateTimeFormatter.ISO_INSTANT.format(startDate.plusDays(10L*r))}

    val resultTiles: Array[MultibandTile] = new OpenEOProcesses().aggregateTemporal(layer,intervals.asJava,labels.asJava,TestOpenEOProcessScriptBuilder.createMedian(true), java.util.Collections.emptyMap()).values.collect()
    val validTile = resultTiles.find(_ !=null).get
    val emptyTile = ArrayMultibandTile.empty(validTile.cellType,validTile.bandCount,validTile.cols,validTile.rows)
    val filledResult = resultTiles.map{t => if(t != null) t.band(0) else emptyTile.band(0)}
    GeoTiff(Raster(MultibandTile(filledResult),raster.extent),rs.crs).write("result.tiff",true)


  }
}
