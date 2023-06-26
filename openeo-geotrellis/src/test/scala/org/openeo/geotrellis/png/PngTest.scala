package org.openeo.geotrellis.png

import ar.com.hjg.pngj.PngReader
import geotrellis.proj4.LatLng
import geotrellis.raster.ColorRamps
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import io.findify.s3mock.S3Mock
import org.apache.spark.SparkContext
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.junit.{AfterClass, BeforeClass, Rule, Test}
import org.openeo.geotrellis.LayerFixtures
import org.openeo.geotrellis.creo.CreoS3Utils
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, GetObjectRequest}

import java.util
import scala.annotation.meta.getter

object PngTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    // originally geotrellis.spark.util.SparkUtils.createLocalSparkContext
    val conf = SparkUtils.createSparkConf
      .setMaster("local[2]")
      .setAppName(PngTest.getClass.getName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .set("spark.kryo.registrationRequired", "true") // this requires e.g. RasterSource to be registered too
      .set("spark.kryo.registrator", Seq(
        classOf[geotrellis.spark.store.kryo.KryoRegistrator].getName,
        classOf[org.openeo.geotrellis.png.KryoRegistrator].getName) mkString ","
      )

    sc = new SparkContext(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit =
    sc.stop()
}

class PngTest {

  @(Rule@getter)
  val environmentVariables = new EnvironmentVariables

  @Test
  def testSaveStitched(): Unit = {

    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)
    val tileLayerRDD =  LayerFixtures.aSpacetimeTileLayerRdd( 8,4)

    val spatialLayer = tileLayerRDD._1.toSpatial()

    val singleBand = spatialLayer.withContext{_.mapValues(_.subsetBands(0))}
    val opts = new PngOptions
    opts.setColorMap(new util.ArrayList(java.util.Arrays.asList(10,20,584854)))
    saveStitched(singleBand, "/tmp/testSaveStitchedColormap.png",null,opts)
  }

  @Test
  def testSaveStitchedColormap(): Unit = {
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)
    val tileLayerRDD =  LayerFixtures.aSpacetimeTileLayerRdd( 8,4)

    val spatialLayer = tileLayerRDD._1.toSpatial()

    val singleBand = spatialLayer.withContext{_.mapValues(_.subsetBands(0))}
    val opts = new PngOptions

    opts.setColorMap(ColorRamps.BlueToRed.toColorMap(Range(0,256).toArray))
    saveStitched(singleBand, "/tmp/testSaveStitchedColormap.png",null,opts)
  }

  @Test
  def testSaveStitchedS3(): Unit = {
    val s3Port = 8001
    environmentVariables.set("SWIFT_URL", s"http://localhost:$s3Port")

    val bucket = "foo"
    val prefix = "j-abc123"
    val filename = "testSaveStitchedS3.png"
    val key = s"$prefix/$filename"

    def savePngToS3(): Unit = {
      val tileLayerRDD = LayerFixtures.aSpacetimeTileLayerRdd(8, 4)

      val spatialLayer = tileLayerRDD._1.toSpatial()

      val singleBand = spatialLayer.withContext {
        _.mapValues(_.subsetBands(0))
      }

      val opts = new PngOptions
      opts.setColorMap(ColorRamps.BlueToRed.toColorMap((0 to 255).toArray))
      saveStitched(singleBand, s"s3://$bucket/$key", null, opts)
    }

    val s3 = S3Mock(s3Port)
    s3.start

    try {
      val s3Client = CreoS3Utils.getCreoS3Client()
      try {
        val createBucket = CreateBucketRequest.builder().bucket(bucket).build()
        s3Client.createBucket(createBucket)

        savePngToS3()

        val in = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())
        try new PngReader(in) // will throw on faulty PNG
        finally in.close()
      } finally s3Client.close()
    } finally s3.shutdown
  }
}
