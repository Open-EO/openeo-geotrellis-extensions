package org.openeo.geotrelliss3

import java.time.format.DateTimeFormatter

import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{AfterClass, Assert, BeforeClass, Ignore, Test}

object CreoPyramidFactoryTest {

  @BeforeClass
  def setupSpark(): Unit = {
    val conf = new SparkConf
    conf.setAppName("PyramidFactoryTest")
    conf.setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.maxResultSize", "4g")

    SparkContext.getOrCreate(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}

@Ignore
class CreoPyramidFactoryTest {

  @Test
  def testCreoPyramid(): Unit = {
    val pyramidFactory = new CreoPyramidFactory(
      Seq("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE",
        "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE"),
      Seq("B02_10m", "B03_10m", "B04_10m")
    )

    val date = "2019-01-01T00:00:00+00:00"

    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(xmin = 35.5517518249512, ymin = 33.7390099230957, xmax = 35.79345103698731, ymax = 33.85985951904297), CRS.fromEpsgCode(4326))
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, boundingBox.crs.toString, date, date)

    assertEquals(15, pyramid.size)

    val rdd = pyramid.head._2

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      val Raster(multibandTile, extent) = rdd
        .toSpatial(timestamp)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, rdd.metadata.crs)
      tif.write(s"/home/niels/pyramidFactory/creo/${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
    }

    for ((_, layer) <- pyramid) {
      assertFalse(layer.isEmpty())
      assertTrue(layer.first()._2.bandCount == 3)
    }
  }
}
