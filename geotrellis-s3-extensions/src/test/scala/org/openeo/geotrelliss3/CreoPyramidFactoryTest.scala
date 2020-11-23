package org.openeo.geotrelliss3

import java.time.format.DateTimeFormatter
import java.util

import geotrellis.proj4.CRS
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.geotiff.saveRDD

object CreoPyramidFactoryTest {

  @BeforeClass
  def setupSpark(): Unit = {
    val conf = new SparkConf
    conf.setAppName("PyramidFactoryTest")
    conf.setMaster("local[2]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.maxResultSize", "1g")

    SparkContext.getOrCreate(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}


class CreoPyramidFactoryTest {

  @Test
  def testCreoPyramid(): Unit = {
    val pyramidFactory = new CreoPyramidFactory(
      Seq("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE",
        "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE"),
      Seq("B02_10m", "B03_10m", "B04_10m")
    )

    val date = "2019-01-01T00:00:00+00:00"

    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(xmin = 35.9517518249512, ymin = 33.7290099230957, xmax = 35.95255103698731, ymax = 33.73085951904297), CRS.fromEpsgCode(4326))
    val utmExtent = boundingBox.reproject(CRS.fromEpsgCode(32637))
    println(utmExtent)
    val projectedPolys = ProjectedPolygons.fromExtent(utmExtent,"EPSG:32637")
    val pyramid = pyramidFactory.datacube_seq(projectedPolys, date, date, new util.HashMap(), "NoID")

    assertEquals(15, pyramid.size)

    val rdd = pyramid.head._2

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      saveRDD(rdd.toSpatial(timestamp),-1,s"${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
    }

    for ((_, layer) <- pyramid) {
      assertFalse(layer.isEmpty())
      assertTrue(layer.first()._2.bandCount == 3)
    }
  }
}
