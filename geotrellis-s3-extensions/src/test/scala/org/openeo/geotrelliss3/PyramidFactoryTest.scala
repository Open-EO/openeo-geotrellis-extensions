package org.openeo.geotrelliss3

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark._
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test, Ignore}

object PyramidFactoryTest {

  @BeforeClass
  def setupSpark(): Unit = {
    val conf = new SparkConf
    conf.setAppName("PyramidFactoryTest")
    conf.setMaster("local[*]")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.driver.maxResultSize", "4g")

    SparkContext.getOrCreate(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}

@Ignore("until there's a suitable/reachable replacement for the to-be phased out ceph-mds1.vgt.vito.be")
class PyramidFactoryTest {

  @Test
  def testPyramid(): Unit = {
    val pyramidFactory = new PyramidFactory(
      endpoint = "http://ceph-mds1.vgt.vito.be:7480",
      region = "eu-central-1",
      bucketName = "unstructured_cog"
    )

    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(xmin =  4.44465, ymin = 51.17, xmax = 4.48414, ymax = 51.2007), CRS.fromEpsgCode(4326))
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, boundingBox.crs.toString, "2018-04-01T00:00:00+00:00", "2018-04-01T00:00:00+00:00")

    assertEquals(15, pyramid.size)

    for ((_, layer) <- pyramid) {
      assertFalse(layer.isEmpty())
    }
  }
}
