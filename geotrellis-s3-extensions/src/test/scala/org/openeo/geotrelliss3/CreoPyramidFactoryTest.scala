package org.openeo.geotrelliss3

import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDate, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME
import java.util

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.spark._
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.{After, AfterClass, Assert, Before, BeforeClass, Ignore, Test}
import org.junit.{AfterClass, Assert, BeforeClass, Ignore, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.geotiff.saveRDD

import scala.collection.mutable.ListBuffer

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

  private var tmpDir: Path = null

  @Before
  def createTmpDir() {
    tmpDir = Files.createTempDirectory("creo")
  }

  @After
  def removeTmpDir(): Unit = {
    FileUtils.deleteDirectory(tmpDir.toFile)
  }

  @Test
  def testCreoPyramid(): Unit = {
    var tiles = ListBuffer[Tile]()

    def randomTile(cols: Int, rows: Int) = {
      ArrayTile(Array.fill(cols * rows)((math.random * 2).toShort), cols, rows)
    }

    def writeTile(dir: Path, band: String) = {
      val tile = randomTile(10, 10)
      tiles += tile
      GeoTiff(tile, Extent(0, 0, 1, 1), LatLng).write(dir.resolve(s"$band.jp2").toString)
    }

    val year = "2019"
    val month = "01"
    val day = "01"

    val dateDir = tmpDir.resolve(Paths.get("Sentinel-2", "MSI", "L2A", year, month, day,
      s"S2A_MSIL2A_$year$month${day}_T37SBT_$year$month$day.SAFE", "GRANULE", s"L2A_T37SBT_A018422_$year$month$day",
      "IMG_DATA", "R10m"))
    Files.createDirectories(dateDir)

    val bands = Seq("B02_10m", "B03_10m", "B04_10m")

    bands.foreach(writeTile(dateDir, _))

    val pyramidFactory = new CreoPyramidFactory(Seq(dateDir.toString), bands)

    val date = ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC).format(ISO_ZONED_DATE_TIME)

    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(xmin = 0, ymin = 0, xmax = 1, ymax = 1), CRS.fromEpsgCode(4326))
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, boundingBox.crs.toString, date, date)

    assertEquals(1, pyramid.size)

    val rdd = pyramid.head._2.cache

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      val Raster(multibandTile, extent) = rdd
        .toSpatial(timestamp)
        .stitch()

      val avgResult = MultibandGeoTiff(multibandTile, extent, rdd.metadata.crs).crop(boundingBox.reproject(rdd.metadata.crs))
        .tile
        .bands
        .map(b => {
          val array = b.toArrayDouble()
          array.sum / array.length
        })

      val avgExpected = MultibandTile(tiles)
        .bands
        .map(b => {
          val array = b.toArrayDouble()
          array.sum / array.length
        })

      assertArrayEquals(avgResult.toArray, avgExpected.toArray, 0.01)
    }

    for ((_, layer) <- pyramid) {
      assertFalse(layer.isEmpty())
      assertTrue(layer.first()._2.bandCount == 3)
    }
  }

  @Test
  def testCreoPyramidDatacube(): Unit = {
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

    assertEquals(1, pyramid.size)

    val rdd = pyramid.head._2.cache

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      saveRDD(rdd.toSpatial(timestamp),-1,s"${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif")
    }
  }

  @Test
  def testCreoPyramidDatacubePolygons(): Unit = {
    val pyramidFactory = new CreoPyramidFactory(
      Seq("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE",
        "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE"),
      Seq("B02_10m", "B03_10m", "B04_10m")
    )

    val date = "2019-01-01T00:00:00+00:00"

    val polygon1 = Extent(xmin = 35.55, ymin = 33.75, xmax = 35.75, ymax = 33.95).toPolygon()
    val polygon2 = Extent(xmin = 33.15, ymin = 35.25, xmax = 33.35, ymax = 35.45).toPolygon()
    val polygon3 = Extent(xmin = 13.15, ymin = 15.25, xmax = 13.35, ymax = 15.45).toPolygon()
    val polygons = Seq(polygon1, polygon2, polygon3)

    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons.apply(polygons, CRS.fromEpsgCode(4326).toString), date, date, null, null)

    assertEquals(1, pyramid.size)

    val rdd = pyramid.head._2.cache

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      val Raster(multibandTile, extent) = rdd
        .toSpatial(timestamp)
        .stitch()

      val avgResult =
        MultibandGeoTiff(multibandTile, extent, rdd.metadata.crs)
          .tile
          .bands
          .map(b => {
            val array = b.toArrayDouble()
            array.sum / array.length
          })

      assertArrayEquals(Array(223.4749, 232.3029, 235.0112), avgResult.toArray, 0.01)
    }
  }
}
