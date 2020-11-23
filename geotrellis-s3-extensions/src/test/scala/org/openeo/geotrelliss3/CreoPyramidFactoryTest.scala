package org.openeo.geotrelliss3

import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDate, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark._
import geotrellis.vector.{Extent, Polygon, ProjectedExtent}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.{AfterClass, BeforeClass, Ignore, Test, _}
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
    val extent = Extent(200000, 3730000, 201000, 3731000)
    val crs = CRS.fromEpsgCode(32637)
    val boundingBox = ProjectedExtent(extent, crs)
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

    var tiles = ListBuffer[Tile]()

    def randomTile(cols: Int, rows: Int) = {
      ArrayTile(Array.fill(cols * rows)((math.random * 1024).toShort), cols, rows)
    }
    val polygons = Seq(polygon1, polygon2, polygon3)

    def writeTile(dir: Path, band: String) = {
      val tile = randomTile(100, 100)
      tiles += tile
      GeoTiff(tile, extent, crs).write(dir.resolve(s"$band.jp2").toString)
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

    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons.fromExtent(extent, crs.toString()), date, date, new util.HashMap(), "NoID")

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

      assertArrayEquals(avgExpected.toArray, avgResult.toArray, 0)

      assertArrayEquals(avgResult.toArray, Array(223.4749, 232.3029, 235.0112), 0.01)
    }
  }

  @Test
  def testCreoPyramidDatacube(): Unit = {
    val pyramidFactory = new CreoPyramidFactory(
      Seq("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE"),
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
      Seq("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE"),
      Seq("B02_10m", "B03_10m", "B04_10m")
    )

    val date = "2019-01-01T00:00:00+00:00"

    val polygon1 = Extent(xmin = 195000, ymin = 3735000, xmax = 196000, ymax = 3736000).toPolygon()
    val polygon2 = Extent(xmin = 54000, ymin = 3915000, xmax = 55000, ymax = 3916000).toPolygon()
    val polygon3 = Extent(xmin = 19000, ymin = 1864000, xmax = 20000, ymax = 1865000).toPolygon()

    val projectedPolygons = ProjectedPolygons(Seq[Polygon](polygon1, polygon2, polygon3), "EPSG:32637")

    val pyramid = pyramidFactory.datacube_seq(projectedPolygons, date, date, new util.HashMap(), "NoID")

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

      assertArrayEquals(Array(1320.3015, 1521.2037, 1482.7769), avgResult.toArray, 0.01)
    }
  }
}
