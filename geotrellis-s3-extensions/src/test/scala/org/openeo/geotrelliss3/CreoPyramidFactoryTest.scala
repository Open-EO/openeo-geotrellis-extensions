package org.openeo.geotrelliss3

import org.openeo.opensearch.OpenSearchResponses.{FeatureCollection, Link}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.raster.{CellSize, Raster, ShortConstantNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.vector.{Extent, Polygon, ProjectedExtent}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertFalse}
import org.junit._
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.file.Sentinel2PyramidFactory
import org.openeo.geotrellis.geotiff.saveRDD

import java.net.URI
import java.nio.file.{Files, Path}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util

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

@Ignore
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

  class MockOpenSearch extends OpenSearchClient {
    override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
      val start = dateRange.get._1
      Seq(OpenSearchResponses.Feature(id="bla",bbox.extent,start, Array(
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B02_10m.jp2"), Some("IMG_DATA_Band_B02_10m_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B03_10m.jp2"), Some("IMG_DATA_Band_B03_10m_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B04_10m.jp2"), Some("IMG_DATA_Band_B04_10m_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/MTD_TL.xml"), Some("S2_Level-2A_Tile1_Metadata"))
      ),Some(10)))
    }

    override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = {
      val start = dateRange.get._1
      FeatureCollection(1,
      Seq(OpenSearchResponses.Feature(id="bla",bbox.extent, start, Array(
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B02_10m.jp2"), Some("IMG_DATA_Band_B02_10m_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B03_10m.jp2"), Some("IMG_DATA_Band_B03_10m_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2019/01/01/S2A_MSIL2A_20190101T082331_N0211_R121_T37SBT_20190101T094029.SAFE/GRANULE/L2A_T37SBT_A018422_20190101T082935/IMG_DATA/R10m/T37SBT_20190101T082331_B04_10m.jp2"), Some("IMG_DATA_Band_B04_10m_Tile1_Data"))
      ),Some(10))).toArray)
    }

    override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  }


  @Test
  def testCreoPyramidDatacube(): Unit = {

    val pyramidFactory = new Sentinel2PyramidFactory(openSearchEndpoint="https://finder.creodias.eu/resto/api/collections/" ,openSearchCollectionId = "Sentinel2",openSearchLinkTitles = util.Arrays.asList("IMG_DATA_Band_B02_10m_Tile1_Data","S2_Level-2A_Tile1_Metadata##3","S2_Level-2A_Tile1_Metadata##1"),rootPath = "/eodata",
      maxSpatialResolution = CellSize(10,10)){
      override def createOpenSearch: OpenSearchClient = new MockOpenSearch
    }

    val date = "2019-01-01T00:00:00+00:00"

    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(xmin = 35.9517518249512, ymin = 33.7290099230957, xmax = 35.95255103698731, ymax = 33.73085951904297), CRS.fromEpsgCode(4326))
    val utmExtent = boundingBox.reproject(CRS.fromEpsgCode(32637))
    println(utmExtent)
    val projectedPolys = ProjectedPolygons.fromExtent(utmExtent,"EPSG:32637")
    val pyramid = pyramidFactory.datacube_seq(projectedPolys, date, date, new util.HashMap(), "NoID")

    assertEquals(1, pyramid.size)

    val rdd = pyramid.head._2.cache
    assertEquals(UShortConstantNoDataCellType,rdd.metadata.cellType)

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    assertFalse(timestamps.isEmpty)

    for (timestamp <- timestamps) {
      val output = s"${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif"
      println(output)
      saveRDD(rdd.toSpatial(timestamp),-1,output)
      val tiff = GeoTiff.readMultiband(output)
      assertEquals(3,tiff.tile.bandCount)
    }
  }

  @Test
  def testCreoPyramidDatacubePolygons(): Unit = {
    val pyramidFactory = new Sentinel2PyramidFactory(openSearchEndpoint="https://finder.creodias.eu/resto/api/collections/" ,openSearchCollectionId = "Sentinel2",openSearchLinkTitles = util.Arrays.asList("IMG_DATA_Band_B02_10m_Tile1_Data","IMG_DATA_Band_B03_10m_Tile1_Data","IMG_DATA_Band_B04_10m_Tile1_Data"),rootPath = "/eodata",
      maxSpatialResolution = CellSize(10,10)){
      override def createOpenSearch: OpenSearchClient = new MockOpenSearch
    }


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

    assertFalse(timestamps.isEmpty)

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


  @Ignore("Requires credentials")
  @Test
  def testPhenologyPyramidDatacube(): Unit = {


    val pyramidFactory = new Sentinel2PyramidFactory(openSearchEndpoint="https://phenology.vgt.vito.be" ,openSearchCollectionId = "copernicus_r_utm-wgs84_10_m_hrvpp-vi_p_2017-ongoing_v01_r01",openSearchLinkTitles = util.Arrays.asList("PPI"),rootPath = "/eodata",
      maxSpatialResolution = CellSize(10,10))

    val date = "2019-01-01T00:00:00+00:00"
    val endDate = "2019-01-10T00:00:00+00:00"

    //http://bboxfinder.com/#11.168332,43.468587,11.250215,43.497546
    val boundingBox: ProjectedExtent = ProjectedExtent(Extent(11.168332,43.468587,11.250215,43.497546), CRS.fromEpsgCode(4326))
    val utmExtent = boundingBox.reproject(CRS.fromEpsgCode(32632))
    println(utmExtent)
    val projectedPolys = ProjectedPolygons.fromExtent(utmExtent,"EPSG:32632")
    val properties = new util.HashMap[String,Any]()
    properties.put("accessedFrom","S3")
    val pyramid = pyramidFactory.datacube_seq(projectedPolys, date, endDate, properties, "NoID")

    assertEquals(1, pyramid.size)

    val rdd = pyramid.head._2.cache
    assertEquals(ShortConstantNoDataCellType,rdd.metadata.cellType)

    val timestamps = rdd.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    assertFalse(timestamps.isEmpty)

    for (timestamp <- timestamps) {
      val output = s"${DateTimeFormatter.ISO_LOCAL_DATE format timestamp}.tif"
      println(output)
      saveRDD(rdd.toSpatial(timestamp),-1,output)
      val tiff = GeoTiff.readMultiband(output)
      assertEquals(1,tiff.tile.bandCount)
    }
  }
}
