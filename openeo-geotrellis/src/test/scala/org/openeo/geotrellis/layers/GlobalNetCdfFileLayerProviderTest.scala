package org.openeo.geotrellis.layers

import cats.data.NonEmptyList

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import com.azavea.gdal.GDALWarp
import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, LayoutDefinition, LayoutTileSource, TileLayerMetadata, TileToLayoutOps}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{CellSize, GridExtent, RasterExtent, RasterSource, TileLayout, UByteUserDefinedNoDataCellType}
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{AfterClass, Test}
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.GlobalNetCDFSearchClient

import java.util
import scala.collection.JavaConverters._

object GlobalNetCdfFileLayerProviderTest extends LocalSparkContext {
  @AfterClass
  def tearDown(): Unit = GDALWarp.deinit()
}

class GlobalNetCdfFileLayerProviderTest {
  import GlobalNetCdfFileLayerProviderTest._

  private def layerProvider = new GlobalNetCdfFileLayerProvider(
    dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/*/*/*/*.nc",
    bandName = "LAI",
    dateRegex = raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored
  )

  private val bands: util.List[String] = util.Arrays.asList("LAI", "NOBS")

  private def multibandFileLayerProvider = FileLayerProvider(
    new GlobalNetCDFSearchClient(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/*/*/*/*.nc",
      bands,
      dateRegex = raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored
    ),
    openSearchCollectionId = "BioPar_LAI300_V1_Global",
    openSearchLinkTitles = NonEmptyList.fromListUnsafe(bands.asScala.toList),
    rootPath = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global",
    maxSpatialResolution = CellSize(0.002976190476204,0.002976190476190),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = FloatingLayoutScheme(256)
  )

  class MockGlobalNetCdf extends GlobalNetCdfFileLayerProvider(dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/*/*/*/*.nc",
    bandName = "LAI",
    dateRegex = raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored) {
    override protected def paths: List[Path] = List(
      new Path("/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2005/20050228/c_gls_FAPAR_200502280000_GLOBE_VGT_V2.0.1/c_gls_FAPAR_200502280000_GLOBE_VGT_V2.0.1.nc"),
      new Path("/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2015/20150630/c_gls_FAPAR-RT6_201506300000_GLOBE_PROBAV_V2.0.2/c_gls_FAPAR-RT6_201506300000_GLOBE_PROBAV_V2.0.2.nc"),
      new Path("/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2015/20150630/c_gls_FAPAR-RT6_201506300000_GLOBE_PROBAV_V2.0.1/c_gls_FAPAR-RT6_201506300000_GLOBE_PROBAV_V2.0.1.nc"),
      new Path("/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2020/20200110/c_gls_FAPAR-RT2_202001100000_GLOBE_PROBAV_V2.0.1/c_gls_FAPAR-RT2_202001100000_GLOBE_PROBAV_V2.0.1.nc"),
      new Path("/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2020/20200110/c_gls_FAPAR-RT6_202001100000_GLOBE_PROBAV_V2.0.1/c_gls_FAPAR-RT6_202001100000_GLOBE_PROBAV_V2.0.1.nc")

    )

    override def queryAll(): Array[(ZonedDateTime, RasterSource)] = super.queryAll()
  }

  @Test
  def testHeterogenousPaths(): Unit = {
    val provider = new MockGlobalNetCdf

    val sources = provider.queryAll()
    assertEquals(3,sources.length)
  }

  @Test
  def readTileLayer(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = layerProvider.readTileLayer(from = date, to = date, boundingBox, sc = sc).cache()

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2_readTileLayer.tif")
  }

  @Test
  def readDataCube(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val layer = layerProvider.readMultibandTileLayer(date, date, ProjectedPolygons.fromExtent(boundingBox.extent,"EPSG:4326") , layerProvider.maxZoom, sc,Option(parameters)).cache()

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2_readDataCube.tif")
  }

  @Test
  def loadMetadata(): Unit = {
    val Some((ProjectedExtent(Extent(xmin, ymin, xmax, ymax), crs), dates)) = layerProvider.loadMetadata(sc)

    assertEquals(LatLng, crs)
    assertEquals(-180.0014881, xmin, 0.001)
    assertEquals(-59.9985119, ymin, 0.001)
    assertEquals(179.9985119, xmax, 0.001)
    assertEquals(80.0014881, ymax, 0.001)

    val years = dates.map(_.getYear).distinct

    assertTrue("year 2014 is missing", years contains 2014)
    assertTrue("year 2020 is missing", years contains 2020)
  }

  @Test
  def readMetadata(): Unit = {
    val TileLayerMetadata(cellType, layout, Extent(xmin, ymin, xmax, ymax), crs, KeyBounds(minKey, maxKey)) =
      layerProvider.readMetadata(sc = sc)

    assertEquals(UByteUserDefinedNoDataCellType(255.toByte), cellType)

    assertEquals(LatLng, crs)
    assertEquals(-180.0014881, xmin, 0.001)
    assertEquals(-59.9985119, ymin, 0.001)
    assertEquals(179.9985119, xmax, 0.001)
    assertEquals(80.0014881, ymax, 0.001)

    val expectedZoom = 9
    val expectedLayoutCols = math.pow(2, expectedZoom).toInt

    assertEquals(expectedLayoutCols, layout.layoutCols)
    //minkey is negative (-1), because of projected extent which is smaller then -180, not sure if space partitioner deals
    //with that very well
    assertEquals(-1, minKey.col)
    assertEquals(expectedLayoutCols - 1, maxKey.col)
  }

  @Test
  def zonalMean(): Unit = {
    val from = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val to = LocalDate.of(2017, 1, 31).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = layerProvider.readTileLayer(from, to, boundingBox, sc = sc).cache()

    def mean(at: ZonedDateTime): Double = {
      val Summary(mean) = layer
        .toSpatial(at)
        .polygonalSummaryValue(boundingBox.extent.toPolygon(), MeanVisitor)

      val netCdfScalingFactor = 0.0333329997956753
      mean.mean * netCdfScalingFactor
    }

    // physical means derived from the corresponding geotiff in QGIS
    assertEquals(1.0038442394683125, mean(from), 0.001)
    assertEquals(1.0080865841250772, mean(to), 0.001)
  }

  @Test
  def readTileLayerWithOpensearchClient(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = multibandFileLayerProvider.readTileLayer(from = date, to = date, boundingBox, sc = sc).cache()

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2.tif")
  }

  @Test
  def readDataCubeWithOpensearchClient(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, "EPSG:4326")
    val layer = multibandFileLayerProvider.readMultibandTileLayer(
      date, date, boundingBox, polygons.polygons, polygons.crs, layerProvider.maxZoom, sc, Option(parameters)).cache()

    val (_, arbitraryTile) = layer.first()
    assertEquals(2, arbitraryTile.bandCount)

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2.tif")
  }

  @Test
  def zonalMeanWithOpensearchClient(): Unit = {
    val from = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val to = LocalDate.of(2017, 2, 1).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = multibandFileLayerProvider.readTileLayer(from, to, boundingBox, sc = sc).cache()

    def mean(at: ZonedDateTime): Double = {
      val Summary(mean) = layer
        .toSpatial(at)
        .polygonalSummaryValue(boundingBox.extent.toPolygon(), MeanVisitor)

      val netCdfScalingFactor = 0.0333329997956753
      mean.mean * netCdfScalingFactor
    }

    assertEquals(1.0106459861932706, mean(from), 0.001)
    assertEquals(1.014475609832233, mean(to.minusDays(1)), 0.001)
  }

  @Test
  def compareDefaultLayerToOpensearchClientLayer(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-81.30859375, 34.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val layerProvider2WithOneBand = FileLayerProvider(new GlobalNetCDFSearchClient(dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/2017/20170110/*/*.nc",bands , raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored),"BioPar_LAI300_V1_Global",
      NonEmptyList.of("LAI"),
      rootPath = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global",
      maxSpatialResolution = CellSize(0.002976190476204,0.002976190476190),
      new Sentinel5PPathDateExtractor(maxDepth = 3),
      layoutScheme = FloatingLayoutScheme(256))

    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, "EPSG:4326")
    val layer1 = layerProvider.readMultibandTileLayer(date, date, ProjectedPolygons.fromExtent(boundingBox.extent,"EPSG:4326") , layerProvider.maxZoom, sc,Option(parameters)).cache()
    val layer2 = layerProvider2WithOneBand.readMultibandTileLayer(date, date, boundingBox, polygons.polygons, polygons.crs , layerProvider.maxZoom, sc,Option(parameters)).cache()

    val collectedLayer1 = layer1.collect()
    val collectedLayer2 = layer2.collect()

    // Check if for every tile in collectedLayer1 there is a tile in collectedLayer2 with the same key and structure.
    collectedLayer1.foreach { case (key, tile) =>
      val possibleTile2 = collectedLayer2.find(_._1 == key)
      assertTrue(possibleTile2.isDefined)
      val tile2 = possibleTile2.get._2
      assertEquals(tile.bandCount, tile2.bandCount)
      assertEquals(tile.cellType, tile2.cellType)
      assertEquals(tile.cols, tile2.cols)
      assertEquals(tile.rows, tile2.rows)
    }
  }

}
