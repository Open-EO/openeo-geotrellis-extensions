package org.openeo.geotrellis.layers

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time._

import org.openeo.geotrellis.TestImplicits._
import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.polygonal.{PolygonalSummaryResult, Summary}
import geotrellis.raster.summary.types.MeanValue
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Ignore, Test}

object Sentinel2FileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]",
    appName = Sentinel2FileLayerProviderTest.getClass.getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

class Sentinel2FileLayerProviderTest {
  import Sentinel2FileLayerProviderTest._

  @Test
  def polygonalMultiplePolygon(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygons = ShapeFileReader.readMultiPolygonFeatures(getClass.getResource("/org/openeo/geotrellis/layers/tile1_polygons.shp")).map(_.geom)
    val bbox = ProjectedExtent(polygons.extent,LatLng)
    val polygonArray = polygons.toArray

    //use lower zoom level to make test go faster
    val layer = faparLayerProvider().readMultibandTileLayer( date, date.plusDays(1), bbox, polygons = polygonArray,polygons_crs = LatLng,zoom = 8, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    //spatialLayer.withContext(_.mapValues(_.band(0))).writeGeoTiff("/tmp/Sentinel2FileLayerProvider_polygonalMultiplePolygon.tif", ProjectedExtent(layer.metadata.extent,layer.metadata.crs))

    val summary = spatialLayer.polygonalSummary(polygons.map{_.reproject(LatLng,layer.metadata.crs)}, MeanVisitor).collect()
    print(summary.size)
    val values: Array[Double] = summary.map(_.data.toOption.get(0).sum)
    val counts: Array[Long] = summary.map(_.data.toOption.get(0).count)
    val resultArray: Array[Double] = Array(15228.0,26313.0,220392.0,511556.0)
    val expectedCounts: Array[Long] = Array(349,489,3415,3738)
    assertArrayEquals(expectedCounts, counts.sorted)
    assertArrayEquals(resultArray, values.sorted,0.001)
  }

  @Test
  def polygonalMean(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    val layer = faparLayerProvider().readTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    // spatialLayer.writeGeoTiff("/tmp/Sentinel2FileLayerProvider_polygonalMean.tif", bbox)

    val polygon = bbox.reprojectAsPolygon(spatialLayer.metadata.crs)

    val Summary(value) = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor)

    val qgisZonalStatisticsPluginResult = 48.7280433452766
    assertEquals(qgisZonalStatisticsPluginResult, value.mean, 0.1)
  }

  @Test
  def polygonalMeanOnOverlap(): Unit = {
    val bbox = ProjectedExtent(Extent(3.032755, 50.839076, 3.039980, 50.843650), LatLng)
    val date = ZonedDateTime.of(LocalDate.of(2018, 8, 14), MIDNIGHT, UTC)

    val layer = sceneclassificationLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()
    val start = System.currentTimeMillis()
    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(bbox.reprojectAsPolygon(spatialLayer.metadata.crs), geotrellis.raster.summary.polygonal.visitors.MeanVisitor)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get
    println("Time: "+ (System.currentTimeMillis() - start)/1000.0)
    assertEquals(1,meanList.length)
    assertEquals(29874.0,meanList.head.sum,0.00001)
    assertEquals(7225,meanList.head.count)

  }

  @Test
  def polygonalMeanOnOverlapNativeUTM(): Unit = {
    val utm31 = CRS.fromEpsgCode(32631)
    val bbox = ProjectedExtent(ProjectedExtent(Extent(3.032755, 50.839076, 3.039980, 50.843650), LatLng).reproject(utm31),utm31)
    val date = ZonedDateTime.of(LocalDate.of(2018, 8, 14), MIDNIGHT, UTC)

    val layer = sceneclassificationLayerProviderUTM.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)
    println(layer.metadata.crs)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    val start = System.currentTimeMillis()
    val summary: PolygonalSummaryResult[Array[MeanValue]] = spatialLayer.polygonalSummaryValue(bbox.reprojectAsPolygon(spatialLayer.metadata.crs), geotrellis.raster.summary.polygonal.visitors.MeanVisitor)

    assertTrue(summary.toOption.isDefined)
    val meanList = summary.toOption.get

    println("Time: "+ (System.currentTimeMillis() - start)/1000.0)
    assertEquals(1,meanList.length)
    assertEquals(29874.0/7225.0,meanList.head.mean,0.01)
    assertEquals(11202.0, meanList.head.sum, 0.01)
    assertEquals(2704, meanList.head.count)

  }

  @Test(timeout = 20000) // generous timeout
  def loadMetadata(): Unit = {
    val Some((extent, dates)) = faparLayerProvider().loadMetadata(sc)

    assertEquals(WebMercator, extent.crs)

    assertTrue(dates.length > 1000)

    val uniqueYears = dates
      .map(_.getYear)
      .distinct

    assertTrue(uniqueYears contains 2015)
    assertTrue(uniqueYears contains 2020)
  }

  @Test
  def multiband(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    val layer = tocLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    spatialLayer.writeGeoTiff(bbox, "/tmp/Sentinel2FileLayerProvider_multiband.tif")
  }

  @Ignore("TODO: verify output")
  @Test
  def filterByAttributeValue(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(4.399681091308594, 51.06869305078254, 4.446201324462891, 51.08233659233106), LatLng)

    val layer = faparLayerProvider(Map("tileId"-> "31UFS", "resolution" -> 10))
      .readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    spatialLayer.writeGeoTiff(bbox, "/tmp/Sentinel2FileLayerProvider_10_UFS.tif")
  }

  @Ignore("TODO: verify output")
  @Test
  def testBlackStreak(): Unit = {
    import org.apache.commons.io.IOUtils
    import geotrellis.vector.io.json.GeoJson

    val date = ZonedDateTime.of(LocalDate.of(2020, 6, 24), MIDNIGHT, UTC)

    val geojson =
      IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/layers/testBlackStreak.geojson"))

    val multiPolygon: MultiPolygon = GeoJson.parse[MultiPolygon](geojson)
    val boundingBox = ProjectedExtent(multiPolygon.extent, LatLng)
    val buffer = boundingBox.extent.width * 1.0 // make additional S2 tile on the left join in
    val bufferedBoundingBox = ProjectedExtent(boundingBox.extent.buffer(buffer), boundingBox.crs)

    val layer = sceneclassificationLayerProvider.readTileLayer(
      from = date,
      to = date,
      bufferedBoundingBox,
      sc = sc
    )

    val spatialLayer = layer.toSpatial(date)

    spatialLayer.writeGeoTiff("/tmp/testBlackStreak_left_GeoTiffRasterSource_ND0_notcropped_test.tif", bufferedBoundingBox)
  }

  private def faparLayerProvider(attributeValues: Map[String, Any] = Map()) =
    new Sentinel2FileLayerProvider(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      oscarsLinkTitle = "FAPAR_10M",
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      attributeValues
    )

  private def tocLayerProvider =
    new Sentinel2FileLayerProvider(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      oscarsLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2"
    )

  private def sceneclassificationLayerProviderUTM =
    new Sentinel2FileLayerProvider(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      oscarsLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      layoutScheme = FloatingLayoutScheme(256)
    )

  private def sceneclassificationLayerProvider =
    new Sentinel2FileLayerProvider(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      oscarsLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2"
    )
}
