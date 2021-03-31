package org.openeo.geotrellis.layers

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time._

import cats.data.NonEmptyList
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.polygonal.{PolygonalSummaryResult, Summary}
import geotrellis.raster.summary.types.MeanValue
import geotrellis.raster.{CellSize, ShortUserDefinedNoDataCellType}
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.util.SizeEstimator
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrelliscommon.DataCubeParameters

object Sentinel2FileLayerProviderTest {
  private var sc: SparkContext = _
  private val openSearchEndpoint = OpenSearch(new URL("https://services.terrascope.be/catalogue"))
  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor

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
    val layer = faparLayerProvider().readMultibandTileLayer( date, date.plusDays(1), bbox, polygons = polygonArray,polygons_crs = LatLng,zoom = 8, sc = sc,datacubeParams = Option.empty)

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
    assertEquals(10966.0, meanList.head.sum, 0.01)
    assertEquals(2652, meanList.head.count)

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

    spatialLayer.writeGeoTiff("/tmp/Sentinel2FileLayerProvider_multiband.tif", bbox)
  }


  /**
   *  Simulate 'patch extraction' as performed by WorldCereal.
   *  This should be as efficiënt as possible, working in native projection.
   */
  @Test
  def testPatchExtract(): Unit = {
    val start = ZonedDateTime.of(LocalDate.of(2020, 3, 1), MIDNIGHT, UTC)
    val end = ZonedDateTime.of(LocalDate.of(2020, 5, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(687640, 5671180, 688280, 5671820), CRS.fromEpsgCode(32631))
    //'(687640, 5671180, 688280, 5671820)'
    val time = System.currentTimeMillis()
    val parameters = new DataCubeParameters
    parameters.maskingStrategyParameters = new java.util.HashMap()
    parameters.maskingStrategyParameters.put("method","mask_scl_dilation")
    val layer = tocLayerProviderUTM.readMultibandTileLayer(from = start, to = end,bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs,zoom = 1, sc = sc, datacubeParams = Some(parameters))

    val localData = layer.collect()
    println(SizeEstimator.estimate(localData))
    println((System.currentTimeMillis()-time)/1000)
    println(localData.map(_._1.time).mkString(";"))
    assertEquals(18,localData.length)
    assertEquals(4,localData(0)._2.bandCount)
    assertFalse(localData(0)._2.band(0).isNoDataTile)
    assertEquals(ShortUserDefinedNoDataCellType(32767),localData(0)._2.band(1).cellType)
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

    spatialLayer.writeGeoTiff("/tmp/Sentinel2FileLayerProvider_10_UFS.tif", bbox)
  }

  @Ignore("TODO: verify output")
  @Test
  def testBlackStreak(): Unit = {
    import geotrellis.vector.io.json.GeoJson
    import org.apache.commons.io.IOUtils

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
    new FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = NonEmptyList.of("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution,
      pathDateExtractor,
      attributeValues
    )

  private def tocLayerProvider =
    new FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor
    )

  private def tocLayerProviderUTM =
    new FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = true
    )

  private def sceneclassificationLayerProviderUTM =
    new FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256)
    )

  private def sceneclassificationLayerProvider =
    new FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor
    )
}
