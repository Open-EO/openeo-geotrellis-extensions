package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{GeoTiffReader, MultibandGeoTiff}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.polygonal.{PolygonalSummaryResult, Summary}
import geotrellis.raster.summary.types.MeanValue
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{CellSize, ShortUserDefinedNoDataCellType, UShortConstantNoDataArrayTile}
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.util.SizeEstimator
import org.junit.Assert._
import org.junit._
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.geotiff.{GTiffOptions, saveRDD}
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcessScriptBuilder, OpenEOProcesses}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters}
import org.openeo.opensearch.OpenSearchResponses.Link
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}

import java.net.URI
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time._
import java.util
import java.util.Collections
import scala.collection.JavaConverters.mapAsJavaMapConverter

object Sentinel2FileLayerProviderTest {
  private var sc: SparkContext = _
  private val openSearchEndpoint = LayerFixtures.client
  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[1]",
    appName = Sentinel2FileLayerProviderTest.getClass.getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()

  @BeforeClass def tracking(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(true)
  }

  @AfterClass def trackingOff(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(false)
  }
}

class Sentinel2FileLayerProviderTest extends RasterMatchers {
  import Sentinel2FileLayerProviderTest._

  @Before
  def clearTracker(): Unit = {
    BatchJobMetadataTracker.clearGlobalTracker()
  }

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
    val inputs = BatchJobMetadataTracker.tracker("").asDict().get("links")

    assertEquals(2,inputs.asInstanceOf[util.Map[String,util.List[String]]].get("urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2").size())
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

  private def dummyMap(keys: String*) = {
    val m = new util.HashMap[String, AnyRef]
    for (key <- keys) {
      m.put(key, "dummy")
    }
    m
  }

  @Test
  def multibandWithSpacetimeMask(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    var mask = sceneclassificationLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val builder: OpenEOProcessScriptBuilder = new OpenEOProcessScriptBuilder
    val args: util.Map[String, AnyRef] = dummyMap("x", "y")
    builder.expressionStart("gte", args)
    builder.argumentStart("x")
    builder.argumentEnd()
    builder.constantArgument("y", 4)
    builder.expressionEnd("gte", args)
    //mask.toSpatial(date).writeGeoTiff("/tmp/Sentinel2FileLayerProvider_multiband_mask.tif", bbox)
    val p = new OpenEOProcesses()
    mask = p.mapBands(mask, builder)

    var layer = tocLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs, sc = sc,zoom = 14,datacubeParams = Option.empty)

    val originalCount = layer.count()
    mask = p.resampleCubeSpatial(mask,layer,ResampleMethod.DEFAULT)._2
    val parameters = new DataCubeParameters()
    parameters.maskingCube = Some(mask)
    layer = tocLayerProvider.readMultibandTileLayer(from = date, to = date, bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs, sc = sc,zoom = 14,datacubeParams = Some(parameters))

    val maskedCount = layer.count()
    val spatialLayer = p.rasterMask(layer,mask,Double.NaN)
      .toSpatial(date)
      .cache()

    spatialLayer.writeGeoTiff("/tmp/Sentinel2FileLayerProvider_multiband.tif", bbox)
    assertNotEquals(originalCount,maskedCount)
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

  @Test
  def testReadDifferentProjection():Unit = {

    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 666460, 5701770), crs)
    val utm32 = CRS.fromEpsgCode(32632)
    val bboxUTM32 = boundingBox.reproject(utm32)

    val dataCubeParameters = new DataCubeParameters

    val layer = LayerFixtures.sentinel2TocLayerProviderUTM.readMultibandTileLayer(
      from = date,
      to = date,
      ProjectedExtent(bboxUTM32,utm32),
      polygons = Array(MultiPolygon(bboxUTM32.toPolygon())),
      polygons_crs = utm32,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val spatialLayer = layer.toSpatial(date)

    val reprojectedBoundingBox = boundingBox.reproject(spatialLayer.metadata.crs)

    val output_path = "/tmp/utm32.tif"
    val options = new GTiffOptions()
    options.overviews = "ALL"
    saveRDD(spatialLayer,-1,output_path,cropBounds=Some(reprojectedBoundingBox),formatOptions=options)


    val stitched: MultibandGeoTiff = GeoTiffReader.readMultiband(output_path)
    assertFalse(stitched.tile.band(0).isNoDataTile)
    assertEquals(utm32,spatialLayer.metadata.crs)
  }

  @Test
  def testMaskSclDilationOnS2TileEdge(): Unit = {
    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 666460, 5701770), crs)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.maskingStrategyParameters = Collections.singletonMap("method", "mask_scl_dilation")

    val layer = tocLayerProviderUTM.readMultibandTileLayer(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val spatialLayer = layer.toSpatial(date)

    val reprojectedBoundingBox = boundingBox.reproject(spatialLayer.metadata.crs)

    spatialLayer.sparseStitch(reprojectedBoundingBox) match {
      case Some(stitched) => MultibandGeoTiff(stitched.crop(reprojectedBoundingBox), spatialLayer.metadata.crs).write("/tmp/masked.tif")
      case _ => throw new IllegalStateException("nothing to sparse-stitch")
    }

    val referenceTile = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/dilation_masked.tif").read().get
    val actualTile = GeoTiffRasterSource("/tmp/masked.tif").read().get
    assertRastersEqual(referenceTile,actualTile,160.0)
  }

  @Test
  def testMaskL1CRasterSourceFiltering(): Unit = {
    class MockOpenSearch extends OpenSearchClient {
      override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
        val start = dateRange.get._1
        Seq(OpenSearchResponses.Feature(id="/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE",bbox.extent,start, Array(
          Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          //Link(URI.create("/data/MTDA/CGS_S2/CGS_S2_L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          Link(URI.create("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/MTD_TL.xml"), Some("S2_Level-1C_Tile1_Metadata")),
          Link(URI.create("https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/QI_DATA/MSK_CLOUDS_B00.gml"), Some("FineCloudMask_Tile1_Data"))
          ),Some(10)))
      }
      override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???
      override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
    }

    val creoL1CLayerProvider = new FileLayerProvider(
      new MockOpenSearch,
      openSearchCollectionId = "Sentinel2",
      openSearchLinkTitles = NonEmptyList.of("IMG_DATA_Band_10m_1_Tile1_Data"),
      rootPath = "/eodata",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(tileSize = 256)
      )

    val date = ZonedDateTime.parse("2021-01-01T00:00:00+00:00")
    val utm11NCrs = CRS.fromEpsgCode(32611)
    val boundingBox = ProjectedExtent(Extent(499980,5200020-1000,499980+1000,5200020), utm11NCrs)
    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.maskingStrategyParameters = Map[String, Object](
      "method" -> "mask_l1c",
      "dilation_distance" -> "10000").asJava

    // A large dilation distance will filter out all raster sources and return an exception.
    assertThrows[IllegalArgumentException](creoL1CLayerProvider.readMultibandTileLayer(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = utm11NCrs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
      ))
  }

  @Test
  def testL1CMultibandTileMask(): Unit = {
    val dilationDistance = 5
    val cloudPath = "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/QI_DATA/MSK_CLOUDS_B00.gml"
    val metadataPath = "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/MTD_TL.xml"

    class MockOpenSearch extends OpenSearchClient {
      override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
        val start = dateRange.get._1
        Seq(OpenSearchResponses.Feature(id="/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE",bbox.reproject(LatLng).extent,start, Array(
          Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B03.jp2"), Some("IMG_DATA_Band_10m_2_Tile1_Data")),
          Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B04.jp2"), Some("IMG_DATA_Band_10m_3_Tile1_Data")),
          //Link(URI.create("/data/MTDA/CGS_S2/CGS_S2_L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          Link(URI.create(metadataPath), Some("S2_Level-1C_Tile1_Metadata")),
          Link(URI.create(cloudPath), Some("FineCloudMask_Tile1_Data"))
          ),Some(10)))
      }
      override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???
      override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
    }

    val creoL1CLayerProvider = new FileLayerProvider(
      new MockOpenSearch,
      openSearchCollectionId = "Sentinel2",
      openSearchLinkTitles = NonEmptyList.of("IMG_DATA_Band_10m_1_Tile1_Data", "IMG_DATA_Band_10m_2_Tile1_Data", "IMG_DATA_Band_10m_3_Tile1_Data"),
      rootPath = "/eodata",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(tileSize = 256)
      )

    // val source = GDALCloudRasterSource(cloudPath, metadataPath, new GDALPath(""))
    // val mergedPolygon: MultiPolygon = MultiPolygon(source.getMergedPolygons(dilationDistance))

    val date = ZonedDateTime.parse("2021-01-01T00:00:00+00:00")
    val utm11NCrs = CRS.fromEpsgCode(32611)
    val boundingBox = ProjectedExtent(Extent(499980+25000,5200020-11000,499980+26000,5200020-10000), utm11NCrs)
    val dataCubeParameters = new DataCubeParameters

    // Create a reference tile without cloud masking.
    //    val layer: MultibandTileLayerRDD[SpaceTimeKey] = creoL1CLayerProvider.readMultibandTileLayer(
    //      from = date,
    //      to = date,
    //      boundingBox,
    //      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
    //      polygons_crs = utm11NCrs,
    //      zoom = 0,
    //      sc,
    //      Some(dataCubeParameters)
    //      )
    //    val spatialLayer = layer.toSpatial(date)
    //    spatialLayer.writeGeoTiff("test_L1C_default.tif", boundingBox)

    // Create the tile to be tested with the mask_l1c masking strategy.
    dataCubeParameters.maskingStrategyParameters = Map[String, Object](
      "method" -> "mask_l1c",
      "dilation_distance" -> dilationDistance.toString).asJava
    val maskedLayer: MultibandTileLayerRDD[SpaceTimeKey] = creoL1CLayerProvider.readMultibandTileLayer(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = utm11NCrs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
      )
    val spatialMaskedLayer = maskedLayer.toSpatial(date)
    spatialMaskedLayer.writeGeoTiff("test_L1C_tile_mask.tif", boundingBox)

    // Compare the two tiles.
    val referenceTile = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/l1c_mask_reference.tif").read().get
    val actualTile = GeoTiffRasterSource("test_L1C_tile_mask.tif").read().get
    // val cloudArea = referenceTile.extent.intersection(mergedPolygon).getArea
    // val cloudPercentage = cloudArea / referenceTile.extent.getArea
    // println("Cloud polygon covers " + cloudArea + " Sq meters of tile with " + referenceTile.extent.getArea + " Sq meters. (" + cloudPercentage*100 +"%)")
    println("Dimensions went from " + referenceTile.dimensions + " to " + actualTile.dimensions)
    var maskedCellCounts = Array[Int]()
    for (bandIndex <- 0 to 2) {
      val actualTileData = actualTile.tile.band(bandIndex).asInstanceOf[UShortConstantNoDataArrayTile].array
      val referenceTileData = referenceTile.tile.band(bandIndex).asInstanceOf[UShortConstantNoDataArrayTile].array
      val actualTileNoZeroCells = actualTileData.zipWithIndex.filter(_._1 != 0)
      val referenceTileNoZeroCells = referenceTileData.zipWithIndex.filter(_._1 != 0)
      // Note: filtering out raster regions can cause the actual tile to have fewer dimensions.
      assert(actualTile.dimensions.cols <= referenceTile.dimensions.cols)
      assert(actualTile.dimensions.rows <= referenceTile.dimensions.rows)
      // Ensure that some cells have been masked.
      //if (cloudArea != 0)
      assert(actualTileData.count(_ == 0) > referenceTileData.count(_ == 0))
      // Ensure that unmasked cells remain unchanged.
      assert(actualTileNoZeroCells.length == 0 || actualTileNoZeroCells.forall(referenceTileNoZeroCells.contains))
      // Ensure that the mask covers the same percentage of area as the cloud polygon. (If no raster regions were filtered out.)
      val maskedCellCount = actualTileData.count(_ == 0) - referenceTileData.count(_ == 0)
      maskedCellCounts = maskedCellCounts :+ maskedCellCount
      val maskedCellPercentage = (maskedCellCount.toDouble / referenceTileData.length.toDouble)
      //if (referenceTile.dimensions == actualTile.dimensions)
      //  assert((cloudPercentage - maskedCellPercentage).abs <= 0.01)
      println("Actual band " + bandIndex + " has " + actualTileData.count(_ == 0) + " zero cells (" + (actualTileData.count(
        _ == 0).toFloat / referenceTileData.length.toFloat) * 100 + "%)")
      println(
        maskedCellCount + " cells have been masked. (" + maskedCellPercentage * 100 + "%) (" + maskedCellCount * 100 + " Sq meters)")
    }
    // Ensure that all bands mask the same amount of cells.
    assert(maskedCellCounts.forall(_ == maskedCellCounts.head))
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

  private def tocLayerProviderUTM = LayerFixtures.sentinel2TocLayerProviderUTM

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
