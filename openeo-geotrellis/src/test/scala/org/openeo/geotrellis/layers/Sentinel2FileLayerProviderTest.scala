package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, LayoutScheme, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffReader, MultibandGeoTiff}
import geotrellis.raster.resample.Average
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.summary.polygonal.{PolygonalSummaryResult, Summary}
import geotrellis.raster.summary.types.MeanValue
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{CellSize, MultibandTile, PaddedTile, ShortUserDefinedNoDataCellType}
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.geotiff.{GTiffOptions, saveRDD}
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcessScriptBuilder, OpenEOProcesses}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, ConfigurableSpaceTimePartitioner, DataCubeParameters, ResampledTile}
import org.openeo.opensearch.OpenSearchResponses.{FeatureCollection, Link}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.sparklisteners.{BatchJobProgressListener, GetInfoSparkListener}

import java.net.URI
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time._
import java.util
import java.util.stream.Stream
import java.util.{Arrays, Collections}
import scala.io.{BufferedSource, Source}
import scala.jdk.CollectionConverters._

object Sentinel2FileLayerProviderTest {
  private val openSearchEndpoint = LayerFixtures.client
  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor

  // Methods with attributes get called in a non-intuitive order:
  // - BeforeAll
  // - ParameterizedTest
  // - AfterAll
  // - BeforeClass
  // - AfterClass
  //
  // This order feels arbitrary, so I made the code robust against order changes.

  private var _sc: Option[SparkContext] = None

  private def sc: SparkContext = {
    if (_sc.isEmpty) {
      println("Creating SparkContext")

      BatchJobMetadataTracker.setGlobalTracking(true)

      val sc = SparkUtils.createLocalSparkContext("local[1]",
        appName = Sentinel2FileLayerProviderTest.getClass.getName)
      _sc = Some(sc)
    }
    _sc.get
  }

  @BeforeAll
  def setUpSpark_BeforeClass(): Unit = sc

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = sc

  var gotAfterAll = false

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    gotAfterAll = true
    maybeStopSpark()
  }

  var gotAfterClass = false

  @AfterAll
  def tearDownSpark_AfterClass(): Unit = {
    gotAfterClass = true;
    maybeStopSpark()
  }

  def maybeStopSpark(): Unit = {
    if (gotAfterAll && gotAfterClass) {
      if (_sc.isDefined) {
        println("Stopping SparkContext...")
        BatchJobMetadataTracker.setGlobalTracking(false)
        _sc.get.stop()
        _sc = None
        println("Stopped SparkContext")
      }
    }
  }

  def maskingParams: Stream[Arguments] = Arrays.stream(Array(
    arguments(Collections.singletonMap("method", "mask_scl_dilation"),"https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/dilation_masked.tif"),
    arguments(Map("method"->"mask_scl_dilation","erosion_kernel_size"->3,"kernel1_size"->0).asJava.asInstanceOf[util.Map[String,Object]],"https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/masked_erosion.tif")
  ))

  def datacubeParams: Stream[Arguments] = Arrays.stream(Array(
    arguments(new DataCubeParameters(),10.asInstanceOf[Integer]),
    arguments({
      val p = new DataCubeParameters()
      p.resampleMethod = Average
      p.loadPerProduct = true
      p
    },11.asInstanceOf[Integer]
      )
  ))
}


class Sentinel2FileLayerProviderTest extends RasterMatchers {
  import Sentinel2FileLayerProviderTest._

  @BeforeEach
  def clearTracker(): Unit = {
    BatchJobMetadataTracker.clearGlobalTracker()
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def polygonalMultiplePolygon(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)

    val polygons = ShapeFileReader.readMultiPolygonFeatures(getClass.getResource("/org/openeo/geotrellis/layers/tile1_polygons.shp")).map(_.geom)
    val bbox = ProjectedExtent(polygons.extent,LatLng)
    val polygonArray = polygons.toArray

    //use lower zoom level to make test go faster
    val layer = layerProvider("org/openeo/geotrellis/polygonalMultiplePolygon_features.json", NonEmptyList.of("FAPAR_10M"), scheme = ZoomedLayoutScheme(WebMercator, 256)).readMultibandTileLayer( date, date.plusDays(1), bbox, polygons = polygonArray,polygons_crs = LatLng,zoom = 8, sc = sc,datacubeParams = Option.empty)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    val summary = spatialLayer.polygonalSummary(polygons.map{_.reproject(LatLng,layer.metadata.crs)}, MeanVisitor).collect()
    print(summary.size)
    val values: Array[Double] = summary.map(_.data.toOption.get(0).sum)
    val counts: Array[Long] = summary.map(_.data.toOption.get(0).count)
    val resultArray: Array[Double] = Array(15509.0,26313.0,220760.0,511556.0)
    val expectedCounts: Array[Long] = Array(349,489,3415,3738)
    assertArrayEquals(expectedCounts, counts.sorted)
    assertArrayEquals(resultArray, values.sorted,0.001)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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

    val qgisZonalStatisticsPluginResult = 48.9074868071421
    assertEquals(qgisZonalStatisticsPluginResult, value.mean, 0.1)
    val inputs = BatchJobMetadataTracker.tracker("").asDict().get("links")

    assertEquals(1,inputs.asInstanceOf[util.Map[String,util.List[String]]].get("urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2").size())
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def derivedFromDocument(): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    val layer = faparLayerProvider().readTileLayer(from = date, to = date, bbox, sc = sc)

    layer
      .toSpatial(date)
      .collect()

    val derivedFromDocuments = BatchJobMetadataTracker.tracker("").asDict()
      .get(BatchJobMetadataTracker.AUXILIARY_FILES)
      .asInstanceOf[util.List[BatchJobMetadataTracker.AuxiliaryFile]]

    assertEquals(1, derivedFromDocuments.size())
    assertEquals("application/geo+json", derivedFromDocuments.get(0).getMediaType)

    val itemCollection = GeoJson.fromFile[JsonFeatureCollection](derivedFromDocuments.get(0).getPath)
    assertTrue(itemCollection.getAllGeometries().nonEmpty)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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
  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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

  @Disabled("Can't query Terrascope with OscarsOpenSearchClient anymore")
  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Timeout(2000)
  @Test
  def loadMetadata(): Unit = {
    val fileLayerProvider = FileLayerProvider(
      openSearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = NonEmptyList.of("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution,
      pathDateExtractor)

    val Some((extent, dates)) = fileLayerProvider.loadMetadata(sc)

    assertEquals(WebMercator, extent.crs)

    assertTrue(dates.length > 1000)

    val uniqueYears = dates
      .map(_.getYear)
      .distinct

    assertTrue(uniqueYears contains 2015)
    assertTrue(uniqueYears contains 2020)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @ParameterizedTest
  @MethodSource(Array("datacubeParams"))
  def multibandWithSpacetimeMask(parameters: DataCubeParameters, expectedNBStages: Int, @TempDir tempDir: java.nio.file.Path): Unit = {

    val refPath = s"org/openeo/geotrellis/Sentinel2FileLayerProvider_multiband_reference_${parameters.resampleMethod.toString.toLowerCase}.tif"

    val date = ZonedDateTime.of(LocalDate.of(2024, 4, 22), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(1.90283, 50.9579, 1.97116, 51.0034), LatLng)

    var mask = layerProvider("org/openeo/geotrellis/multibandWithSpacetimeMask_features.json", NonEmptyList.of("SCENECLASSIFICATION_20M"), scheme = ZoomedLayoutScheme(WebMercator, 256)).readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val builder: OpenEOProcessScriptBuilder = new OpenEOProcessScriptBuilder
    val args: util.Map[String, AnyRef] = dummyMap("x", "y")
    builder.expressionStart("gte", args)
    builder.argumentStart("x")
    builder.argumentEnd()
    builder.constantArgument("y", 6)
    builder.expressionEnd("gte", args)
    mask.toSpatial(date).writeGeoTiff(tempDir.resolve(f"Sentinel2FileLayerProvider_multiband_SCL_${parameters.hashCode()}.tif"), bbox)
    val p = new OpenEOProcesses()
    mask = p.mapBands(mask, builder)
    mask.toSpatial(date).writeGeoTiff(tempDir.resolve(f"Sentinel2FileLayerProvider_multiband_mask_${parameters.hashCode()}.tif"), bbox)

    var layer = layerProvider("org/openeo/geotrellis/multibandWithSpacetimeMask_features2.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"), scheme = ZoomedLayoutScheme(WebMercator, 256)).readMultibandTileLayer(from = date, to = date, bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs, sc = sc,zoom = 13,datacubeParams = Option.empty)

    val originalCount = layer.count()
    parameters.maskingCube = Some(mask)

    val listener = new GetInfoSparkListener()
    SparkContext.getOrCreate().addSparkListener(listener)

    layer = layerProvider("org/openeo/geotrellis/multibandWithSpacetimeMask_features3.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"), scheme = ZoomedLayoutScheme(WebMercator, 256)).readMultibandTileLayer(from = date, to = date, bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs, sc = sc,zoom = 13,datacubeParams = Some(parameters))
    print(layer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index)
    assertTrue(layer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.isInstanceOf[ConfigurableSpaceTimePartitioner])
    val maskedCount = layer.count()
    SparkContext.getOrCreate().removeSparkListener(listener)
    assertTrue(Math.abs(expectedNBStages - listener.getStagesCompleted) <= 2)
    val spatialLayer = p.rasterMask(layer,mask,Double.NaN)
      .toSpatial(date)
      .cache()

    spatialLayer.writeGeoTiff(tempDir.resolve(f"Sentinel2FileLayerProvider_multiband_${parameters.hashCode()}.tif"), bbox)
    assertNotEquals(originalCount,maskedCount)

    val resultTiff = GeoTiff.readMultiband(tempDir.resolve(f"Sentinel2FileLayerProvider_multiband_${parameters.hashCode()}.tif"))

    val refFile = Thread.currentThread().getContextClassLoader.getResource(refPath)
    val refTiff = GeoTiff.readMultiband(refFile.getPath)


    withGeoTiffClue(resultTiff.raster, refTiff.raster, refTiff.crs)  {
      //TODO lower the threshold from this silly high value. It is so high because of nearest neighbour resampling, which causes this
      // Due to issue with resampling, we're temporarily stuck with it
      assertRastersEqual(refTiff.raster,resultTiff.raster,601)
    }

  }




  /**
   *  Simulate 'patch extraction' as performed by WorldCereal.
   *  This should be as efficiënt as possible, working in native projection.
   */
  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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
    val layer = layerProvider("org/openeo/geotrellis/testPatchExtract_features.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")).readMultibandTileLayer(from = start, to = end,bbox, Array(MultiPolygon(bbox.extent.toPolygon())),bbox.crs,zoom = 1, sc = sc, datacubeParams = Some(parameters))

    val localData = layer.collect()
    println(SizeEstimator.estimate(localData))
    println((System.currentTimeMillis()-time)/1000)
    println(localData.map(_._1.time).mkString(";"))
    assertEquals(43,localData.length)
    assertEquals(4,localData(0)._2.bandCount)
    assertFalse(localData(0)._2.band(0).isNoDataTile)
    assertEquals(ShortUserDefinedNoDataCellType(32767),localData(0)._2.band(1).cellType)
  }

  @Disabled("TODO: verify output")
  @Test
  def filterByAttributeValue(@TempDir tempDir: java.nio.file.Path): Unit = {
    val date = ZonedDateTime.of(LocalDate.of(2020, 4, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(4.399681091308594, 51.06869305078254, 4.446201324462891, 51.08233659233106), LatLng)

    val layer = faparLayerProvider(Map("tileId"-> "31UFS", "resolution" -> 10))
      .readMultibandTileLayer(from = date, to = date, bbox, sc = sc)

    val spatialLayer = layer
      .toSpatial(date)
      .cache()

    spatialLayer.writeGeoTiff(tempDir.resolve("Sentinel2FileLayerProvider_10_UFS.tif"), bbox)
  }

  @Disabled("TODO: verify output")
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

  def assertResampledLayerValid(crs: CRS, actualMean: Double): Unit = {
    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 640860+2560, 5676170+2560), CRS.fromEpsgCode(32631))
    val reprojectedBoundingBox = boundingBox.reproject(crs)
    val parameters = new DataCubeParameters
    parameters.noResampleOnRead = true

    val layer = LayerFixtures.sentinel2TocLayerProviderUTMMultiResolution.readMultibandTileLayer(
      from = date,
      to = date,
      ProjectedExtent(reprojectedBoundingBox, crs),
      polygons = Array(MultiPolygon(reprojectedBoundingBox.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(parameters)
    )
    val layerArray = layer.collect()

    // Ensure that ResampledTiles exist.
    layerArray.foreach({ case (_, tile) =>
      val tile20m = tile.band(1)
      tile20m match {
        case paddedTile: PaddedTile => assert(paddedTile.chunk.isInstanceOf[ResampledTile])
        case resTile: ResampledTile =>
          assertEquals(0.5, resTile.sourceCols.toDouble / resTile.targetCols.toDouble, 0.01)
          assertEquals(0.5, resTile.sourceRows.toDouble / resTile.targetRows.toDouble, 0.01)
        case _ => assert(false)
      }
    })

    // Check the mean of the resampled band in the layer.
    val spatialLayer = layer.toSpatial(date).cache()
    val polygon = boundingBox.reprojectAsPolygon(spatialLayer.metadata.crs)
    val summary = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor)
    val meanList = summary.toOption.get
    // Delta is large to simply ensure that the mean is reasonably valid.
    assertEquals(actualMean, meanList.apply(1).mean, actualMean * 0.1)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testS2ResampledTilesCRSEqualToRasterSource(): Unit = {
    // When feature.crs == targetExtent.crs
    // This case normally uses GeoTiffResampleRasterSources.
    assertResampledLayerValid(CRS.fromEpsgCode(32631), 9589.844968268359)
  }


  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testS2ResampledTilesCRSDiffersFromRasterSource(): Unit = {
    // When feature.crs != targetExtent.crs
    // This case normally uses GeoTiffReprojectRasterSources.
    assertResampledLayerValid(CRS.fromEpsgCode(32632), 9589.844968268359)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testReadDifferentProjection(@TempDir tempDir: java.nio.file.Path):Unit = {

    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 666460, 5701770), crs)
    val utm32 = CRS.fromEpsgCode(32632)
    val bboxUTM32 = boundingBox.reproject(utm32)

    val dataCubeParameters = new DataCubeParameters

    val layer = layerProvider("org/openeo/geotrellis/testReadDifferentProjection_features.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")).readMultibandTileLayer(
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

    val output_path = tempDir.resolve("utm32.tif")
    val options = new GTiffOptions()
    options.overviews = "ALL"
    saveRDD(spatialLayer,-1,output_path,cropBounds=Some(reprojectedBoundingBox),formatOptions=options)


    val stitched: MultibandGeoTiff = GeoTiffReader.readMultiband(output_path)
    assertFalse(stitched.tile.band(0).isNoDataTile)
    assertEquals(utm32,spatialLayer.metadata.crs)
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @ParameterizedTest
  @MethodSource(Array("maskingParams"))
  def testMaskSclDilationOnS2TileEdge(params:util.Map[String,Object],ref:String, @TempDir tempDir: java.nio.file.Path): Unit = {
    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)
    val crs = CRS.fromEpsgCode(32631)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 666460, 5701770), crs)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.maskingStrategyParameters = params

    val layer = layerProvider("org/openeo/geotrellis/testMaskSclDilationOnS2TileEdge_features.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")).readMultibandTileLayer(
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

    val actual = tempDir.resolve("masked.tif")
    spatialLayer.sparseStitch(reprojectedBoundingBox) match {
      case Some(stitched) => MultibandGeoTiff(stitched.crop(reprojectedBoundingBox), spatialLayer.metadata.crs).write(actual)
      case _ => throw new IllegalStateException("nothing to sparse-stitch")
    }

    val referenceTile = GeoTiffRasterSource(ref).read().get
    val actualTile = GeoTiffRasterSource(actual.toString).read().get
    assertRastersEqual(referenceTile,actualTile,160.0)
  }

  /**
   * Test simulates the very common case where an 'scl dilation mask' is applied at load time.
   */
  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
  @Test
  def testToSclDilationMaskOnS2TileEdge(@TempDir tempDir: java.nio.file.Path): Unit = {
    val ref = "https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/toscldilationmask_masked_ref.tif"
    val actual = tempDir.resolve("toscldilationmask_masked_actual.tif")

    // Create spatialLayer.
    val date = LocalDate.of(2019, 3, 7).atStartOfDay(UTC)
    val crs = CRS.fromEpsgCode(32631)
    val boundingBox = ProjectedExtent(Extent(640860, 5676170, 666460, 5701770), crs)
    val dataCubeParameters = new DataCubeParameters
    val ProgressListener = new BatchJobProgressListener()
    sc.addSparkListener(ProgressListener)
    val listener = new GetInfoSparkListener()
    SparkContext.getOrCreate().addSparkListener(listener)
    // dataCubeParameters.tileSize = 2048 (This requires increased spark.kryoserializer.buffer.max)
    val sclCube = layerProvider("org/openeo/geotrellis/testToSclDilationMaskOnS2TileEdge_features.json", NonEmptyList.of("SCENECLASSIFICATION_20M")).readMultibandTileLayer(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    // Create mask.

    val mask1Values = util.Arrays.asList(2, 4, 5, 6, 7)
    val mask2Values = util.Arrays.asList(3, 8, 9, 10, 11)
    val erosionKernelSize = 0
    val kernel1Size = 17
    val kernel2Size = 201
    val mask: MultibandTileLayerRDD[SpaceTimeKey] = new OpenEOProcesses().toSclDilationMask(sclCube, erosionKernelSize, mask1Values, mask2Values, kernel1Size, kernel2Size)

    dataCubeParameters.setMaskingCube(mask)

    val rgbCube = layerProvider("org/openeo/geotrellis/testToSclDilationMaskOnS2TileEdge_features_2.json", NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M")).readMultibandTileLayer(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )
    val maskedCube: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = rgbCube.toSpatial(date)

    // Apply Mask.
    val reprojectedBoundingBox = boundingBox.reproject(maskedCube.metadata.crs)

    // Compare results.
    maskedCube.sparseStitch(reprojectedBoundingBox) match {
      case Some(stitched) => MultibandGeoTiff(stitched.crop(reprojectedBoundingBox), maskedCube.metadata.crs).write(actual)
      case _ => throw new IllegalStateException("nothing to sparse-stitch")
    }
    SparkContext.getOrCreate().removeSparkListener(listener)

    listener.printStatus()

    val referenceTile = GeoTiffRasterSource(ref).read().get
    val actualTile = GeoTiffRasterSource(actual.toString).read().get
    assertRastersEqual(referenceTile, actualTile, 160.0)
    //because debug logging is enabled during tests, it actually runs more jobs and stages than done in production
    assertEquals(5, listener.getJobsCompleted, "unexpected number of jobs")
    assertEquals(18, listener.getStagesCompleted, "unexpected number of stages")

  }

  @Test
  def testMaskL1CRasterSourceFiltering(): Unit = {
    object MockOpenSearch extends OpenSearchClient with IdentityEquals {
      override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
        val start = dateRange.get._1
        Seq(OpenSearchResponses.Feature(id="/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE",bbox.extent,start, Array(
          Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          //Link(URI.create("/data/MTDA/CGS_S2/CGS_S2_L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
          Link(URI.create("https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/MTD_TL.xml"), Some("S2_Level-1C_Tile1_Metadata")),
          Link(URI.create("https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/QI_DATA/MSK_CLOUDS_B00.gml"), Some("FineCloudMask_Tile1_Data"))
          ),Some(10)))
      }
      override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???
      override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
    }

    val creoL1CLayerProvider = FileLayerProvider(
      MockOpenSearch,
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

  val cloudPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/QI_DATA/MSK_CLOUDS_B00.gml"
  val metadataPath = "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/MTD_TL.xml"

  object MockOpenSearch extends OpenSearchClient with IdentityEquals {
    override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
      val start = dateRange.get._1
      Seq(OpenSearchResponses.Feature(id = "/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2A_MSIL1C_20210101T075331_N0209_R135_T35JPM_20210101T100240.SAFE", bbox.reproject(LatLng).extent, start, Array(
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B03.jp2"), Some("IMG_DATA_Band_10m_2_Tile1_Data")),
        Link(URI.create("/vsicurl/https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B04.jp2"), Some("IMG_DATA_Band_10m_3_Tile1_Data")),
        //Link(URI.create("/data/MTDA/CGS_S2/CGS_S2_L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2"), Some("IMG_DATA_Band_10m_1_Tile1_Data")),
        Link(URI.create(metadataPath), Some("S2_Level-1C_Tile1_Metadata")),
        Link(URI.create(cloudPath), Some("FineCloudMask_Tile1_Data"))
      ), Some(10)))
    }

    override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???

    override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  }


  @Test
  @Disabled("Covered by faster integration test now: https://git.vito.be/projects/TPT/repos/os_creodias_openeo_k8s/commits/538ebf0a7995d582a5429a11237b951d8838d36f")
  def testL1CResolutionResample(): Unit = {
    val creoL1CLayerProvider = FileLayerProvider(
      MockOpenSearch,
      openSearchCollectionId = "Sentinel2",
      openSearchLinkTitles = NonEmptyList.of(
        "IMG_DATA_Band_10m_1_Tile1_Data", "IMG_DATA_Band_10m_2_Tile1_Data",
        "IMG_DATA_Band_10m_3_Tile1_Data", "S2_Level-1C_Tile1_Metadata",
      ),
      rootPath = "/eodata",
      CellSize(30, 30), // maxSpatialResolution
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(tileSize = 256)
    )

    val date = ZonedDateTime.parse("2021-01-01T00:00:00+00:00")
    val utm11NCrs = CRS.fromEpsgCode(32611)
    val boundingBox = ProjectedExtent(Extent(499980 + 25000, 5200020 - 11000, 499980 + 26000, 5200020 - 10000), utm11NCrs)
    val dataCubeParameters = new DataCubeParameters

    // Create the tile to be tested with the mask_l1c masking strategy.
    dataCubeParameters.maskingStrategyParameters = Map[String, Object](
      "method" -> "mask_l1c",
    ).asJava
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
  }

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasGdalInstalled")
  @Test
  def testL1CMultibandTileMask(@TempDir tempDir: java.nio.file.Path): Unit = {
    val listener = new BatchJobProgressListener()
    sc.addSparkListener(listener)
    val dilationDistance = 5

    val creoL1CLayerProvider = FileLayerProvider(
      MockOpenSearch,
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
    spatialMaskedLayer.writeGeoTiff(tempDir.resolve("test_L1C_tile_mask.tif"), boundingBox)

    // Compare the two tiles.
    val referenceTile = GeoTiffRasterSource("https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/l1c_mask_reference.tif").read().get
    val actualTile = GeoTiffRasterSource(tempDir.resolve("test_L1C_tile_mask.tif").toString).read().get
    // val cloudArea = referenceTile.extent.intersection(mergedPolygon).getArea
    // val cloudPercentage = cloudArea / referenceTile.extent.getArea
    // println("Cloud polygon covers " + cloudArea + " Sq meters of tile with " + referenceTile.extent.getArea + " Sq meters. (" + cloudPercentage*100 +"%)")
    println("Dimensions went from " + referenceTile.dimensions + " to " + actualTile.dimensions)
    var maskedCellCounts = Array[Int]()
    for (bandIndex <- 0 to 2) {
      val actualTileData = actualTile.tile.band(bandIndex).toArray()
      val referenceTileData = referenceTile.tile.band(bandIndex).toArray()
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


  private def layerProvider(featuresJsonResourcePath: String, bandNames: NonEmptyList[String], attributeValues: Map[String, Any] = Map("resolution" -> 10 /* exclude 20m features like in layercatalog.json */), scheme: LayoutScheme = FloatingLayoutScheme(256)) = {
    val client = new FixedFeaturesOpenSearchClient
    val source: BufferedSource = Source.fromResource(featuresJsonResourcePath)
    val features = FeatureCollection.parse(
      source.getLines().mkString("")).features
    features.foreach(feature => client.addFeature(feature))
    FileLayerProvider(
      client,
      openSearchCollectionId = "???",
      openSearchLinkTitles = bandNames,
      rootPath = "???",
      maxSpatialResolution,
      pathDateExtractor,
      attributeValues,
      layoutScheme = scheme
    )
  }


  private def faparLayerProvider(attributeValues: Map[String, Any] = Map("resolution" -> 10 /* exclude 20m features like in layercatalog.json */)) = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2:S2B_20200405T104619_31UDS_FAPAR_10M_V210",
        |            "geometry": {"type":"Polygon","coordinates":[[[1.5885551,50.5261385],[1.5907216,50.4552653],[3.1375125,50.4637172],[3.140458,51.4510981],[1.9612252,51.444565],[1.9365656,51.3849571],[1.8766533,51.2391211],[1.8170155,51.093279],[1.7577979,50.9474451],[1.6990549,50.8015836],[1.6405846,50.6556649],[1.5885551,50.5261385]]]},
        |            "bbox": [1.5885551,50.4552653,3.140458,51.4510981],
        |            "properties":
        |            	{"date":"2020-04-05T10:46:19.024Z","updated":"2024-05-18T17:47:54.738Z","available":"2024-05-18T17:47:58Z","published":"2024-05-18T17:47:58Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2","title":"S2B_20200405T104619_31UDS_FAPAR_10M_V210","identifier":"urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2:S2B_20200405T104619_31UDS_FAPAR_10M_V210","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"DESCENDING","orbitNumber":16093,"relativeOrbitNumber":51,"beginningDateTime":"2020-04-05T10:46:19.024Z","endingDateTime":"2020-04-05T10:46:19.024Z","tileId":"31UDS"}}],"additionalAttributes":{"resolution":10},"productInformation":{"cloudCover":0.009,"productType":"FAPAR","availabilityTime":"2024-05-18T17:47:58Z","productVersion":"V210","processingCenter":"VITO","processingDate":"2024-05-18T17:47:54.738Z"},"links":{"previews":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2020/04/05/S2B_20200405T104619_31UDS_FAPAR_V210/10M/S2B_20200405T104619_31UDS_FAPAR_QUICKLOOK_V210.tif","type":"image/tiff","length":267379,"category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_FAPAR&TIME=2020-04-05&BBOX=176837.14482905777,6525496.291736421,349594.1854176624,6701479.020766405&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2020/04/05/S2B_20200405T104619_31UDS_FAPAR_V210/10M/S2B_20200405T104619_31UDS_FAPAR_10M_V210.xml","type":"application/vnd.iso.19139+xml","length":32544,"title":"Inspire metadata"}],"related":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2020/04/05/S2B_20200405T104619_31UDS_FAPAR_V210/10M/S2B_20200405T104619_31UDS_SCENECLASSIFICATION_20M_V210.tif","type":"image/tiff","length":2750970,"title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"}],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2020/04/05/S2B_20200405T104619_31UDS_FAPAR_V210/10M/S2B_20200405T104619_31UDS_FAPAR_10M_V210.tif","type":"image/tiff","length":66354428,"title":"FAPAR_10M","bandNames":["FAPAR_10M"]}]}}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",
      openSearchLinkTitles = NonEmptyList.of("FAPAR_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2",
      maxSpatialResolution,
      pathDateExtractor,
      attributeValues
    )
  }

  private def tocLayerProvider = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20200405T104619_31UDS_TOC_V210",
        |            "geometry": {"type":"Polygon","coordinates":[[[1.5885551,50.5261385],[1.5907216,50.4552653],[3.1375125,50.4637172],[3.140458,51.4510981],[1.9612252,51.444565],[1.9365656,51.3849571],[1.8766533,51.2391211],[1.8170155,51.093279],[1.7577979,50.9474451],[1.6990549,50.8015836],[1.6405846,50.6556649],[1.5885551,50.5261385]]]},
        |            "bbox": [1.5885551,50.4552653,3.140458,51.4510981],
        |            "properties":
        |            	{"date":"2020-04-05T10:46:19.024Z","updated":"2024-05-18T17:47:54.738Z","available":"2024-05-18T17:47:56Z","published":"2024-05-18T17:47:56Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","title":"S2B_20200405T104619_31UDS_TOC_V210","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20200405T104619_31UDS_TOC_V210","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"DESCENDING","orbitNumber":16093,"relativeOrbitNumber":51,"beginningDateTime":"2020-04-05T10:46:19.024Z","endingDateTime":"2020-04-05T10:46:19.024Z","tileId":"31UDS"}}],"productInformation":{"cloudCover":0.009,"productType":"TOC","availabilityTime":"2024-05-18T17:47:56Z","productVersion":"V210","processingCenter":"VITO","processingDate":"2024-05-18T17:47:54.738Z"},"links":{"previews":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC_QUICKLOOK_V210.tif","type":"image/tiff","length":916241,"category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2020-04-05&BBOX=176837.14482905777,6525496.291736421,349594.1854176624,6701479.020766405&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC_V210.xml","type":"application/vnd.iso.19139+xml","length":39917,"title":"Inspire metadata"}],"related":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_AOT_60M_V210.tif","type":"image/tiff","length":126778,"title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_RAA_60M_V210.tif","type":"image/tiff","length":446816,"title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_SCENECLASSIFICATION_20M_V210.tif","type":"image/tiff","length":2750970,"title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_SZA_60M_V210.tif","type":"image/tiff","length":96322,"title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_VZA_60M_V210.tif","type":"image/tiff","length":236833,"title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_WVP_60M_V210.tif","type":"image/tiff","length":4372298,"title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B01_60M_V210.tif","type":"image/tiff","length":4145578,"title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B02_10M_V210.tif","type":"image/tiff","length":150087700,"title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B03_10M_V210.tif","type":"image/tiff","length":150248378,"title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B04_10M_V210.tif","type":"image/tiff","length":151146309,"title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B05_20M_V210.tif","type":"image/tiff","length":39667111,"title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B06_20M_V210.tif","type":"image/tiff","length":40032943,"title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B07_20M_V210.tif","type":"image/tiff","length":40619597,"title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B08_10M_V210.tif","type":"image/tiff","length":147847722,"title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B11_20M_V210.tif","type":"image/tiff","length":38187925,"title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B12_20M_V210.tif","type":"image/tiff","length":38712077,"title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B8A_20M_V210.tif","type":"image/tiff","length":40492355,"title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}]}}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

      FileLayerProvider(
        client,
        openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
        openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
        rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
        maxSpatialResolution,
        pathDateExtractor
      )
  }

  private def tocLayerProviderUTM = LayerFixtures.sentinel2TocLayerProviderUTM

  private def sceneclassificationLayerProviderUTM = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UDS_TOC_V200",
        |            "geometry": {"coordinates":[[[1.5885764,50.5254416],[1.5907216,50.4552653],[3.1375125,50.4637172],[3.140458,51.4510981],[1.9627111,51.4445733],[1.9099861,51.3170677],[1.8499932,51.1712046],[1.7902599,51.0253139],[1.7312378,50.8793156],[1.6721822,50.7334027],[1.6134054,50.5875269],[1.5885764,50.5254416]]],"type":"Polygon"},
        |            "bbox": [1.5885764,50.4552653,3.140458,51.4510981],
        |            "properties":
        |            	{"date":"2018-08-14T10:50:19.024Z","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UDS_TOC_V200","available":"2021-09-20T10:38:29Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","productInformation":{"processingCenter":"VITO","productVersion":"V200","processingDate":"2020-04-12T21:03:47.889Z","cloudCover":10.5464,"productType":"TOC","availabilityTime":"2021-09-20T10:38:29Z"},"links":{"related":[{"length":249228,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_VZA_60M_V200.tif","type":"image/tiff","title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"length":3332760,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_WVP_60M_V200.tif","type":"image/tiff","title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"},{"length":4440737,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_SCENECLASSIFICATION_20M_V200.tif","type":"image/tiff","title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"length":113477,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_SZA_60M_V200.tif","type":"image/tiff","title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"length":936225,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_RAA_60M_V200.tif","type":"image/tiff","title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"length":78194,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_AOT_60M_V200.tif","type":"image/tiff","title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"}],"data":[{"length":40847983,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B11_20M_V200.tif","type":"image/tiff","title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"length":155758791,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B03_10M_V200.tif","type":"image/tiff","title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"length":41609069,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B06_20M_V200.tif","type":"image/tiff","title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"length":4534100,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B01_60M_V200.tif","type":"image/tiff","title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"length":155450613,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B08_10M_V200.tif","type":"image/tiff","title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"length":42066708,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B07_20M_V200.tif","type":"image/tiff","title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"length":41135283,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B05_20M_V200.tif","type":"image/tiff","title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"length":40468740,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B12_20M_V200.tif","type":"image/tiff","title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"length":155043298,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B02_10M_V200.tif","type":"image/tiff","title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"length":155976620,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B04_10M_V200.tif","type":"image/tiff","title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"length":42026477,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B8A_20M_V200.tif","type":"image/tiff","title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}],"previews":[{"length":912402,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC_QUICKLOOK_V200.tif","type":"image/tiff","category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2018-08-14&BBOX=176839.5159342117,6525496.291736421,349594.1854176624,6701479.020766405&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":39916,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC_V200.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2021-09-20T10:38:29Z","title":"S2B_20180814T105019_31UDS_TOC_V200","updated":"2020-04-12T21:03:47.889Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"31UDS","relativeOrbitNumber":51,"beginningDateTime":"2018-08-14T10:50:19.024Z","orbitDirection":"DESCENDING","endingDateTime":"2018-08-14T10:50:19.024Z","orbitNumber":7513},"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UES_TOC_V200",
        |            "geometry": {"coordinates":[[[2.9997122,51.4511822],[2.9997182,50.4637984],[4.5464364,50.4535233],[4.579544,51.4405412],[2.9997122,51.4511822]]],"type":"Polygon"},
        |            "bbox": [2.9997122,50.4535233,4.579544,51.4511822],
        |            "properties":
        |            	{"date":"2018-08-14T10:50:19.024Z","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UES_TOC_V200","available":"2022-09-13T09:59:12Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","productInformation":{"processingCenter":"VITO","productVersion":"V200","processingDate":"2022-09-13T09:59:05.729Z","cloudCover":75.648,"productType":"TOC","availabilityTime":"2022-09-13T09:59:12Z"},"links":{"related":[{"length":86676,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_AOT_60M_V200.tif","type":"image/tiff","title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"length":2674487,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_RAA_60M_V200.tif","type":"image/tiff","title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"length":3797956,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_SCENECLASSIFICATION_20M_V200.tif","type":"image/tiff","title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"length":114641,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_SZA_60M_V200.tif","type":"image/tiff","title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"length":310979,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_VZA_60M_V200.tif","type":"image/tiff","title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"length":1812748,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_WVP_60M_V200.tif","type":"image/tiff","title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"length":2624395,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B01_60M_V200.tif","type":"image/tiff","title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"length":84102224,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B02_10M_V200.tif","type":"image/tiff","title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"length":83858459,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B03_10M_V200.tif","type":"image/tiff","title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"length":84269495,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B04_10M_V200.tif","type":"image/tiff","title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"length":22852785,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B05_20M_V200.tif","type":"image/tiff","title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"length":23112823,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B06_20M_V200.tif","type":"image/tiff","title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"length":23218244,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B07_20M_V200.tif","type":"image/tiff","title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"length":83671010,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B08_10M_V200.tif","type":"image/tiff","title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"length":22771960,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B11_20M_V200.tif","type":"image/tiff","title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"length":22582981,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B12_20M_V200.tif","type":"image/tiff","title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"length":23196877,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B8A_20M_V200.tif","type":"image/tiff","title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}],"previews":[{"length":437424,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC_QUICKLOOK_V200.tif","type":"image/tiff","category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2018-08-14&BBOX=333926.4346303704,6525191.719840584,509792.5061453912,6701494.043620578&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":39916,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC_V200.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2022-09-13T09:59:12Z","title":"S2B_20180814T105019_31UES_TOC_V200","updated":"2022-09-13T09:59:05.729Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"31UES","relativeOrbitNumber":51,"beginningDateTime":"2018-08-14T10:50:19.024Z","orbitDirection":"DESCENDING","endingDateTime":"2018-08-14T10:50:19.024Z","orbitNumber":7513},"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"}}],"status":"ARCHIVED"}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256))
  }


  private def sceneclassificationLayerProvider = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
    """{
      |    "features": [
      |        {
      |            "type": "Feature",
      |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UDS_TOC_V200",
      |            "geometry": {"coordinates":[[[1.5885764,50.5254416],[1.5907216,50.4552653],[3.1375125,50.4637172],[3.140458,51.4510981],[1.9627111,51.4445733],[1.9099861,51.3170677],[1.8499932,51.1712046],[1.7902599,51.0253139],[1.7312378,50.8793156],[1.6721822,50.7334027],[1.6134054,50.5875269],[1.5885764,50.5254416]]],"type":"Polygon"},
      |            "bbox": [1.5885764,50.4552653,3.140458,51.4510981],
      |            "properties":
      |            	{"date":"2018-08-14T10:50:19.024Z","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UDS_TOC_V200","available":"2021-09-20T10:38:29Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","productInformation":{"processingCenter":"VITO","productVersion":"V200","processingDate":"2020-04-12T21:03:47.889Z","cloudCover":10.5464,"productType":"TOC","availabilityTime":"2021-09-20T10:38:29Z"},"links":{"related":[{"length":249228,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_VZA_60M_V200.tif","type":"image/tiff","title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"length":3332760,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_WVP_60M_V200.tif","type":"image/tiff","title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"},{"length":4440737,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_SCENECLASSIFICATION_20M_V200.tif","type":"image/tiff","title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"length":113477,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_SZA_60M_V200.tif","type":"image/tiff","title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"length":936225,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_RAA_60M_V200.tif","type":"image/tiff","title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"length":78194,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_AOT_60M_V200.tif","type":"image/tiff","title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"}],"data":[{"length":40847983,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B11_20M_V200.tif","type":"image/tiff","title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"length":155758791,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B03_10M_V200.tif","type":"image/tiff","title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"length":41609069,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B06_20M_V200.tif","type":"image/tiff","title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"length":4534100,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B01_60M_V200.tif","type":"image/tiff","title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"length":155450613,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B08_10M_V200.tif","type":"image/tiff","title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"length":42066708,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B07_20M_V200.tif","type":"image/tiff","title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"length":41135283,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B05_20M_V200.tif","type":"image/tiff","title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"length":40468740,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B12_20M_V200.tif","type":"image/tiff","title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"length":155043298,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B02_10M_V200.tif","type":"image/tiff","title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"length":155976620,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B04_10M_V200.tif","type":"image/tiff","title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"length":42026477,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC-B8A_20M_V200.tif","type":"image/tiff","title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}],"previews":[{"length":912402,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC_QUICKLOOK_V200.tif","type":"image/tiff","category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2018-08-14&BBOX=176839.5159342117,6525496.291736421,349594.1854176624,6701479.020766405&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":39916,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UDS_TOC_V200/S2B_20180814T105019_31UDS_TOC_V200.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2021-09-20T10:38:29Z","title":"S2B_20180814T105019_31UDS_TOC_V200","updated":"2020-04-12T21:03:47.889Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"31UDS","relativeOrbitNumber":51,"beginningDateTime":"2018-08-14T10:50:19.024Z","orbitDirection":"DESCENDING","endingDateTime":"2018-08-14T10:50:19.024Z","orbitNumber":7513},"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"}}],"status":"ARCHIVED"}
      |         }
      |        ,{
      |            "type": "Feature",
      |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UES_TOC_V200",
      |            "geometry": {"coordinates":[[[2.9997122,51.4511822],[2.9997182,50.4637984],[4.5464364,50.4535233],[4.579544,51.4405412],[2.9997122,51.4511822]]],"type":"Polygon"},
      |            "bbox": [2.9997122,50.4535233,4.579544,51.4511822],
      |            "properties":
      |            	{"date":"2018-08-14T10:50:19.024Z","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20180814T105019_31UES_TOC_V200","available":"2022-09-13T09:59:12Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","productInformation":{"processingCenter":"VITO","productVersion":"V200","processingDate":"2022-09-13T09:59:05.729Z","cloudCover":75.648,"productType":"TOC","availabilityTime":"2022-09-13T09:59:12Z"},"links":{"related":[{"length":86676,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_AOT_60M_V200.tif","type":"image/tiff","title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"length":2674487,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_RAA_60M_V200.tif","type":"image/tiff","title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"length":3797956,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_SCENECLASSIFICATION_20M_V200.tif","type":"image/tiff","title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"length":114641,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_SZA_60M_V200.tif","type":"image/tiff","title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"length":310979,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_VZA_60M_V200.tif","type":"image/tiff","title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"length":1812748,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_WVP_60M_V200.tif","type":"image/tiff","title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"length":2624395,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B01_60M_V200.tif","type":"image/tiff","title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"length":84102224,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B02_10M_V200.tif","type":"image/tiff","title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"length":83858459,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B03_10M_V200.tif","type":"image/tiff","title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"length":84269495,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B04_10M_V200.tif","type":"image/tiff","title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"length":22852785,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B05_20M_V200.tif","type":"image/tiff","title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"length":23112823,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B06_20M_V200.tif","type":"image/tiff","title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"length":23218244,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B07_20M_V200.tif","type":"image/tiff","title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"length":83671010,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B08_10M_V200.tif","type":"image/tiff","title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"length":22771960,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B11_20M_V200.tif","type":"image/tiff","title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"length":22582981,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B12_20M_V200.tif","type":"image/tiff","title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"length":23196877,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC-B8A_20M_V200.tif","type":"image/tiff","title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}],"previews":[{"length":437424,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC_QUICKLOOK_V200.tif","type":"image/tiff","category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2018-08-14&BBOX=333926.4346303704,6525191.719840584,509792.5061453912,6701494.043620578&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":39916,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2018/08/14/S2B_20180814T105019_31UES_TOC_V200/S2B_20180814T105019_31UES_TOC_V200.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2022-09-13T09:59:12Z","title":"S2B_20180814T105019_31UES_TOC_V200","updated":"2022-09-13T09:59:05.729Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"31UES","relativeOrbitNumber":51,"beginningDateTime":"2018-08-14T10:50:19.024Z","orbitDirection":"DESCENDING","endingDateTime":"2018-08-14T10:50:19.024Z","orbitNumber":7513},"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"}}],"status":"ARCHIVED"}
      |         }
      |    ]
      |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

    FileLayerProvider(
      client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor
    )
  }
}
