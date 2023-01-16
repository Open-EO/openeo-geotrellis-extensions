package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff._
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ArrayMultibandTile, ByteConstantTile, DoubleArrayTile, FloatConstantTile, GridBounds, IntConstantNoDataCellType, MultibandTile, Raster, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.AggregateSpatialTest.{assertEqualTimeseriesStats, parseCSV}
import org.openeo.geotrellis.aggregate_polygon.intern.splitOverlappingPolygons
import org.openeo.geotrellis.aggregate_polygon.{AggregatePolygonProcess, SparkAggregateScriptBuilder}
import org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory
import org.openeo.geotrellis.geotiff.{ContextSeq, saveRDD, saveRDDTileGrid}
import org.openeo.geotrellisaccumulo.PyramidFactory

import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.JavaConverters._

object OpenEOProcessesSpec {
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
      val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().setMaster("local[2]") //.set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
      //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf))
    }
    _sc.get
  }

  @BeforeClass
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

  @AfterClass
  def tearDownSpark_AfterClass(): Unit = {
    gotAfterClass = true;
    maybeStopSpark()
  }

  def maybeStopSpark(): Unit = {
    if (gotAfterAll && gotAfterClass) {
      if (_sc.isDefined) {
        _sc.get.stop()
        _sc = None
      }
    }
  }

  def getPixel(layer:MultibandTileLayerRDD[SpaceTimeKey]): Array[Int] = {
    new OpenEOProcesses().filterEmptyTile(layer).groupBy(_._1).mapValues(values => {
      val raster: Raster[MultibandTile] = ContextSeq(values.map(v => (v._1.spatialKey, v._2)).seq, layer.metadata).stitch()
      raster.tile.band(0).get(0,0)
    }).collect().sortBy(_._1.instant).map(_._2)
  }

}

class OpenEOProcessesSpec extends RasterMatchers {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  private lazy val accumuloPyramidFactory = {new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")}
  private val pyramidFactory = new Sentinel2RadiometryPyramidFactory

  private def dataCube(minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid = pyramidFactory.pyramid_seq(bbox, srs, minDateString, maxDateString, java.util.Arrays.asList(1, 2))
    System.out.println("pyramid = " + pyramid)

    val pyramidAsMap = pyramid.toMap
    val maxZoom = pyramidAsMap.keys.max
    val datacube = pyramidAsMap.get(maxZoom).get
    datacube
  }

  private def accumuloDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid = accumuloPyramidFactory.pyramid_seq(layer,bbox, srs, minDateString, maxDateString)
    System.out.println("pyramid = " + pyramid)

    val pyramidAsMap = pyramid.toMap
    val maxZoom = pyramidAsMap.keys.max
    val datacube = pyramidAsMap.get(maxZoom).get
    datacube
  }

  /**
    * Test created in the frame of:
    * https://github.com/locationtech/geotrellis/issues/3168
    */
  @Test
  def applyMask() = {
    val date = "2018-05-06T00:00:00Z"

    val extent = Extent(3.4, 51.0, 3.5, 51.05)
    val datacube= dataCube( date, date, extent, "EPSG:4326")

    val selectedBands = datacube.withContext(_.mapValues(_.subsetBands(1)))

    val mask = accumuloDataCube("S2_SCENECLASSIFICATION_PYRAMID_20200407", date, date, extent, "EPSG:4326")
    val binaryMask = mask.withContext(_.mapValues( _.map(0)(pixel => if ( pixel == 5) 0 else 1)))

    print(binaryMask.partitioner)

    val maskedCube: MultibandTileLayerRDD[SpaceTimeKey] = new OpenEOProcesses().rasterMask(selectedBands, binaryMask, Double.NaN)
    val stitched = maskedCube.toSpatial().stitch()

    MultibandGeoTiff(stitched, maskedCube.metadata.crs).write("applyMask.tif")
    print(stitched)
  }

  @Test
  def applyMask_spacetime_spatial() = {
    val date = "2018-05-06T00:00:00Z"

    val extent = Extent(3.4, 51.0, 3.5, 51.05)

    val selectedBands = LayerFixtures.sentinel2B04Layer

    val maskTile = new ByteConstantTile(0.toByte, 256, 256).mutable
    maskTile.set(0, 0, 0)
    maskTile.set(0, 1, 1)
    maskTile.set(0, 2, 1)

    val mask = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new Raster(new ArrayMultibandTile(Array[Tile](maskTile)),selectedBands.metadata.extent), selectedBands.metadata.tileLayout,selectedBands.metadata.crs)
                  .withContext(_.mapValues(t => MultibandTile(maskTile)))

    val maskedCube: MultibandTileLayerRDD[SpaceTimeKey] = new OpenEOProcesses().rasterMask_spacetime_spatial(selectedBands, mask, 123)
    val tiles = maskedCube.collectAsMap()

    import scala.collection.JavaConversions._
    for (tileEntry <- tiles.entrySet) {
      val tile = tileEntry.getValue.band(0)

      //get method applies a conversion to int, also nodata is converted
      val value = tile.get(0, 1)
      assertTrue(123 == value || IntConstantNoDataCellType.noDataValue == value)
      val value2 = tile.get(0, 2)
      assertTrue(123 == value || IntConstantNoDataCellType.noDataValue == value)

    }


  }


  @Test
  def applyMaskFFT(): Unit = {
    val tile: Tile = DoubleArrayTile.fill(1.0,1280, 1280)
    val tileSize = 256
    val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new ArrayMultibandTile(Array[Tile](tile)), new TileLayout(1 + tile.cols / tileSize, 1 + tile.rows / tileSize, tileSize, tileSize))
    val kernel: Tile = DoubleArrayTile.fill(1.0,61, 61)

    val resultCube = new OpenEOProcesses().apply_kernel_spatial(datacube, kernel)

    val theResultTile = time{ resultCube.stitch().tile.band(0) }
    val expectedConvolution = time{Convolve.apply(tile, new Kernel(kernel), Option.empty, TargetCell.All)}
    assertEqual(expectedConvolution,theResultTile)
  }

  @Test
  def testGroupAndMaskByGeometry(): Unit = {
    val processes = new OpenEOProcesses

    val dates = Seq("2017-01-15T00:00:00Z","2017-01-16T00:00:00Z")
    val bands = List.fill(2)(new FloatConstantTile(100, 512, 512).asInstanceOf[Tile])

    val arrayMultibandTile = new ArrayMultibandTile(bands.toArray)
    // Chop each large tile in bands into a 16x16 array (with 32x32 pixels each).
    val tileLayout = new TileLayout(16, 16, 32, 32)

    // Create cube with (x, y, bands) dimensions.
    val cubeXYB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] =
      TileLayerRDDBuilders.createMultibandTileLayerRDD(
        SparkContext.getOrCreate, arrayMultibandTile, tileLayout
      ).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]
    // Add time dimension.
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val cubeXYTB: RDD[(SpaceTimeKey, MultibandTile)] = cubeXYB.flatMap((pair: Tuple2[SpatialKey, MultibandTile]) => {
      times.map((time: ZonedDateTime) => (SpaceTimeKey(pair._1, TemporalKey(time)), pair._2))
    })
    // Combine cube with metadata to create the final datacube.
    val md: TileLayerMetadata[SpatialKey] = cubeXYB.metadata
    val bounds: Bounds[SpatialKey] = md.bounds
    val minKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.minKey, TemporalKey(times.head))
    val maxKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.maxKey, TemporalKey(times.last))
    val metadata: TileLayerMetadata[SpaceTimeKey] = new TileLayerMetadata[SpaceTimeKey](md.cellType, md.layout, md.extent, md.crs, new KeyBounds[SpaceTimeKey](minKey, maxKey))
    val datacube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(cubeXYTB, metadata)

    // Datacube extent(-180.0, -89.99999, 179.99999, 89.99999).
    val projectedPolygons1 = ProjectedPolygons.fromExtent(
      new Extent(-180, -80, -156, -72), "EPSG:4326"
    )
    val projectedPolygons2 = ProjectedPolygons.fromExtent(
      new Extent(-170, -80, -130, -70), "EPSG:4326"
    )
    val projectedPolygons3 = ProjectedPolygons.fromExtent(
      new Extent(-140, -80, -100, -70), "EPSG:4326"
    )
    val projectedPolygonsTriangle = ProjectedPolygons(List(Polygon((-20.0,-70.0), (-50.0,-70.0), (-20.0, -80.0), (-20.0,-70.0))), "EPSG:4326")
    val projectedPolygons = new ProjectedPolygons(
      projectedPolygons1.polygons ++ projectedPolygons2.polygons
        ++ projectedPolygons3.polygons
        ++ projectedPolygonsTriangle.polygons,
      projectedPolygons1.crs
    )

    val groupedAndMaskedByGeometry: RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])] =
      processes.groupAndMaskByGeometry(datacube, projectedPolygons, -1.0f)
    assertFalse(groupedAndMaskedByGeometry.isEmpty)

    // Check polygons.
    val polygons = groupedAndMaskedByGeometry.map(_._1).collect()
    assertNotEquals(0, polygons.length)
    assertEquals(polygons.length, projectedPolygons.polygons.length)
    for (polygon <- polygons) assert(projectedPolygons.polygons.contains(polygon))

    // Check if tiles are masked correctly
    groupedAndMaskedByGeometry.collect().foreach {
      case (polygon, tilesByDate) =>
        val numberOfDataCells = tilesByDate.head._3.band(0).toArray().count(_ > 0)
        tilesByDate.foreach(keyAndTile => {
          for (band <- keyAndTile._3.bands) {
            // Every tile in the list of dates should be masked by the same polygon.
            assertEquals(numberOfDataCells, band.toArray().count(_ > 0))
          }
        })
    }

    // Check if tiles are merged correctly.
    val resultCube: MultibandTileLayerRDD[SpaceTimeKey] = processes.mergeGroupedByGeometry(groupedAndMaskedByGeometry, datacube.metadata)

    // Compare to reference tile.
    saveRDD(resultCube.toSpatial(times.head),2, "groupByGeometry_2017-01-15_actual.tif", 6, Some(datacube.metadata.extent))
    val actualRaster = GeoTiffRasterSource("groupByGeometry_2017-01-15_actual.tif").read().get
    val referenceRaster = GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/groupByGeometry_2017-01-15_reference.tif").read().get
    assertRastersEqual(referenceRaster, actualRaster)

    // Visualize RDD.
    //val resultArray: Array[(SpaceTimeKey, MultibandTile)] = resultCube.collect()
    // val tiles: Iterable[(SpatialKey, Tile)] = resultArray.map(tile => (tile._1.spatialKey, tile._2.band(0)))
    // val fullRaster: Raster[Tile] = org.openeo.geotrellis.netcdf.NetCDFRDDWriter.ContextSeq(tiles, datacube.metadata.layout).stitch()
    // fullRaster.tile.renderPng(ColorRamps.BlueToRed).write("fullTile")
  }

  @Test
  def makeSquareTile(): Unit = {
    val tile: MultibandTile = MultibandTile( DoubleArrayTile.fill(1.0,144, 160))
    var squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(tile,new GridBounds[Int](0,16,128,144)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(4,20),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(20,4),0.0)

    squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(MultibandTile( DoubleArrayTile.fill(1.0,160, 144)),new GridBounds[Int](16,0,144,128)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(1.0,squareTile.band(0).getDouble(4,20),0.0)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(20,4),0.0)

    squareTile = new OpenEOProcesses().makeSquareTile(BufferedTile(MultibandTile( DoubleArrayTile.fill(1.0,160, 144)),new GridBounds[Int](16,16,144,144)),128,128,16,16)
    assertEquals(squareTile.cols,160)
    assertEquals(squareTile.rows,160)
    assertEquals(Double.NaN,squareTile.band(0).getDouble(20,150),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(20,140),0.0)
    assertEquals(1.0,squareTile.band(0).getDouble(4,20),0.0)
  }

  @Test
  def medianComposite():Unit = {

    val layer:MultibandTileLayerRDD[SpaceTimeKey] = LayerFixtures.sentinel2B04Layer

    val startDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val intervals = Range(0,20).flatMap{r => Seq(startDate.plusDays(10L*r),startDate.plusDays(10L*(r+1)))}.map(DateTimeFormatter.ISO_INSTANT.format(_))
    val labels = Range(0,20).map{r => DateTimeFormatter.ISO_INSTANT.format(startDate.plusDays(10L*r))}

    val resultTiles: Array[MultibandTile] = new OpenEOProcesses().aggregateTemporal(layer,intervals.asJava,labels.asJava,TestOpenEOProcessScriptBuilder.createMedian(true), java.util.Collections.emptyMap()).values.collect()
    val validTile = resultTiles.find(_ !=null).get
    val emptyTile = ArrayMultibandTile.empty(validTile.cellType,validTile.bandCount,validTile.cols,validTile.rows)
    val filledResult = resultTiles.map{t => if(t != null) t.band(0) else emptyTile.band(0)}
    GeoTiff(Raster(MultibandTile(filledResult),layer.metadata.extent),layer.metadata.crs).write("result.tiff",true)


  }

  @ParameterizedTest
  @EnumSource(classOf[PixelType])
  def aggregateTemporalTest(pixelType: PixelType): Unit = {
    val outDir = "/tmp/aggregateTemporalTest/"
    Files.createDirectories(Paths.get(outDir))
    val layer: MultibandTileLayerRDD[SpaceTimeKey] = LayerFixtures.randomNoiseLayer(pixelType)
    val bounds = layer.metadata.bounds
    val middleDate = SpaceTimeKey(0, 0, (bounds.get.minKey.instant + bounds.get.maxKey.instant) / 2).time

    // intervals is a list of start,end-pairs
    val intervals = List(middleDate.plusYears(-1000), middleDate, middleDate, middleDate.plusYears(1000))
      .map(DateTimeFormatter.ISO_INSTANT.format(_))
    val labels = (intervals.indices.collect { case i if i % 2 == 0 => intervals(i) }).toList

    val resultTiles: Array[MultibandTile] = new OpenEOProcesses().aggregateTemporal(layer,
      intervals.asJava,
      labels.asJava,
      TestOpenEOProcessScriptBuilder.createMedian(true),
      java.util.Collections.emptyMap()
    ).values.collect()

    val validTile = resultTiles.find(_ != null).get
    val emptyTile = ArrayMultibandTile.empty(validTile.cellType, validTile.bandCount, validTile.cols, validTile.rows)
    val filledResult = resultTiles.map { t => if (t != null) t.band(0) else emptyTile.band(0) }
    pixelType match {
      case PixelType.Double => assertEquals(64, validTile.band(0).cellType.bits); assertTrue(validTile.band(0).cellType.isFloatingPoint)
      case PixelType.Float => assertEquals(32, validTile.band(0).cellType.bits); assertTrue(validTile.band(0).cellType.isFloatingPoint)
      case PixelType.Int => assertEquals(32, validTile.band(0).cellType.bits); assertFalse(validTile.band(0).cellType.isFloatingPoint)
      case PixelType.Short => assertEquals(16, validTile.band(0).cellType.bits); assertFalse(validTile.band(0).cellType.isFloatingPoint)
      case PixelType.Byte => assertEquals(8, validTile.band(0).cellType.bits); assertFalse(validTile.band(0).cellType.isFloatingPoint)
      case PixelType.Bit => assertEquals(1, validTile.band(0).cellType.bits); assertFalse(validTile.band(0).cellType.isFloatingPoint)
      case _ => throw new IllegalStateException(s"pixelType $pixelType not supported")
    }

    GeoTiff(Raster(MultibandTile(filledResult), layer.metadata.extent), layer.metadata.crs)
      .write(outDir + pixelType.getClass.getSimpleName + ".tiff", optimizedOrder = true)

    val builder = new SparkAggregateScriptBuilder
    val emptyMap = new util.HashMap[String, Object]()
    builder.expressionEnd("min", emptyMap)
    builder.expressionEnd("max", emptyMap)
    builder.expressionEnd("mean", emptyMap)

    val geometries = ProjectedPolygons.fromExtent(layer.metadata.extent, layer.metadata.crs.toString())
    val splitPolygons = splitOverlappingPolygons(geometries.polygons)
    val outDirSpacial = outDir + pixelType.getClass.getSimpleName
    new AggregatePolygonProcess().aggregateSpatialGeneric(scriptBuilder = builder, datacube = layer, polygonsWithIndexMapping = splitPolygons,
      geometries.crs, bandCount = new OpenEOProcesses().RDDBandCount(layer), outDirSpacial)

    val groupedStats = parseCSV(outDirSpacial)
    for ((_, stats) <- groupedStats) pixelType match {
      case PixelType.Bit => assertEqualTimeseriesStats(Seq(Seq(0, 1, 0.5)), stats, 0.01)
      case _ => assertEqualTimeseriesStats(Seq(Seq(5, 15, 10.0)), stats, 0.1)
    }
  }

  @Test
  def resampleCubeSpatial_spatial():Unit = {
    val tile: Tile = DoubleArrayTile.fill(1.0,1280, 1280)
    val tileSize = 256
    val targetTileSize = 302
    val layout = new TileLayout(1 + tile.cols / tileSize, 1 + tile.rows / tileSize, tileSize, tileSize)
    val targetLayout = new TileLayout((0.5*(1 + tile.cols / targetTileSize)).toInt, (0.5*(1 + tile.rows / targetTileSize)).toInt, targetTileSize, targetTileSize)
    val datacube = TileLayerRDDBuilders.createMultibandTileLayerRDD(OpenEOProcessesSpec.sc, new ArrayMultibandTile(Array[Tile](tile)), layout)

    val targetExtent = ProjectedExtent(Extent(-40,-40,40,40),LatLng).reproject(WebMercator)
    val resampled = new OpenEOProcesses().resampleCubeSpatial_spatial(datacube.withContext(_.repartition(10)),WebMercator,LayoutDefinition(targetExtent,targetLayout),ResampleMethod.DEFAULT,null)
    assertEquals(WebMercator, resampled._2.metadata.crs)
    assertEquals(302, resampled._2.metadata.tileCols)
    val stitched: Raster[MultibandTile] = resampled._2.stitch()

    val resampledBounds = resampled._2.metadata.bounds
    assertEquals(0,resampledBounds.get.minKey.col)
    assertEquals(0,resampledBounds.get.minKey.row)

  }

}
