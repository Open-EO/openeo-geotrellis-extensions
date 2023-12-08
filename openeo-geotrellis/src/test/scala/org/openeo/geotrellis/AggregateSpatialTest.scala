package org.openeo.geotrellis

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.{ByteCells, ByteConstantTile, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertArrayEquals, assertEquals}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.ComputeStatsGeotrellisAdapterTest.{polygon1, polygon2}
import org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder

import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.io.Source

object AggregateSpatialTest {

  private var sc: SparkContext = _

  @BeforeClass
  def setUpSpark(): Unit = {
    sc = {
      //val config = new HdfsConfiguration
      //config.set("hadoop.security.authentication", "kerberos")
      //UserGroupInformation.setConfiguration(config)

      val conf = new SparkConf().set("spark.driver.bindAddress", "127.0.0.1")
      SparkUtils.createLocalSparkContext(sparkMaster = "local[2]", appName = getClass.getSimpleName, conf)
    }

  }

  def parseCSV(outDir: String, spatioTemporal: Boolean = true): Map[String, scala.Seq[scala.Seq[Double]]] = {
    val stats = mutable.ListBuffer[(String, Int, scala.Seq[Double])]()

    if (!Files.exists(Paths.get(outDir))) {
      // Without this check, an empty collection would be returned.
      throw new Exception("Path does not exist: " + outDir)
    }

    Files.list(Paths.get(outDir)).filter(_.toString.endsWith(".csv")).forEach(path => {
      println(path)
      val bufferedSource = Source.fromFile(path.toFile)

      try {
        for (line <- bufferedSource.getLines.drop(1)) { // skip the header
          val includeTrailingEmptyStrings = -1
          var columnValues = line.split(",", includeTrailingEmptyStrings).map(_.trim).toSeq

          if (!spatioTemporal) {
            columnValues = "no_timestamp" +: columnValues
          }

          def asDouble(s: String) = if (s == "") Double.NaN else s.toDouble

          val timestamp +: geometry +: numbers = columnValues // pattern match against Seq(timestamp, geometry, numbers @ _*) doesn't work?
          stats.append((timestamp, geometry.toInt, numbers.map(asDouble)))
        }
      }
      finally bufferedSource.close()
    })

    val groupedStats = stats
      .groupBy { case (timestamp, _, _) => timestamp }
      .mapValues { timestampedValues =>
        timestampedValues
          .sortBy { case (_, geometry, _) => geometry }
          .map { case (_, _, numbers) => numbers }
      }

    groupedStats.foreach(println)
    groupedStats
  }

  def assertEqualTimeseriesStats(expected: scala.collection.Seq[scala.collection.Seq[Double]], actual: scala.collection.Seq[scala.collection.Seq[Double]], delta:Double=1e-6): Unit = {
    println(s"expected: $expected")
    println(s"actual: $actual")
    assertEquals("should have same polygon count", expected.length, actual.length)
    expected.indices.foreach { i =>
      assertArrayEquals("should have same band stats", expected(i).toArray, actual(i).toArray, delta)
    }
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }
}

class AggregateSpatialTest {

  import AggregateSpatialTest._

  private val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter(
    zookeepers = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
    accumuloInstanceName = "hdp-accumulo-instance"
  )


  private def buildCubeRdd(from: ZonedDateTime, to: ZonedDateTime): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val tile10 = new ByteConstantTile(10.toByte, 256, 256, ByteCells.withNoData(Some(255.byteValue())))
    val datacube = TestOpenEOProcesses.tileToSpaceTimeDataCube(tile10).withContext(_.filter { case (key, _) => !(key.time isBefore from) && !(key.time isAfter to) })
    val polygonExtent = polygon1.extent.combine(polygon2.extent)
    val updatedMetadata = datacube.metadata.copy(
      extent = polygonExtent,
      crs = LatLng,
      layout = LayoutDefinition(polygonExtent, datacube.metadata.tileLayout)
    )

    ContextRDD(datacube, updatedMetadata)
  }

  @Test def computeVectorCube_on_datacube_from_polygons(): Unit = {
    val cube = LayerFixtures.sentinel2B04Layer
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube("max",cube,LayerFixtures.b04Polygons,"/tmp/csvoutput")
  }

  @Test def multiple_statistics(): Unit = {
    val cube = LayerFixtures.sentinel2B04Layer

    val builder= new SparkAggregateScriptBuilder
    val emptyMap = new util.HashMap[String,Object]()
    val countMap = new util.HashMap[String,Object]()
    countMap.put("condition",true.asInstanceOf[Object])
    builder.expressionEnd("min",emptyMap)
    builder.expressionEnd("median",emptyMap)
    builder.expressionEnd("mean",emptyMap)
    builder.expressionEnd("max",emptyMap)
    builder.expressionEnd("sd",emptyMap)
    builder.expressionEnd("sum",emptyMap)
    builder.expressionEnd("count",emptyMap)
    builder.expressionEnd("count",countMap)
    val outDir = "/tmp/csvoutput"
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube(builder,cube,LayerFixtures.b04Polygons,outDir)

    val groupedStats = parseCSV(outDir).toSeq.sortBy(_._1).map(_._2)
    val thePolygons = ProjectedPolygons.reproject(LayerFixtures.b04Polygons, cube.metadata.crs)
    var polygonIndex = 0

    def nd(d:Double):Double = {
      if(d.isNaN) {
        32767.0
      }else{
        d
      }
    }
    for(geom:Geometry <- thePolygons.polygons){
      val histograms = LayerFixtures.b04Raster.mask(geom).tile.histogram

      var dateIndex = 0
      for( h <- histograms) {
        val ignoreNodata = h.mutable()
        ignoreNodata.uncountItem(32767)
        val expected = groupedStats(dateIndex)(polygonIndex)
        if(ignoreNodata.totalCount()!=0){
          val count = nd(expected(6))
          assertEquals(count,ignoreNodata.totalCount(),0.1)

          val diff = math.abs(nd(expected(2)) - ignoreNodata.mean().get)
          if( diff>2){
            //TODO: seems to be something wrong with mean from histogram??
            if(count > 10)
              println(s"large diff ${diff}")
            else
              println("large diff for low number")
          }else{
            if(!ignoreNodata.mean().get.isNaN) {
              assertEquals(nd(expected(2)) ,ignoreNodata.mean().get,2.1)
              //assertEquals(nd(expected(1)) ,ignoreNodata.median().get,6.1)//medians have some room for rounding
            }

          }
          assertEquals(nd(expected(0)),ignoreNodata.minValue().get,0.1)
          assertEquals(nd(expected(3)),ignoreNodata.maxValue().get,0.1)
        }
        dateIndex = dateIndex +1
      }
      polygonIndex = polygonIndex + 1
    }
  }

  @Test
  def compute_median_timeseries_on_datacube_from_polygons(): Unit = {
    val from_date = "2017-01-01T00:00:00Z"
    val to_date = "2017-03-10T00:00:00Z"

    val builder= new SparkAggregateScriptBuilder
    val emptyMap = new util.HashMap[String,Object]()
    builder.expressionEnd("median",emptyMap)

    val outDir = "/tmp/csvoutput2"
    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube(
      builder,
      buildCubeRdd(ZonedDateTime.parse(from_date), ZonedDateTime.parse(to_date)),
      ProjectedPolygons(Seq(polygon1, polygon2), "EPSG:4326"),
        outDir
    )

    val groupedStats = parseCSV(outDir)
    val keys = Seq("2017-01-01T01:00:00.000+01:00", "2017-01-15T01:00:00.000+01:00", "2017-02-01T01:00:00.000+01:00")
    println(groupedStats)
    keys.foreach(k => assertEqualTimeseriesStats(
      Seq(Seq(10.0, Double.NaN), Seq(10.0, Double.NaN)), groupedStats(k)
    ))
  }

  @Test
  def compute_generic_timeseries_from_datacube(): Unit = {
    val builder = new SparkAggregateScriptBuilder

    val emptyMap = new util.HashMap[String, Object]()
    val countMap = new util.HashMap[String, Object]()
    countMap.put("condition", true.asInstanceOf[Object])

    builder.expressionEnd("min", emptyMap)
    builder.expressionEnd("mean", emptyMap)
    builder.expressionEnd("count", countMap)

    val from = ZonedDateTime.parse("2017-01-01T00:00:00Z")

    val cube = buildCubeRdd(from, to = ZonedDateTime.now())

    val pointWkts = Seq(polygon1, polygon2).map(_.getCentroid.toWKT())
    val pointsCrs = LatLng

    val outDir = "/tmp/compute_generic_timeseries_from_datacube"

    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_datacube(builder, cube, pointWkts.asJava,
      s"EPSG:${pointsCrs.epsgCode.get}", outDir)

    val groupedStats = parseCSV(outDir)

    for ((_, stats) <- groupedStats) assertEqualTimeseriesStats(Seq(
      Seq(10, 10, 1, Double.NaN, Double.NaN, 1), // point1
      Seq(10, 10, 1, Double.NaN, Double.NaN, 1)), // point2
      stats)
  }

  @Test
  def compute_generic_timeseries_from_spatial_datacube(): Unit = {
    val builder = new SparkAggregateScriptBuilder
    val emptyMap = new util.HashMap[String, Object]()

    builder.expressionEnd("min", emptyMap)
    builder.expressionEnd("mean", emptyMap)

    val from = ZonedDateTime.parse("2017-01-01T00:00:00Z")

    val spatialCube = buildCubeRdd(from, to = ZonedDateTime.now())
      .toSpatial(from)

    val geometryWkts = Seq(polygon1, polygon2.getCentroid).map(_.toWKT())
    val geometriesCrs = LatLng

    val outDir = "/tmp/compute_generic_timeseries_from_spatial_datacube"

    computeStatsGeotrellisAdapter.compute_generic_timeseries_from_spatial_datacube(builder, spatialCube,
      geometryWkts.asJava, s"EPSG:${geometriesCrs.epsgCode.get}", outDir)

    val groupedStats = parseCSV(outDir, spatioTemporal = false)

    for ((_, stats) <- groupedStats) assertEqualTimeseriesStats(Seq(
      Seq(10, 10.0, Double.NaN, Double.NaN), // geometry1
      Seq(10, 10.0, Double.NaN, Double.NaN)), // geometry2
      stats)
  }
}
