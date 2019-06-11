package org.openeo.geotrellisaccumulo

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.spark.{MultibandTileLayerRDD, SpaceTimeKey}
import geotrellis.vector.MultiPolygon
import geotrellis.vector.io.json.{GeoJson, _}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Timeseries computation benchmark, to be executed as a standalone application.
  */
object AccumuloRDDBenchmark extends App {

  def sparkContext(): Unit = {
    val config = new HdfsConfiguration
    config.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(config)
    val conf = new SparkConf
    conf.setAppName("PyramidFactoryTest")
    //conf.setMaster("local[16]")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = SparkContext.getOrCreate(conf)
    //creating context may have screwed up security settings
    UserGroupInformation.setConfiguration(config)
  }

  val units = List((TimeUnit.DAYS,"days"),(TimeUnit.HOURS,"hours"), (TimeUnit.MINUTES,"minutes"), (TimeUnit.SECONDS,"seconds"))
  def humanReadable(timediff:Long):String = {
    val init = ("", timediff)
    units.foldLeft(init){ case (acc,next) =>
    val (human, rest) = acc
    val (unit, name) = next
    val res = unit.convert(rest,TimeUnit.MILLISECONDS)
    val str = if (res > 0) human + " " + res + " " + name else human
    val diff = rest - TimeUnit.MILLISECONDS.convert(res,unit)
    (str,diff)
    }._1
  }


  val file = {
    if(args.length > 0) {
      args(0)
    }else{
      "/home/driesj/workspace/openeo-geotrellis-extensions/geotrellis-accumulo-extensions/src/test/resources/2017_CroptypesFlemishParcels_DeptLV_30ha.geojson"
    }

  }
  val splitRanges = {
    if(args.length > 1) {
      args(1).toBoolean
    }else{
      false
    }

  }
  println("Split ranges: " + splitRanges)

  sparkContext()
  val sc = SparkContext.getOrCreate()

  val layerCRS = WebMercator
  val layername = "S2_FAPAR_V102_WEBMERCATOR2"

  //construct query, similar to:
  //https://proba-v-mep.esa.int/api/timeseries/v1.0/ts/S2_FAPAR_V102_WEBMERCATOR2/point?lon=4.959340&lat=51.190701&startDate=2017-01-01&endDate=2018-03-01
  //val bbox = new Extent(3.4, 51, 3.5, 52)
  val pyramidFactory: PyramidFactory = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")
  pyramidFactory.setSplitRanges(splitRanges)


  var srs: String = "EPSG:32632"
  val value : JsonFeatureCollection = GeoJson.fromFile[JsonFeatureCollection](file)

  val polygons = value.getAllPolygons()
  var durations = List[Long]()
  var perPoints = List[Double]()
  for( polygon <- polygons ){

    val bbox = polygon.envelope
    val reprojected = polygon.reproject(LatLng,layerCRS)

    val t0 = System.currentTimeMillis()
    val rdd: MultibandTileLayerRDD[SpaceTimeKey] = pyramidFactory.load_rdd(layername, 14, reprojected.envelope, "EPSG:" + layerCRS.epsgCode.get, Option.apply(ZonedDateTime.parse("2017-12-31T00:00:00Z")), Option.apply(ZonedDateTime.parse("2018-06-30T02:00:00Z")))
    val tileRDD = rdd.withContext(_.mapValues{_.band(0)})
    val series = tileRDD.meanSeries(MultiPolygon(reprojected))
    val t1 = System.currentTimeMillis()

    val  numberOfPoints = series.size
    durations = (t1-t0) :: durations
    print("Total time:" + humanReadable(t1-t0))
    val timePerPoint = (t1 - t0) / numberOfPoints.toDouble
    println( " Time per point: " + timePerPoint +  " Points: " + numberOfPoints)
    perPoints = timePerPoint :: perPoints
  }
  private val durationsRDD = sc.parallelize(durations)

  private val stats: StatCounter = durationsRDD.stats()
  println("Mean: " + stats.mean)
  println("Max: " + stats.max)
  println("Min: " + stats.min)
  println("StdDev: " + stats.stdev)


}
