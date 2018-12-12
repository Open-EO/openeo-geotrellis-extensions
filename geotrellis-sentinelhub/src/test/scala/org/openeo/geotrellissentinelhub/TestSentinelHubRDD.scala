package org.openeo.geotrellissentinelhub

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.Base64

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}
class TestSentinelHubRDD extends FlatSpec with Matchers {

  //bbox close to "GanzenStraat, Geel", tile 31UFS
  val bbox = new Extent(4.959340,51.190701,4.965531,51.193989)
  val projectedExtent = ProjectedExtent(bbox,LatLng)

  def sparkContext(): Unit = {
    val config = new HdfsConfiguration
    config.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(config)
    val conf = new SparkConf
    conf.setAppName("PyramidFactoryTest")
    conf.setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = SparkContext.getOrCreate(conf)
    //creating context may have screwed up security settings
    UserGroupInformation.setConfiguration(config)
  }

  it should "download a single sentinelhub tile" in {
    val script = createSentinelHubScript(Array("B04", "B03","B02"))
    val encodedScript = new String(Base64.getEncoder.encode(script.getBytes))
    val tile = retrieveTileFromSentinelHub("2018-10-28",bbox.reproject(LatLng,WebMercator),Some(encodedScript))
    print(tile.band(0).asciiDraw())
  }
  sparkContext()

  it should "create a valid script" in {
    val result = createSentinelHubScript(Array("B02","VAA"))
    print(result)
    result should include ("[a.B02,a.VAA]")
  }

  it should "load data from SentinelHub" in {

    val date = ZonedDateTime.of(2017,1,1,0,0,0,0,ZoneId.systemDefault())
    createGeotrellisRDD(ProjectedExtent(bbox.reproject(LatLng,WebMercator),WebMercator),date)
  }

  it should "load timeseries data from SentinelHub" in {
    val date = ZonedDateTime.of(2018,10,7,0,0,0,0,ZoneId.systemDefault())
    val endDate = ZonedDateTime.of(2018,11,1,0,0,0,0,ZoneId.systemDefault())
    val rdd = createGeotrellisRDD(ProjectedExtent(bbox.reproject(LatLng,WebMercator),WebMercator),date,endDate).cache()
    rdd.toSpatial(parse("2018-10-28")).stitch().tile.renderPng().write("/tmp/out2018-10-28.png")
    rdd.toSpatial(parse("2018-10-15")).stitch().tile.renderPng().write("/tmp/out2018-10-15.png")
    rdd.toSpatial(parse("2018-10-10")).stitch().tile.renderPng().write("/tmp/out2018-10-10.png")
  }




  it should "return only dates for extent" in {

    val dates = fetchDates(projectedExtent, parse("2018-10-01"),parse("2018-10-31"))
    println(dates)
    //compare with manually verified list
    dates should be (Array("2018-10-28","2018-10-20", "2018-10-18", "2018-10-15", "2018-10-13", "2018-10-10", "2018-10-05", "2018-10-03"))
  }
  def parse(dateString:String) = LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dateString)).atStartOfDay(ZoneId.of("UTC"))

}
