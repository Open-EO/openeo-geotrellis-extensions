package org.openeo.geotrellisaccumulo

import geotrellis.raster.MultibandTile
import geotrellis.spark.SpaceTimeKey
import geotrellis.vector.Extent
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class GeotrellisAccumuloRDDTest  extends FlatSpec with Matchers{

  def isSorted[T](s: Seq[T])(implicit ord: Ordering[T]): Boolean = s match {
    case Seq() => true
    case Seq(_) => true
    case _ => s.sliding(2).forall { case List(x, y) => ord.lteq(x, y) }
  }


  it should "load data from accumulo" in {
    val config = new HdfsConfiguration
    config.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(config)
    val conf = new SparkConf
    conf.setAppName("IngestS2")
    conf.setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.hadoop.fs.permissions.umask-mode", "000")
    val sc = new SparkContext(conf)
    UserGroupInformation.setConfiguration(config)

    val layername = "CGS_SENTINEL2_RADIOMETRY_V102"
    val bbox = new Extent(3, 51, 3.5, 52)

    val accumuloTileLayerRepository = new PyramidFactory("hdp-accumulo-instance", "epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181")
    accumuloTileLayerRepository.pyramid(layername,bbox,"EPSG:4326")
    val rdd = accumuloTileLayerRepository.rdd[MultibandTile](layername)

    println(rdd)
    val partitions = rdd.partitions

    //rdd.foreach(v => println(v))
    //val startkeys = partitions.flatMap(p => p.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.t.asInstanceOf[BatchInputSplit].getRanges().asScala).map(r => r.getStartKey)
    //isSorted(startkeys) shouldEqual true
    /*partitions.foreach(p => p.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.t.asInstanceOf[BatchInputSplit].getRanges().stream().forEach(new Consumer[data.Range] {
      override def accept(t: data.Range): Unit = {
        if(t!=null) {
          print(t.getStartKey.getRow())
          println(" " + t.getStartKey.getTimestamp)
        }
      }
    }))*/
    //println(partitions)
    val tile = rdd.lookup(new SpaceTimeKey(0,0,1518134400000L))
    println(tile)

  }
}
