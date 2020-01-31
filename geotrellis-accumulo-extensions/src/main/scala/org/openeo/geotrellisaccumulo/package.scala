package org.openeo

import geotrellis.layer._
import geotrellis.raster.MultibandTile
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.partition.PartitionerIndex
import geotrellis.store.index.zcurve.{Z3, ZSpaceTimeKeyIndex}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters.mapAsScalaMapConverter

package object geotrellisaccumulo {

    def createGeotrellisPyramid(levels: java.util.Map[Integer,RDD[(SpaceTimeKey,MultibandTile)]  with Metadata[TileLayerMetadata[SpaceTimeKey]]]): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] ={
      val map: Map[Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = levels.asScala.toMap.map(t=>(t._1.toInt,t._2))
      new Pyramid(map)
    }

  def decodeIndexKey(region:BigInt):SpaceTimeKey = {
    val (x,y,t) = new Z3(region.longValue() << 8 ).decode

    new SpaceTimeKey(x,y,t*1000L * 60 * 60 * 24 )
  }
  /*
  def decodeIndexKey(region:BigInt):SpaceTimeKey = {
    val (x,y,t) = new Z3(region.longValue()).decode
    val day = t%31
    val month = ((t-day)/31)%13
    val year = ((t - day)/31-month)/13
    SpaceTimeKey(x <<4,y << 4,ZonedDateTime.of(LocalDate.of(year,month,day+1),LocalTime.MIDNIGHT,ZoneId.of("UTC")))
  }*/

  /*implicit object SpaceTimeByMonthPartitioner extends  PartitionerIndex[SpaceTimeKey] {
    private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col >> 4, key.row >> 4, 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
      Z3.zranges(toZ(keyRange._1), toZ(keyRange._2))
  }*/
  implicit object SpaceTimeByMonthPartitioner extends  PartitionerIndex[SpaceTimeKey] {

    val keyIndex = ZSpaceTimeKeyIndex.byDay(null)
    //private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col , key.row , 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> 8

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)
      val mappedRanges = originalRanges.map(range => (range._1 >> 8,range._2 >> 8 ))
      val distinct = mappedRanges.distinct
      var previousEnd: BigInt = null
      var wasSingleValue: Boolean = false

      //filter out regions that only span 1 value, and are already included in another region, so basically duplicates
      val filtered = distinct.filter(range => {
        try{
          if(range._1 == previousEnd && range._1 == range._2) {
            false
          }else if(wasSingleValue && range._1 ==previousEnd) {
            false
          }else{
            true
          }
        }finally {
          previousEnd = range._2
          wasSingleValue = range._2 == range._1
        }

      })
      return filtered
    }

  }
}
