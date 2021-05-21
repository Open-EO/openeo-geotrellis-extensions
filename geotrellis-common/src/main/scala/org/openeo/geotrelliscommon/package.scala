package org.openeo

import geotrellis.layer.SpaceTimeKey
import geotrellis.spark.partition.PartitionerIndex
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

package object geotrelliscommon {
  object SparseSpaceTimePartitioner {
    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)

    // Shift by 8 removes the last 8 bytes: 256 tiles max in one partition.
    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> 8
  }

  class SparseSpaceTimePartitioner (val indices: Array[BigInt]) extends PartitionerIndex[SpaceTimeKey] {

    def toIndex(key: SpaceTimeKey): BigInt = SparseSpaceTimePartitioner.toIndex(key)

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      indices.map(i => (i,i))
    }
  }

  implicit object SpaceTimeByMonthPartitioner extends PartitionerIndex[SpaceTimeKey] {

    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)
    //private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col , key.row , 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> 8

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)

      val mappedRanges = originalRanges.map(range => (range._1 >> 8,(range._2 >> 8) ))

      val distinct = mappedRanges.distinct
      var previousEnd: BigInt = null

      //filter out regions that only span 1 value, and are already included in another region, so basically duplicates
      var lookAheadIndex = 0
      val filtered = distinct.filter(range => {
        lookAheadIndex +=1
        try{
          if(range._1 == previousEnd && range._1 == range._2) {
            false
          }else if(lookAheadIndex < distinct.size && range._1 == range._2 && distinct(lookAheadIndex)._1 == range._2) {
            false
          }else{
            true
          }
        }finally {
          previousEnd = range._2
        }

      })
      return filtered
    }

  }
}
