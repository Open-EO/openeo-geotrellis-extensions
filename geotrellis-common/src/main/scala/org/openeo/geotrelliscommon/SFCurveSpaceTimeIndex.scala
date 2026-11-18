package org.openeo.geotrelliscommon
import geotrellis.layer.SpaceTimeKey
import geotrellis.spark.partition.PartitionerIndex
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

class SFCurveSpaceTimeIndex(val keyIndex:SfCurveZSpaceTimeKeyIndex, spatialReduction: Int = 8) extends PartitionerIndex[SpaceTimeKey]  {

  import geotrellis.layer.SpaceTimeKey

  def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> spatialReduction

  def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
    val originalRanges = keyIndex.indexRanges(keyRange)

    val mappedRanges = originalRanges.map(range => (range._1 >> spatialReduction,(range._2 >> spatialReduction) ))

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