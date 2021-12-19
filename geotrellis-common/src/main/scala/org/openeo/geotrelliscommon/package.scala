package org.openeo

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.spark.partition.PartitionerIndex
import org.apache.spark.Partitioner
import org.locationtech.sfcurve.zorder.{Z2, ZRange}
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

package object geotrelliscommon {

  class ByKeyPartitioner[K](splits: Array[K]) extends Partitioner {
    override def numPartitions: Int = splits.length

    override def getPartition(key: Any): Int = splits.indexOf(key)
  }

  /**
   * Spatial partitioner with only 1 tile per partition: for tiles with lots of bands!
   */
  object ByTileSpatialPartitioner extends  PartitionerIndex[SpatialKey] {
    private def toZ(key: SpatialKey): Z2 = Z2(key.col, key.row)

    def toIndex(key: SpatialKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] =
      Z2.zranges(ZRange(toZ(keyRange._1), toZ(keyRange._2))).map(r => (BigInt(r.lower), BigInt(r.upper)))

  }

  object SparseSpaceOnlyPartitioner {
    // Shift by 8 removes the last 8 bytes: 256 tiles max in one partition.
    def toIndex(key: SpaceTimeKey, indexReduction:Int = 8): BigInt = Z2(key.col,key.row).z >> indexReduction
  }

  object SparseSpaceTimePartitioner {
    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)

    // Shift by 8 removes the last 8 bytes: 256 tiles max in one partition.
    def toIndex(key: SpaceTimeKey, indexReduction:Int = 8): BigInt = keyIndex.toIndex(key) >> indexReduction
  }

  class SparseSpaceTimePartitioner (val indices: Array[BigInt], val indexReduction:Int = 8) extends PartitionerIndex[SpaceTimeKey] {

    def toIndex(key: SpaceTimeKey): BigInt = SparseSpaceTimePartitioner.toIndex(key, indexReduction)

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      indices.map(i => (i,i))
    }


    def canEqual(other: Any): Boolean = other.isInstanceOf[SparseSpaceTimePartitioner]

    /**
     * This equals method does not compare the indices, so makes the decision of equality only depend on the region indices it generates.
     * The merge operation and use of geotrellis.spark.partition.ReorderedSpaceRDD depends on this
     * @param other
     * @return
     */
    override def equals(other: Any): Boolean = other match {
      case that: SparseSpaceTimePartitioner =>
        (that canEqual this) &&
          indexReduction == that.indexReduction
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(indices, indexReduction)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }


    override def toString = s"SparseSpaceTimePartitioner ${indices.length}"
  }

  class SparseSpaceOnlyPartitioner (val indices: Array[BigInt], val indexReduction:Int = 8) extends PartitionerIndex[SpaceTimeKey] {

    def toIndex(key: SpaceTimeKey): BigInt = SparseSpaceOnlyPartitioner.toIndex(key, indexReduction)

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      indices.map(i => (i,i))
    }


    def canEqual(other: Any): Boolean = other.isInstanceOf[SparseSpaceOnlyPartitioner]

    override def equals(other: Any): Boolean = other match {
      case that: SparseSpaceOnlyPartitioner =>
        (that canEqual this) &&
          indices == that.indices &&
          indexReduction == that.indexReduction
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(indices, indexReduction)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
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
