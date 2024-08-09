package org.openeo

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.spark.partition.PartitionerIndex
import org.apache.spark.Partitioner
import org.locationtech.sfcurve.zorder.{Z2, ZRange}
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

package object geotrelliscommon {

  def autoUtmEpsg(lon: Double, lat: Double): Int = {
    val zone: Int = (math.floor((lon + 180.0) / 6.0) % 60).toInt + 1
    //Use latitude to determine north / south
    if( lat >= 0.0)
      return (32600 + zone)
    else{

      return 32700 + zone
    }
  }



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

  object ByTileSpacetimePartitioner extends PartitionerIndex[SpaceTimeKey] {
    private def toZ(key: SpaceTimeKey): Z2 = Z2(key.col, key.row)

    def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
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

  class SparseSpaceTimePartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpaceTimeKey]] = Option.empty) extends PartitionerIndex[SpaceTimeKey] {

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
      val state = Seq(indexReduction)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }


    override def toString = s"SparseSpaceTimePartitioner ${indices.length} ${theKeys.isDefined}"
  }

  class SparseSpaceOnlyPartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpaceTimeKey]] = Option.empty ) extends PartitionerIndex[SpaceTimeKey] {

    def toIndex(key: SpaceTimeKey): BigInt = SparseSpaceOnlyPartitioner.toIndex(key, indexReduction)

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      indices.map(i => (i,i))
    }


    def canEqual(other: Any): Boolean = other.isInstanceOf[SparseSpaceOnlyPartitioner]

    override def equals(other: Any): Boolean = other match {
      case that: SparseSpaceOnlyPartitioner =>
        (that canEqual this) &&
          indexReduction == that.indexReduction
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq( indexReduction)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  class SparseSpatialPartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpatialKey]] = Option.empty ) extends PartitionerIndex[SpatialKey] {

    def toIndex(key: SpatialKey): BigInt = Z2(key.col,key.row).z >> indexReduction

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] = {
      indices.map(i => (i,i))
    }


    def canEqual(other: Any): Boolean = other.isInstanceOf[SparseSpatialPartitioner]

    override def equals(other: Any): Boolean = other match {
      case that: SparseSpatialPartitioner =>
        (that canEqual this) &&
          indexReduction == that.indexReduction
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq( indexReduction)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  class ConfigurableSpatialPartitioner(val indexReduction:Int = 4) extends PartitionerIndex[SpatialKey] {
    private def toZ(key: SpatialKey): Z2 = Z2(key.col >> indexReduction, key.row >> indexReduction)

    def toIndex(key: SpatialKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] =
      Z2.zranges(ZRange(toZ(keyRange._1), toZ(keyRange._2))).map(t=> (BigInt.long2bigInt(t.lower),BigInt.long2bigInt(t.upper)))
  }

  class ConfigurableSpaceTimePartitioner ( val indexReduction:Int = SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION )  extends PartitionerIndex[SpaceTimeKey] {

    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)

    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> indexReduction

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)

      val mappedRanges = originalRanges.map(range => (range._1 >> indexReduction, (range._2 >> indexReduction)))

      val distinct = mappedRanges.distinct
      var previousEnd: BigInt = null

      //filter out regions that only span 1 value, and are already included in another region, so basically duplicates
      var lookAheadIndex = 0
      val filtered = distinct.filter(range => {
        lookAheadIndex += 1
        try {
          if (range._1 == previousEnd && range._1 == range._2) {
            false
          } else if (lookAheadIndex < distinct.size && range._1 == range._2 && distinct(lookAheadIndex)._1 == range._2) {
            false
          } else {
            true
          }
        } finally {
          previousEnd = range._2
        }

      })
      return filtered
    }

  }

  implicit object SpaceTimeByMonthPartitioner extends PartitionerIndex[SpaceTimeKey] {

    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)
    //private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col , key.row , 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    val DEFAULT_INDEX_REDUCTION = 7

    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> DEFAULT_INDEX_REDUCTION

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)

      val mappedRanges = originalRanges.map(range => (range._1 >> DEFAULT_INDEX_REDUCTION,(range._2 >> DEFAULT_INDEX_REDUCTION) ))

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

  import java.time.Duration
  import java.util.concurrent.TimeUnit


  def retryForever[R](delay: Duration, retries: Int = 20 /* actually: attempts */, onAttemptFailed: Exception => Unit = _ => ())(f: => R): R = {
    var lastException: Exception = null
    var countDown = retries
    while (countDown>0) {
      try return f
      catch {
        case e: Exception =>
          onAttemptFailed(e)
          lastException = e
          if (countDown > 1) TimeUnit.SECONDS.sleep(delay.getSeconds)
      }
      countDown = countDown - 1
    }


    throw lastException
  }



}
