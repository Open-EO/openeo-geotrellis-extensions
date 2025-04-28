package org.openeo

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.spark.partition.PartitionerIndex
import geotrellis.store.index.KeyIndex
import org.apache.spark.Partitioner
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z2, ZRange}
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalTime, OffsetTime, ZonedDateTime}

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

  class ByTileSpacetimePartitioner(val theKeys: Option[Array[SpatialKey]] = Option.empty) extends PartitionerIndex[SpaceTimeKey] with SpatialKeysProvider {
    private def toZ(key: SpaceTimeKey): Z2 = Z2(key.col, key.row)

    def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
      Z2.zranges(ZRange(toZ(keyRange._1), toZ(keyRange._2))).map(r => (BigInt(r.lower), BigInt(r.upper)))

    override def spatialKeys: Option[Array[SpatialKey]] = {
      theKeys
    }

    override def toString = s"ByTileSpacetimePartitioner ${theKeys.map(_.length).getOrElse(0)}"
  }

  object SparseSpaceOnlyPartitioner {
    // Shift by 8 removes the last 8 bytes: 256 tiles max in one partition.
    def toIndex(key: SpaceTimeKey, indexReduction:Int = 8): BigInt = Z2(key.col,key.row).z >> indexReduction
    def toIndex(key: SpatialKey, indexReduction:Int): BigInt = Z2(key.col,key.row).z >> indexReduction
  }

  object SparseSpaceTimePartitioner {
    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)

    // Shift by 8 removes the last 8 bytes: 256 tiles max in one partition.
    def toIndex(key: SpaceTimeKey, indexReduction:Int = 8): BigInt = keyIndex.toIndex(key) >> indexReduction
  }


  trait SpatialKeysProvider {
    def spatialKeys: Option[Array[SpatialKey]]
  }

  class SparseSpaceTimePartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpaceTimeKey]] = Option.empty) extends PartitionerIndex[SpaceTimeKey] with SpatialKeysProvider {

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

    override def spatialKeys: Option[Array[SpatialKey]] = {
      theKeys.map(_.map(_.spatialKey).distinct)
    }

  }

  class SparseSpaceOnlyPartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpaceTimeKey]] = Option.empty ) extends PartitionerIndex[SpaceTimeKey] with SpatialKeysProvider {

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

    override def spatialKeys: Option[Array[SpatialKey]] = theKeys.map(_.map(_.spatialKey).distinct)
  }

  class SparseSpatialPartitioner (val indices: Array[BigInt], val indexReduction:Int = 8, val theKeys: Option[Array[SpatialKey]] = Option.empty ) extends PartitionerIndex[SpatialKey] with SpatialKeysProvider {

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

    override def spatialKeys: Option[Array[SpatialKey]] = theKeys
  }

  class ConfigurableSpatialPartitioner(val indexReduction:Int = 4) extends PartitionerIndex[SpatialKey] {
    private def toZ(key: SpatialKey): Z2 = Z2(key.col >> indexReduction, key.row >> indexReduction)

    def toIndex(key: SpatialKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] =
      Z2.zranges(ZRange(toZ(keyRange._1), toZ(keyRange._2))).map(t=> (BigInt.long2bigInt(t.lower),BigInt.long2bigInt(t.upper)))
  }

  object ConfigurableSpatialPartitionerReduceZ {
    /**
     * Maps a sequence of index ranges by applying a reduction to their values, and filters out redundant ranges.
     *
     * @param originalRanges A sequence of tuples where each tuple represents a range with a start and an end value.
     * @param indexReduction The number of bits to reduce from each index value in the ranges.
     * @return A sequence of distinct and filtered ranges after applying the index reduction.
     */
    def mapIndexRangeWithReduction(originalRanges: Seq[(BigInt, BigInt)], indexReduction: Int): Seq[(BigInt, BigInt)] = {
      val mappedRanges: Seq[(BigInt, BigInt)] = originalRanges.map(range => (range._1 >> indexReduction, (range._2 >> indexReduction)))

      val distinct = mappedRanges.distinct
      var previousEnd: BigInt = null

      // Filter out regions that only span 1 value, and are already included in another region, so basically duplicates
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

  class ConfigurableSpatialPartitionerReduceZ(val indexReduction:Int = 2) extends PartitionerIndex[SpatialKey] {
    // Identical to ConfigurableSpatialPartitioner but indexReduction is applied on toZ()'s output.
    // This allows you to specify the maximum amount of records in powers of two rather than powers of four.
    private def toZ(key: SpatialKey): Z2 = Z2(key.col, key.row)

    def toIndex(key: SpatialKey): BigInt = toZ(key).z >> indexReduction

    def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] = {
      val originalZRanges: Seq[IndexRange] = Z2.zranges(ZRange(toZ(keyRange._1), toZ(keyRange._2)))
      val originalRanges: Seq[(BigInt, BigInt)] = originalZRanges.map(t => (BigInt.long2bigInt(t.lower), BigInt.long2bigInt(t.upper)))
      return ConfigurableSpatialPartitionerReduceZ.mapIndexRangeWithReduction(originalRanges, indexReduction)
    }
  }

  class ConfigurableSpaceTimePartitioner ( val indexReduction:Int = SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION, val keyIndex: KeyIndex[SpaceTimeKey] = SfCurveZSpaceTimeKeyIndex.byDay(null) )  extends PartitionerIndex[SpaceTimeKey] {

    // No matter if keyIndex is by day or by month, the indexReduction decides the maximum records per partition.
    // A higher temporal resolution means a lower spatial resolution and vice versa.
    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> indexReduction

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)
      return ConfigurableSpatialPartitionerReduceZ.mapIndexRangeWithReduction(originalRanges, indexReduction)
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


  def retryForever[R](delay: Duration, attempts: Int = 20, onAttemptFailed: Exception => Unit = _ => ())(f: => R): R = {
    var lastException: Exception = null
    var countDown = attempts
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

  def parseToInclusiveTemporalInterval(from_datetime: String, until_datetime: String): (ZonedDateTime, ZonedDateTime) = {
    // exclusive "until" becomes inclusive "to", with backwards compatibility

    val from = ZonedDateTime.parse(from_datetime, ISO_OFFSET_DATE_TIME)
    val until = ZonedDateTime.parse(until_datetime, ISO_OFFSET_DATE_TIME)

    val to =
      if (from isEqual until) { // include end day
        val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
        until.toLocalDate.atTime(endOfDay).toZonedDateTime
      } else until minusNanos 1

    (from, to)
  }
}
