import java.time.{LocalDate, ZoneId, ZonedDateTime}

import geotrellis.layer.{KeyBounds, SpaceTimeKey}
import geotrellis.spark.partition.SpacePartitioner
import org.junit.Test
import org.openeo.geotrelliscommon._
import org.junit.Assert._

import scala.collection.immutable

class SpaceTimePartitionerOptimizationTest {

  @Test
  def testKeyIndexPreservesOrdering(){


    val allKeys: immutable.Seq[SpaceTimeKey] = generateKeys

    var previousIndex  = 0L
    for(key <- allKeys){
      //println(key)
      val index = SpaceTimeByMonthPartitioner.keyIndex.toIndex(key).toLong
      //println(index)
      assertTrue(index > previousIndex)
      previousIndex = index
    }

  }

  private def generateKeys = {
    val startDate = LocalDate.of(2018,2,2).atStartOfDay(ZoneId.systemDefault())
    for (f <- 0 to 100) yield SpaceTimeKey(10 + f, 0 + f, startDate.plusDays(f))
  }

  /**

   */
  @Test
  def testPartitionIndicesPreserveOrdering(): Unit ={

    val allKeys: immutable.Seq[SpaceTimeKey] = generateKeys

    val partitioner = SpacePartitioner(KeyBounds(allKeys(0),allKeys.last))

    partitioner.getPartition(SpaceTimeKey(16,6,LocalDate.of(2018,2,8).atStartOfDay(ZoneId.systemDefault())))

    var previousIndex  = -1L
    for(key <- allKeys){
      val partition = partitioner.getPartition(key)
      //println(partition)
      //test if an absolute ordering is maintained, this is important when using this partitioner as a range partitioner
      assertTrue(partition >= previousIndex)
      previousIndex = partition
    }
  }
}