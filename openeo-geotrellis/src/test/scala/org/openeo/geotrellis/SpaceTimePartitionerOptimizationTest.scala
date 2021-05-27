import java.time.{LocalDate, ZoneId, ZonedDateTime}

import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, SpaceTimeKey}
import geotrellis.proj4.{CRS, LatLng, Proj4Transform}
import geotrellis.raster.{ByteConstantNoDataCellType, CellSize}
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert._
import org.junit.Test
import org.openeo.geotrellis.layers.FileLayerProvider
import org.openeo.geotrelliscommon
import org.openeo.geotrelliscommon._
import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex

import scala.collection.immutable
import scala.reflect.ClassTag

class SpaceTimePartitionerOptimizationTest {

  val utm31 = CRS.fromEpsgCode(32631)

  val coords: (Double, Double) = Proj4Transform(LatLng,utm31)(5.072050,51.217270)
  val myExtent = ProjectedExtent(Extent(coords._1,coords._2,coords._1+10*4*256,coords._2+10*4*256),utm31)

  @Test
  def testBasicProperties(): Unit ={

    import geotrelliscommon.SpaceTimeByMonthPartitioner

    val minTime = ZonedDateTime.parse("2015-01-01T00:00:00Z")
    val maxTime = ZonedDateTime.now

    //construct metadata in the same way we do it in practice
    val metadata = FileLayerProvider.layerMetadata(myExtent, ZonedDateTime.parse("2015-01-01T00:00:00Z"), ZonedDateTime.parse("2015-01-10T00:00:00Z"), 12, ByteConstantNoDataCellType, new FloatingLayoutScheme(256, 256), CellSize(10, 10))

    //implicit val index: geotrelliscommon.SpaceTimeByMonthPartitioner.type = SpaceTimeByMonthPartitioner
    val partitioner = SpacePartitioner(metadata.bounds)
    val minPartition = partitioner.getPartition(metadata.bounds.get.minKey)
    val maxPartition = partitioner.getPartition(metadata.bounds.get.maxKey)

    //bounds is 4 tiles wide
    val maxCol = metadata.bounds.get.maxKey.col
    val minCol = metadata.bounds.get.minKey.col
    assertEquals(4,maxCol-minCol)
    val maxRow = metadata.bounds.get.maxKey.row
    val minRow = metadata.bounds.get.minKey.row
    assertEquals(4,maxRow-minRow)

    //this shows that the current default generates quite a few partititons
    assertEquals(0, minPartition)
    assertEquals(5, maxPartition)
    assertEquals(6,partitioner.regions.length)

    assertEquals(SpaceTimeByMonthPartitioner,partitioner.index)
    val matrix: Array[Array[Int]] = Array.ofDim(5,5)
    for (row: Int <- minRow to maxRow) {
      for (col: Int <- minCol to maxCol) {
        val minPart = partitioner.getPartition(SpaceTimeKey(col, row, minTime))
        println(s"${col}-${row}")
        matrix(row-minRow)(col-minCol) = minPart
        println(minPart)

      }
    }
    //print to show partitioning scheme
    matrix foreach { row => row foreach{x => print(s"${x} - ")}; println }
  }

  @Test
  def testPropertiesPartitionFullYear(): Unit ={

    val minTime = ZonedDateTime.parse("2015-01-01T00:00:00Z")
    val maxTime = ZonedDateTime.now
    //construct metadata in the same way we do it in practice
    val metadata = FileLayerProvider.layerMetadata(myExtent, minTime, maxTime, 12, ByteConstantNoDataCellType, new FloatingLayoutScheme(256, 256), CellSize(10, 10))

    //bounds is 4 tiles wide
    val maxCol = metadata.bounds.get.maxKey.col
    val minCol = metadata.bounds.get.minKey.col
    assertEquals(4,maxCol-minCol)
    val maxRow = metadata.bounds.get.maxKey.row
    val minRow = metadata.bounds.get.minKey.row
    assertEquals(4,maxRow-minRow)

    implicit val index: PartitionerIndex[SpaceTimeKey] = new SFCurveSpaceTimeIndex(SfCurveZSpaceTimeKeyIndex.byYears(metadata.bounds.get, 1000),1)

    val partitioner = SpacePartitioner[SpaceTimeKey](metadata.bounds.get)(null,ClassTag(SpaceTimeKey.getClass),index)
    val minPartition = partitioner.getPartition(metadata.bounds.get.minKey)
    val maxPartition = partitioner.getPartition(metadata.bounds.get.maxKey)

    //we only have 25 spatial keys (5x5), so 25 is max number of partitions
    assertEquals(0, minPartition)
    assertEquals(14, maxPartition)
    assertEquals(15,partitioner.regions.length)
    assertEquals(index, partitioner.index)
    val matrix: Array[Array[Int]] = Array.ofDim(5,5)
    for (row: Int <- minRow to maxRow) {
      for (col: Int <- minCol to maxCol) {
        val minPart = partitioner.getPartition(SpaceTimeKey(col, row, minTime))
        val maxPart = partitioner.getPartition(SpaceTimeKey(col, row, maxTime))
        println(s"${col}-${row}")
        matrix(row-minRow)(col-minCol) = minPart
        println(minPart)
        assertEquals(minPart,maxPart)

      }
    }
    //print to show partitioning scheme
    matrix foreach { row => row foreach{x => print(s"${x} - ")}; println }

  }

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