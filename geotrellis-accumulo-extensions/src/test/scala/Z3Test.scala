import java.time.{LocalDate, ZoneId}

import geotrellis.layer.SpaceTimeKey
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.junit.{BeforeClass, Test}
import org.openeo.geotrellisaccumulo.{SpaceTimeByMonthPartitioner, SpaceTimeByMonthSfCurvePartitioner}

class Z3Test {

  private val start = SpaceTimeKey(0, 0, LocalDate.of(2015, 1, 1).atStartOfDay(ZoneId.systemDefault()))
  private val end = SpaceTimeKey(2000, 2000, LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.systemDefault()))

  @Test
  def testOriginalRanges(): Unit = {
    val time = System.currentTimeMillis()

    val ranges = SpaceTimeByMonthPartitioner.keyIndex.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - time}ms")


    val time2 = System.currentTimeMillis()

    val ranges2 = SpaceTimeByMonthSfCurvePartitioner.originalRanges(start, end)

    println(s"${System.currentTimeMillis() - time2}ms")


    assertNotEquals(ranges, ranges2)
  }

  @Test
  def testFinalRanges(): Unit = {
    val time = System.currentTimeMillis()

    val ranges = SpaceTimeByMonthPartitioner.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - time}ms")


    val time2 = System.currentTimeMillis()

    val ranges2 = SpaceTimeByMonthSfCurvePartitioner.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - time2}ms")


    assertEquals(ranges, ranges2)
  }
}
