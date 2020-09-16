import java.time.{LocalDate, ZoneId}

import geotrellis.layer.SpaceTimeKey
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.junit.{Ignore, Test}
import org.openeo.geotrellisaccumulo.{SpaceTimeByMonthPartitioner, SpaceTimeByMonthSfCurvePartitioner}

@Ignore
class Z3Test {

  private val start = SpaceTimeKey(0, 0, LocalDate.of(2015, 1, 1).atStartOfDay(ZoneId.systemDefault()))
  private val end = SpaceTimeKey(1000, 1000, LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.systemDefault()))

  @Test
  def testOriginalRanges(): Unit = {
    val time = System.currentTimeMillis()

    val ranges = SpaceTimeByMonthPartitioner.keyIndex.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - time}ms")


    val time2 = System.currentTimeMillis()

    val ranges2 = SpaceTimeByMonthSfCurvePartitioner.keyIndex.indexRanges((start, end))

    println(s"${System.currentTimeMillis() - time2}ms")


    assertNotEquals(ranges, ranges2)
  }

  @Test
  def testFinalRanges(): Unit = {
    val time = System.currentTimeMillis()

    val ranges = SpaceTimeByMonthPartitioner.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - time}ms")


    val time2 = System.currentTimeMillis()

    val ranges2 = SpaceTimeByMonthSfCurvePartitioner.indexRanges((start, end))

    println(s"${System.currentTimeMillis() - time2}ms")


    assertEquals(ranges, ranges2)
  }

  @Test
  def compareRangesForDifferentMaxRecurseSettings(): Unit = {
    println(s"Current implementation")
    println()

    val cTime = System.currentTimeMillis()

    val cOriginalRanges = SpaceTimeByMonthPartitioner.keyIndex.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - cTime}ms")
    println(s"Size: ${cOriginalRanges.size}")


    val cTime2 = System.currentTimeMillis()

    val cFinalRanges = SpaceTimeByMonthPartitioner.indexRanges(start, end)

    println(s"${System.currentTimeMillis() - cTime2}ms")
    println(s"Size: ${cFinalRanges.size}")

    println(s"SfCurve implementation")
    println()

    for (maxRecurse <- 7 to 15) {
      println(s"MaxRecurse $maxRecurse")

      val time = System.currentTimeMillis()

      val originalRanges = SpaceTimeByMonthSfCurvePartitioner.keyIndex.indexRanges((start, end), maxRecurse)

      println(s"${System.currentTimeMillis() - time}ms")
      println(s"Size: ${originalRanges.size}")
      println(s"Equals: ${originalRanges.equals(cOriginalRanges)}")


      val time2 = System.currentTimeMillis()

      val finalRanges = SpaceTimeByMonthSfCurvePartitioner.indexRanges((start, end), maxRecurse)

      println(s"${System.currentTimeMillis() - time2}ms")
      println(s"Size: ${finalRanges.size}")
      println(s"Equals: ${finalRanges.equals(cFinalRanges)}")
      println()
    }
  }
}
