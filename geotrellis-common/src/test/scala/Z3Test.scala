import java.time.{LocalDate, ZoneId}

import org.openeo.geotrelliscommon.zcurve.SfCurveZSpaceTimeKeyIndex
import geotrellis.layer.SpaceTimeKey
import geotrellis.store.index.zcurve.ZSpaceTimeKeyIndex
import org.junit.{Ignore, Test}

@Ignore
class Z3Test {

  private val start = SpaceTimeKey(0, 0, LocalDate.of(2015, 1, 1).atStartOfDay(ZoneId.systemDefault()))
  private val end = SpaceTimeKey(1000, 1000, LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.systemDefault()))

  @Test
  def compareRangesForDifferentMaxRecurseSettings(): Unit = {
    println(s"Current implementation")
    println()

    val cTime = System.currentTimeMillis()

    val cRanges = ZSpaceTimeKeyIndex.byDay(null).indexRanges(start, end)

    println(s"${System.currentTimeMillis() - cTime}ms")
    println(s"Size: ${cRanges.size}")


    println(s"SfCurve implementation")
    println()

    for (maxRecurse <- 7 to 15) {
      println(s"MaxRecurse $maxRecurse")

      val time = System.currentTimeMillis()

      val ranges = SfCurveZSpaceTimeKeyIndex.byDay(null).indexRanges((start, end), maxRecurse)

      println(s"${System.currentTimeMillis() - time}ms")
      println(s"Size: ${ranges.size}")
      println(s"Equals: ${ranges.equals(cRanges)}")
      println()
    }
  }
}
