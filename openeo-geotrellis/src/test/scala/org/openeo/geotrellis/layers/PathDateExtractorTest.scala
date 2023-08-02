package org.openeo.geotrellis.layers

import nl.jqno.equalsverifier.EqualsVerifier
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.nio.file.Paths
import java.time.{LocalDate, ZoneId}

class PathDateExtractorTest {

  @Test
  def testSentinel5PDaily(): Unit = {
    val pathDateExtractor = Sentinel5PPathDateExtractor.Daily

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA_DEV/TERRASCOPE_Sentinel5P/L3_CO_TD_V100"))
    val expected = LocalDate.of(2019, 12, 18).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testSentinel5PMonthly(): Unit = {
    val pathDateExtractor = Sentinel5PPathDateExtractor.Monthly

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA_DEV/TERRASCOPE_Sentinel5P/L3_CO_TM_V100"))
    val expected = LocalDate.of(2020, 5, 1).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testSentinel2(): Unit = {
    val pathDateExtractor = SplitYearMonthDayPathDateExtractor

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA/TERRASCOPE_Sentinel2/NDVI_V2"))
    val expected = LocalDate.of(2020, 4, 24).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testProbaV(): Unit = {
    val pathDateExtractor = ProbaVPathDateExtractor

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA/PROBAV_C2/COG/PROBAV_L3_S10_TOC_333M"))
    val expected = LocalDate.of(2014, 4, 11).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testSigma0(): Unit = {
    val pathDateExtractor = SplitYearMonthDayPathDateExtractor

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1"))
    val expected = LocalDate.of(2018, 4, 24).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testCoherence(): Unit = {
    val pathDateExtractor = SplitYearMonthDayPathDateExtractor

    val dates = pathDateExtractor.extractDates(Paths.get("/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/"))
    val expected = LocalDate.of(2017, 4, 24).atStartOfDay(ZoneId.of("UTC"))

    assertTrue(dates contains expected)
  }

  @Test
  def testSuitableAsCacheKey(): Unit = {
    EqualsVerifier.forClass(classOf[Sentinel5PPathDateExtractor]).verify()

    assertEquals(ProbaVPathDateExtractor, ProbaVPathDateExtractor)
    assertEquals(ProbaVPathDateExtractor.hashCode(), ProbaVPathDateExtractor.hashCode())

    assertEquals(SplitYearMonthDayPathDateExtractor, SplitYearMonthDayPathDateExtractor)
    assertEquals(SplitYearMonthDayPathDateExtractor.hashCode(), SplitYearMonthDayPathDateExtractor.hashCode())
  }
}
