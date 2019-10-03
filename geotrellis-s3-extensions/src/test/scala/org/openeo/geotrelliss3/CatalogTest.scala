package org.openeo.geotrelliss3

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

import org.junit.Test

class CatalogTest {

  val catalog = Catalog("Sentinel-2", "Level-2A")

  val startDate = ZonedDateTime.of(LocalDate.of(2019, 10, 1), MIDNIGHT, UTC)
  val endDate = ZonedDateTime.of(LocalDate.of(2019, 10, 2), MIDNIGHT, UTC)

  @Test
  def testQueryOnePage() {
    val results = catalog.query(startDate, endDate, ulx = 0, uly = 55, brx = 10, bry = 50)
    val count = catalog.count(startDate, endDate, ulx = 0, uly = 55, brx = 10, bry = 50)

    assert(count > 0 && count <= 100)
    assert(results.length == count)
  }

  @Test
  def testQueryMultiplePages() {
    val results = catalog.query(startDate, endDate, ulx = 0, uly = 60, brx = 20, bry = 40)
    val count = catalog.count(startDate, endDate, ulx = 0, uly = 60, brx = 20, bry = 40)

    assert(count > 100)
    assert(results.length == count)
  }

  @Test
  def testQueryTileIds() {
    val tileIds = Seq("55UGV", "56VNL")

    val results = catalog.query(startDate, endDate, tileIds)

    assert(results.length == 2)
  }

  @Test
  def testGetS3Bucket() {
    assert("dsd-s2-2019-10" == CatalogEntry("S2B_MSIL2A_20191001T005649_N0213_R088_T55UGV_20191001T030835").getS3bucket)
  }

  @Test
  def testGetS3Key() {
    val productId = "S2B_MSIL2A_20191001T005649_N0213_R088_T55UGV_20191001T030835"
    assert(s"tiles/55/U/GV/$productId.SAFE" == CatalogEntry(productId).getS3Key)
  }

  @Test
  def testGetTileNb() {
    assert("55UGV" == CatalogEntry("S2B_MSIL2A_20191001T005649_N0213_R088_T55UGV_20191001T030835").getTileNb)
  }
}
