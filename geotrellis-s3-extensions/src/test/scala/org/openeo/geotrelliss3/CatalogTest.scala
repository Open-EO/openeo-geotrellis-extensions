package org.openeo.geotrelliss3

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import org.junit.{Ignore, Test}
import org.junit.Assume.{assumeFalse => skipTestIf}

class CatalogTest {

  val catalog = Catalog("Sentinel-2", "Level-2A")

  val date = ZonedDateTime.of(LocalDate.of(2020, 3, 1), MIDNIGHT, UTC)

  @Test
  def testQueryOnePage(): Unit = {
    try {
      val results = catalog.query(date, date, ulx = 0, uly = 70, brx = 40, bry = 20)
      val count = catalog.count(date, date, ulx = 0, uly = 70, brx = 40, bry = 20)

      assert(results.length == count)
    } catch {
      case CatalogException(message, _) => skipTestIf(message, message contains "Bad Gateway")
    }
  }

  @Test
  def testQueryMultiplePages(): Unit = {
    try {
      val results = catalog.query(date, date, ulx = 0, uly = 70, brx = 80, bry = 20)
      val count = catalog.count(date, date, ulx = 0, uly = 70, brx = 80, bry = 20)

      assert(results.length == count)
    } catch {
      case CatalogException(message, _) => skipTestIf(message, message contains "Bad Gateway")
    }
  }

  @Ignore
  @Test
  def testQueryTileIds() {
    val tileIds = Seq("39TVN", "39TWN")

    val results = catalog.query(date, date, tileIds)

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
  def testGetTileId() {
    assert("55UGV" == CatalogEntry("S2B_MSIL2A_20191001T005649_N0213_R088_T55UGV_20191001T030835").getTileId)
  }
}
