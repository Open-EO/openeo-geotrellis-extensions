package org.openeo.geotrellissentinelhub;

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.{LocalDate, ZoneId}

class CatalogApiTest {
  private val catalogApi = new CatalogApi
  private val utc = ZoneId.of("UTC")

  private def accessToken: String = new AuthApi().authenticate(Utils.clientId, Utils.clientSecret).access_token

  @Test
  def testSearch(): Unit = {
    val dateTimes = catalogApi.dateTimes(
      collectionId = "sentinel-1-grd",
      ProjectedExtent(Extent(16.162995875210488, 48.305237663134704, 16.198050293067634, 48.328618668560985), LatLng),
      from = LocalDate.of(2020, 11, 5).atStartOfDay(utc),
      to = LocalDate.of(2020, 11, 7).atStartOfDay(utc),
      accessToken
    )

    println(dateTimes)

    val availableDates = dateTimes
      .sortWith(_ isBefore _)
      .map(_.toLocalDate)
      .distinct

    val expectedDates = Seq(LocalDate.of(2020, 11, 5), LocalDate.of(2020, 11, 6))
    assertEquals(expectedDates, availableDates)
  }
}
