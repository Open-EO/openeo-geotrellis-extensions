package org.openeo.geotrellissentinelhub;

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, Feature, ProjectedExtent}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.{LocalDate, ZoneId, ZonedDateTime}

class CatalogApiTest {
  private val endpoint = "https://services.sentinel-hub.com"
  private val catalogApi = new CatalogApi(endpoint)
  private val utc = ZoneId.of("UTC")

  private def accessToken: String = new AuthApi().authenticate(Utils.clientId, Utils.clientSecret).access_token

  @Test
  def dateTimes(): Unit = {
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

  @Test
  def searchCard4L(): Unit = {
    import geotrellis.vector._

    val bbox = ProjectedExtent(Extent(35.666439, -6.23476, 35.861576, -6.075694), LatLng)

    val features = catalogApi.searchCard4L(
      collectionId = "sentinel-1-grd",
      bbox,
      from = LocalDate.of(2021, 2, 1).atStartOfDay(utc),
      to = LocalDate.of(2021, 2, 17).atTime(23, 59, 59).atZone(utc),
      accessToken
    ) // gives you 3 features

    val (id, Feature(geometry, datetime)) = features.head

    val intersection = geometry intersection bbox.extent.toPolygon
    println(intersection.toGeoJson())

    val timeRange = Seq(datetime, datetime plusSeconds 1)

    println(id)

    // next: launch batch requests with:
    //  geometry == intersection(bbox, feature.geometry)
    //  timeRange == [feature.properties.datetime, feature.properties.datetime + 1s]
    //  description: "card4lId: xyz"
  }
}
