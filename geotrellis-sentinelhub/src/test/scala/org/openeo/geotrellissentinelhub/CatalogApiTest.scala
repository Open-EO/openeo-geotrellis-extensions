package org.openeo.geotrellissentinelhub

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.time.{LocalDate, ZoneId}

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

  @Test
  def searchCard4LPaged(): Unit = {
    val bbox = ProjectedExtent(Extent(6.611, 45.665, 13.509, 51.253), LatLng)

    val features = catalogApi.searchCard4L(
      collectionId = "sentinel-1-grd",
      bbox,
      from = LocalDate.of(2021, 1, 6).atStartOfDay(utc),
      to = LocalDate.of(2021, 1, 25).atTime(23, 59, 59).atZone(utc),
      accessToken
    )

    assertTrue(s"number of features ${features.size} should exceed default page size 10", features.size > 10)
  }

  @Test
  def dateTimesForUnknownCollection(): Unit =
    try {
      catalogApi.dateTimes(
        collectionId = "some-unknown-collection",
        ProjectedExtent(Extent(16.162995875210488, 48.305237663134704, 16.198050293067634, 48.328618668560985), LatLng),
        from = LocalDate.of(2020, 11, 5).atStartOfDay(utc),
        to = LocalDate.of(2020, 11, 7).atStartOfDay(utc),
        accessToken
      )
    } catch {
      case e: SentinelHubException => assertTrue(e.getMessage, e.getMessage contains "Collection not found")
    }

  @Test(expected = classOf[SentinelHubException])
  def searchCard4LWithUnknownQueryProperty(): Unit =
      catalogApi.searchCard4L(
        collectionId = "sentinel-1-grd",
        ProjectedExtent(Extent(6.611, 45.665, 13.509, 51.253), LatLng),
        from = LocalDate.of(2021, 1, 6).atStartOfDay(utc),
        to = LocalDate.of(2021, 1, 25).atTime(23, 59, 59).atZone(utc),
        accessToken,
        queryProperties = Map("someUnknownProperty" -> "???")
      )
}
