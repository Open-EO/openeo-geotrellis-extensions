package org.openeo.geotrellissentinelhub

import geotrellis.proj4.LatLng
import geotrellis.shapefile.ShapeFileReader
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{Ignore, Test}

import java.time.{LocalDate, ZoneId}
import java.util.Collections.{emptyMap, singletonMap}
import scala.collection.JavaConverters._

class DefaultCatalogApiTest {
  private val endpoint = "https://services.sentinel-hub.com"
  private val catalogApi = new DefaultCatalogApi(endpoint)
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
  def dateTimesWithFilter(): Unit = {
    val dateTimes = catalogApi.dateTimes(
      collectionId = "sentinel-1-grd",
      ProjectedExtent(Extent(16.162995875210488, 48.305237663134704, 16.198050293067634, 48.328618668560985), LatLng),
      from = LocalDate.of(2020, 11, 5).atStartOfDay(utc),
      to = LocalDate.of(2020, 11, 7).atStartOfDay(utc),
      accessToken,
      queryProperties = singletonMap("sat:orbit_state", singletonMap("eq", "ascending"))
    )

    println(dateTimes)

    val availableDates = dateTimes
      .sortWith(_ isBefore _)
      .map(_.toLocalDate)
      .distinct

    val expectedDates = Seq(LocalDate.of(2020, 11, 6))
    assertEquals(expectedDates, availableDates)
  }

  @Test
  def searchWithFilter(): Unit = {
    val features = catalogApi.search(
      collectionId = "sentinel-1-grd",
      geometry = Extent(16.162995875210488, 48.305237663134704, 16.198050293067634, 48.328618668560985).toPolygon(),
      geometryCrs = LatLng,
      from = LocalDate.of(2020, 11, 5).atStartOfDay(utc),
      to = LocalDate.of(2020, 11, 7).atStartOfDay(utc),
      accessToken,
      queryProperties = Map(
        "sat:orbit_state" -> singletonMap("eq", "ascending": Any),
        "s1:timeliness" -> singletonMap("eq", "NRT3h": Any)
      ).asJava
    )

    assertEquals(1, features.size)
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

    val (id, Feature(geometry, featureData)) = features.head

    val intersection = geometry intersection bbox.extent.toPolygon
    println(intersection.toGeoJson())

    val timeRange = Seq(featureData.dateTime, featureData.dateTime plusSeconds 1)

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

      fail("should have thrown a SentinelHubException")
    } catch {
      case SentinelHubException(_, 400, _, responseBody) if responseBody contains "Illegal collection" => /* expected */
    }

  @Test
  def dateTimesForPeculiarTimeInterval(): Unit = {
    try {
      catalogApi.dateTimes(
        collectionId = "sentinel-2-l2a",
        boundingBox = ProjectedExtent(Extent(11.89, 41.58, 12.56, 42.2), LatLng),
        from = LocalDate.of(2021, 12, 7).atStartOfDay(utc),
        to = LocalDate.of(2021, 12, 6).atTime(23, 59, 59, 999999999).atZone(utc),
        accessToken,
        queryProperties = emptyMap()
      )

      fail("should have thrown a SentinelHubException")
    } catch {
      case SentinelHubException(_, 400, _, responseBody)
        if responseBody contains "Cannot parse parameter `datetime`." => /* expected */
    }
  }

  @Test
  def searchCard4LWithUnknownQueryProperty(): Unit =
    try {
      catalogApi.searchCard4L(
        collectionId = "sentinel-1-grd",
        ProjectedExtent(Extent(6.611, 45.665, 13.509, 51.253), LatLng),
        from = LocalDate.of(2021, 1, 6).atStartOfDay(utc),
        to = LocalDate.of(2021, 1, 25).atTime(23, 59, 59).atZone(utc),
        accessToken,
        queryProperties = singletonMap("someUnknownProperty", singletonMap("eq", "???"))
      )

      fail("should have thrown a SentinelHubException")
    } catch {
      case SentinelHubException(_, 400, _, responseBody)
        if responseBody contains "Querying is not supported on property 'someUnknownProperty'" => /* expected */
    }

  @Ignore("not to be run automatically")
  @Test
  def debugReadTimedOut(): Unit = {
    val geometries = ShapeFileReader
      .readMultiPolygonFeatures("/data/users/Public/deroob/Fields_to_extract/Fields_to_extract_2021_30SVH_RAW_0.shp")
      .map(_.geom)

    assert(geometries.nonEmpty, "no MultiPolygons found in shapefile")

    val multiPolygon = simplify(geometries)
    println(s"simplified ${geometries.map(_.getNumGeometries).sum} polygons to ${multiPolygon.getNumGeometries}")

    val from = LocalDate.of(2020, 7, 1).minusDays(90).atStartOfDay(utc)
    val to = from plusWeeks 1 /*LocalDate.of(2022, 7, 1).plusDays(90).atStartOfDay(utc)*/

    val features = catalogApi.search(
      collectionId = "sentinel-1-grd",
      multiPolygon,
      LatLng,
      from,
      to,
      accessToken,
      queryProperties = emptyMap()
    )

    println(features.size)
  }

  @Ignore("test-drive CDSE Catalog API; not to be run automatically")
  @Test
  def testSearchCdseCatalog(): Unit = {
    val geometry = ProjectedExtent(Extent(5.027, 51.1974, 5.0438, 51.2213), LatLng)
    val from = LocalDate.of(2018, 8, 6).atStartOfDay(utc)
    val to = from plusDays 1

    val accessToken = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJYVUh3VWZKaHVDVWo0X3k4ZF8xM0hxWXBYMFdwdDd2anhob2FPLUxzREZFIn0.eyJleHAiOjE2ODI1MTE1NDcsImlhdCI6MTY4MjUxMDk0NywianRpIjoiN2JkOGYxYmYtMDQ0Mi00M2IxLThmZDktODQxNzgwMTY4YjQ2IiwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS5kYXRhc3BhY2UuY29wZXJuaWN1cy5ldS9hdXRoL3JlYWxtcy9DRFNFIiwic3ViIjoiMjIyNjRjYWUtODA3Mi00MzUxLWIxYjYtZDMyYzE3M2VjZTc5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoic2gtMDVmOTM5YjUtODQ0NS00ZDVkLWIwNjAtMTk4OGI0N2Q2MzZhIiwic2NvcGUiOiJlbWFpbCBwcm9maWxlIHVzZXItY29udGV4dCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiY2xpZW50SWQiOiJzaC0wNWY5MzliNS04NDQ1LTRkNWQtYjA2MC0xOTg4YjQ3ZDYzNmEiLCJjbGllbnRIb3N0IjoiMTkzLjE5MC4xODkuMTIiLCJvcmdhbml6YXRpb25zIjpbImRlZmF1bHQtOTc2MGU1OWItNTY2ZC00MmQxLWI2NWItMzRlZDE4NzlkYThhIl0sInVzZXJfY29udGV4dF9pZCI6IjYxNjEyZmM4LWFkNjQtNDU1ZC04YTFmLTViZmFlZTUwNGNhMCIsImNvbnRleHRfcm9sZXMiOnt9LCJjb250ZXh0X2dyb3VwcyI6WyIvb3JnYW5pemF0aW9ucy9kZWZhdWx0LTk3NjBlNTliLTU2NmQtNDJkMS1iNjViLTM0ZWQxODc5ZGE4YS8iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LXNoLTA1ZjkzOWI1LTg0NDUtNGQ1ZC1iMDYwLTE5ODhiNDdkNjM2YSIsInVzZXJfY29udGV4dCI6ImRlZmF1bHQtOTc2MGU1OWItNTY2ZC00MmQxLWI2NWItMzRlZDE4NzlkYThhIiwiY2xpZW50QWRkcmVzcyI6IjE5My4xOTAuMTg5LjEyIn0.ep31ee3wvui5oGcgUVj1ZjX5o2cfIWizr74SGiUIjFBk1OxoqQ4Ec5PM6KH_ITPooW7cT-0374Op5lj-0tWUcI9hFYb2kNd5cAddlPCz3fzL7fICK9Je_D3D4inV5ua4rmQDXiPcwGDJx0T6UzrDNgX18iOc12tz_3QCVH3nXmQqmL7OhHF0jDrF4BfjQGZa7UUjctltAJl4n08GLgfhEfoj9bcrH2YBuFqt62OOWQvXvGcLBtCX6-soHrJcNut_bZLVZAuWm1TEEIVRKGzmwFPvt_UScY_8daFs6UDoAOAG6NqZipl73F4TYYyCV22gsEWqpoqUfoPUjM7jYwLPoA"

    val cdseCatalog = new DefaultCatalogApi(endpoint = "https://sh.dataspace.copernicus.eu")
    val features = cdseCatalog.search(
      "sentinel-3-olci",
      geometry.extent.toPolygon(), geometry.crs,
      from, to,
      accessToken,
      queryProperties = emptyMap()
    )

    println(features.size)
  }
}
