package org.openeo.geotrellissentinelhub

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, Feature}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.{LocalDate, ZoneOffset}
import java.util

class MadeToMeasureCatalogApiTest {
  private val catalogApi: CatalogApi = new MadeToMeasureCatalogApi

  private val collectionId = "dem"
  private val geometry = Extent(2.59003, 51.069, 2.8949, 51.2206).toPolygon()
  private val geometryCrs = LatLng
  private val from = LocalDate.of(2022, 1, 1).atStartOfDay(ZoneOffset.UTC)
  private val to = from plusDays 2
  private val accessToken = "s3cr3t"
  private val queryProperties = util.Collections.emptyMap[String, util.Map[String, Any]]()

  @Test
  def search(): Unit = {
    val actualFeatures = catalogApi.search(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)

    val expectedFeatures = Map(
      "0" -> Feature(geometry, from),
      "1" -> Feature(geometry, from plusDays 1),
      "2" -> Feature(geometry, from plusDays 2)
    )

    assertEquals(expectedFeatures, actualFeatures)
  }

  @Test
  def dateTimes(): Unit = {
    val actualDateTimes =
      catalogApi.dateTimes(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)

    val expectedDateTimes = Seq(
      from,
      from plusDays 1,
      from plusDays 2
    )

    assertEquals(expectedDateTimes, actualDateTimes)
  }
}
