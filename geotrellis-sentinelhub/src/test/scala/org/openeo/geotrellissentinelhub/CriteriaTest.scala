package org.openeo.geotrellissentinelhub

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConverters._

class CriteriaTest {
  private val objectMapper = new ObjectMapper

  @Test
  def queryPropertiesForSatOrbitState(): Unit = {
    val metadata_properties = Map(
      "sat:orbit_state" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map(
      "sat:orbit_state" -> Map("eq" -> "descending").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties))
  }

  @Test
  def queryPropertiesForSatOrbitDirection(): Unit = {
    val metadata_properties = Map(
      "orbitDirection" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map(
      "sat:orbit_state" -> Map("eq" -> "descending").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties))
  }

  @Test
  def queryPropertiesForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava
    ).asJava

    val expected = Map(
      "eo:cloud_cover" -> Map("lte" -> 20).asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties)

    assertEquals(expected, actual)
    assertEquals("""{"eo:cloud_cover":{"lte":20}}""", objectMapper.writeValueAsString(actual))
  }

  @Test
  def dataFiltersForSatOrbitState(): Unit = {
    val metadata_properties = Map(
      "sat:orbit_state" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map("orbitDirection" -> "descending").asJava

    assertEquals(expected, Criteria.toDataFilters(metadata_properties))
  }

  @Test
  def dataFiltersForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava
    ).asJava

    val expected = Map("maxCloudCoverage" -> 20).asJava

    val actual = Criteria.toDataFilters(metadata_properties)

    assertEquals(expected, actual)
    assertEquals("""{"maxCloudCoverage":20}""", objectMapper.writeValueAsString(actual))
  }
}
