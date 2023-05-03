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
  def queryPropertiesForOrbitDirection(): Unit = {
    val metadata_properties = Map(
      "orbitDirection" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map(
      "sat:orbit_state" -> Map("eq" -> "descending").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties))
  }

  @Test
  def queryPropertiesForSarPolarization(): Unit = {
    val metadata_properties = Map(
      "sar:polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map(
      "polarization" -> Map("eq" -> "DV").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties))
  }

  @Test
  def queryPropertiesForPolarization(): Unit = {
    val metadata_properties = Map(
      "polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map(
      "polarization" -> Map("eq" -> "DV").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties))
  }

  @Test
  def queryPropertiesForSarInstrumentMode(): Unit = {
    val metadata_properties = Map(
      "sar:instrument_mode" -> Map("eq" -> ("IW": Any)).asJava
    ).asJava

    val expected = Map(
      "sar:instrument_mode" -> Map("eq" -> "IW").asJava
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

  @Test(expected=classOf[IllegalArgumentException])
  def acceptOnlyLteQueryPropertiesForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("eq" -> (20: Any)).asJava
    ).asJava

    Criteria.toQueryProperties(metadata_properties)
  }

  @Test
  def ignoreQueryPropertiesForProviderBackend(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava,
      "provider:backend" -> Map("eq" -> ("vito": Any)).asJava
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
  def dataFiltersForOrbitDirection(): Unit = {
    val metadata_properties = Map(
      "orbitDirection" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map("orbitDirection" -> "descending").asJava

    assertEquals(expected, Criteria.toDataFilters(metadata_properties))
  }

  @Test
  def dataFiltersForSarPolarization(): Unit = {
    val metadata_properties = Map(
      "sar:polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map("polarization" -> "DV").asJava

    assertEquals(expected, Criteria.toDataFilters(metadata_properties))
  }

  @Test
  def dataFiltersForPolarization(): Unit = {
    val metadata_properties = Map(
      "polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map("polarization" -> "DV").asJava

    assertEquals(expected, Criteria.toDataFilters(metadata_properties))
  }

  @Test
  def dataFiltersForSarInstrumentMode(): Unit = {
    val metadata_properties = Map(
      "sar:instrument_mode" -> Map("eq" -> ("IW": Any)).asJava
    ).asJava

    val expected = Map("acquisitionMode" -> "IW").asJava

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

  @Test(expected = classOf[IllegalArgumentException])
  def acceptOnlyLteDataFiltersForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("eq" -> (20: Any)).asJava
    ).asJava

    Criteria.toDataFilters(metadata_properties)
  }

  @Test
  def ignoreDataFiltersForProviderBackend(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava,
      "provider:backend" -> Map("eq" -> ("vito": Any)).asJava
    ).asJava

    val expected = Map("maxCloudCoverage" -> 20).asJava

    val actual = Criteria.toDataFilters(metadata_properties)

    assertEquals(expected, actual)
    assertEquals("""{"maxCloudCoverage":20}""", objectMapper.writeValueAsString(actual))
  }
}
