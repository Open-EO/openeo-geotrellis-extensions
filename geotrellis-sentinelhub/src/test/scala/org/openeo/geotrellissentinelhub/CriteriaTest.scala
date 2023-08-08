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

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd"))
  }

  @Test
  def queryPropertiesForOrbitDirection(): Unit = {
    val metadata_properties = Map(
      "orbitDirection" -> Map("eq" -> ("descending": Any)).asJava
    ).asJava

    val expected = Map(
      "sat:orbit_state" -> Map("eq" -> "descending").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd"))
  }

  @Test
  def queryPropertiesForSarPolarization(): Unit = {
    val metadata_properties = Map(
      "sar:polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map(
      "s1:polarization" -> Map("eq" -> "DV").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd"))
  }

  @Test
  def queryPropertiesForPolarization(): Unit = {
    val metadata_properties = Map(
      "polarization" -> Map("eq" -> ("DV": Any)).asJava
    ).asJava

    val expected = Map(
      "s1:polarization" -> Map("eq" -> "DV").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd"))
  }

  @Test
  def queryPropertiesForSarInstrumentMode(): Unit = {
    val metadata_properties = Map(
      "sar:instrument_mode" -> Map("eq" -> ("IW": Any)).asJava
    ).asJava

    val expected = Map(
      "sar:instrument_mode" -> Map("eq" -> "IW").asJava
    ).asJava

    assertEquals(expected, Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd"))
  }

  @Test
  def queryPropertiesForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava
    ).asJava

    val expected = Map(
      "eo:cloud_cover" -> Map("lte" -> 20).asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-2-l2a")

    assertEquals(expected, actual)
    assertEquals("""{"eo:cloud_cover":{"lte":20}}""", objectMapper.writeValueAsString(actual))
  }

  @Test(expected=classOf[IllegalArgumentException])
  def acceptOnlyLteQueryPropertiesForEoCloudCover(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("eq" -> (20: Any)).asJava
    ).asJava

    Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-2-l2a")
  }


  @Test
  def queryPropertiesForTimelinessS1(): Unit = {
    val metadata_properties = Map(
      "timeliness" -> Map("eq" -> ("NRT3h": Any)).asJava
    ).asJava

    val expected = Map(
      "s1:timeliness" -> Map("eq" -> "NRT3h").asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd")

    assertEquals(expected, actual)
  }

  @Test
  def queryPropertiesForTimelinessS5P(): Unit = {
    val metadata_properties = Map(
      "timeliness" -> Map("eq" -> ("OFFL": Any)).asJava
    ).asJava

    val expected = Map(
      "s5p:timeliness" -> Map("eq" -> "OFFL").asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-5p-l2")

    assertEquals(expected, actual)
  }

  @Test
  def queryPropertiesForResolution(): Unit = {
    val metadata_properties = Map(
      "resolution" -> Map("eq" -> ("HIGH": Any)).asJava
    ).asJava

    val expected = Map(
      "s1:resolution" -> Map("eq" -> "HIGH").asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd")

    assertEquals(expected, actual)
  }

  @Test
  def queryPropertiesForOrbitId(): Unit = {
    val metadata_properties = Map(
      "orbit_id" -> Map("eq" -> (0: Any)).asJava
    ).asJava

    val expected = Map(
      "sat:absolute_orbit" -> Map("eq" -> 0).asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-5p-l2")

    assertEquals(expected, actual)
  }

  @Test
  def queryPropertiesForType(): Unit = {
    val metadata_properties = Map(
      "type" -> Map("eq" -> ("CO": Any)).asJava
    ).asJava

    val expected = Map(
      "s5p:type" -> Map("eq" -> "CO").asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-5p-l2")

    assertEquals(expected, actual)
  }

  @Test
  def ignoreQueryPropertiesForProviderBackend(): Unit = {
    val metadata_properties = Map(
      "eo:cloud_cover" -> Map("lte" -> (20: Any)).asJava,
      "provider:backend" -> Map("eq" -> ("vito": Any)).asJava,
      "federation:backends" -> Map("eq" -> ("vito": Any)).asJava
    ).asJava

    val expected = Map(
      "eo:cloud_cover" -> Map("lte" -> 20).asJava
    ).asJava

    val actual = Criteria.toQueryProperties(metadata_properties, collectionId = "sentinel-1-grd")

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
      "provider:backend" -> Map("eq" -> ("vito": Any)).asJava,
      "federation:backends" -> Map("eq" -> ("vito": Any)).asJava
    ).asJava

    val expected = Map("maxCloudCoverage" -> 20).asJava

    val actual = Criteria.toDataFilters(metadata_properties)

    assertEquals(expected, actual)
    assertEquals("""{"maxCloudCoverage":20}""", objectMapper.writeValueAsString(actual))
  }
}
