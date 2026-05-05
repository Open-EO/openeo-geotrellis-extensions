package org.openeo.geotrellissentinelhub

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Collections
import scala.jdk.CollectionConverters._

class Cql2TextFormatterTest {
  private val cql2TextFormatter = new Cql2TextFormatter

  @Test
  def formatNone(): Unit = {
    val cql2Text = cql2TextFormatter.format(queryProperties = Collections.emptyMap())
    assertEquals("", cql2Text)
  }

  @Test
  def formatSingleString(): Unit = {
    val queryProperties = Collections.singletonMap("sat:orbit_state", Collections.singletonMap("eq", "ascending": Any))
    val cql2Text = cql2TextFormatter.format(queryProperties)

    assertEquals("sat:orbit_state = 'ascending'", cql2Text)
  }

  @Test
  def formatSingleNumber(): Unit = {
    val queryProperties = Collections.singletonMap("sat:absolute_orbit", Collections.singletonMap("eq", 0: Any))
    val cql2Text = cql2TextFormatter.format(queryProperties)

    assertEquals("sat:absolute_orbit = 0", cql2Text)
  }

  @Test
  def formatMultipleProperties(): Unit = {
    val queryProperties = Map(
      "s5p:type" -> Map("eq" -> ("CO": Any)).asJava,
      "sat:absolute_orbit" -> Map("eq" -> (0: Any)).asJava
    ).asJava

    val cql2Text = cql2TextFormatter.format(queryProperties)

    assertEquals("s5p:type = 'CO' and sat:absolute_orbit = 0", cql2Text)
  }
}
