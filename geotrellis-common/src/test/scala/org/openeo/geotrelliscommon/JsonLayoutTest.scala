package org.openeo.geotrelliscommon

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Layout, Level, Logger}
import org.junit.Assert.assertEquals
import org.junit.Test

class JsonLayoutTest {

  private val logger = Logger.getLogger(classOf[JsonLayoutTest])

  @Test
  def testLogger(): Unit = {
    logger.error(s"It was the best of times.", new IllegalArgumentException("expected"))
  }

  @Test
  def testFormat(): Unit = {
    val jsonLayout: Layout = new JsonLayout

    val timestamp = 1638526627000L
    val message = "It was the blorst of times."

    val loggingEvent = new LoggingEvent(null, logger, timestamp, Level.INFO, message, null)
    val logLine = jsonLayout.format(loggingEvent)

    println(logLine)
    val logEntry = decode[Map[String, Json]](logLine).valueOr(throw _)

    // TODO: extend this
    assertEquals("org.openeo.geotrelliscommon.JsonLayoutTest", logEntry("name").asString.get)
    logEntry("process").asNumber.flatMap(_.toInt).get
    assertEquals("INFO", logEntry("levelname").asString.get)
    assertEquals(message, logEntry("message").asString.get)
    assertEquals(timestamp / 1000, logEntry("created").asNumber.flatMap(_.toLong).get)
  }
}
