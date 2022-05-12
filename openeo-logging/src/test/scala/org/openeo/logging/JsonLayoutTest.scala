package org.openeo.logging

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{Layout, Level, Logger}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.slf4j.LoggerFactory

class JsonLayoutTest {

  @Test
  def testLogger(): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    logger.error(s"It was the best of times.", new IllegalArgumentException("expected"))
  }

  @Test
  def testFormat(): Unit = {
    val jsonLayout: Layout = new JsonLayout

    val logger = Logger.getLogger(getClass)
    val timestamp = 1638526627123L
    val message = "It was the blorst of times."

    val loggingEvent = new LoggingEvent(null, logger, timestamp, Level.INFO, message, null)
    val logLine = jsonLayout.format(loggingEvent)

    assertTrue(s"$logLine doesn't end with a newline", logLine.endsWith(System.lineSeparator()))
    assertTrue(s"""$logLine uses scientific notation for "created"""", logLine contains "1638526627.123")

    print(logLine)
    val logEntry = decode[Map[String, Json]](logLine).valueOr(throw _)

    // TODO: extend this
    assertEquals("org.openeo.logging.JsonLayoutTest", logEntry("name").asString.get)
    logEntry("process").asNumber.flatMap(_.toInt).get
    assertEquals("INFO", logEntry("levelname").asString.get)
    assertEquals(message, logEntry("message").asString.get)
    assertEquals(1638526627.123, logEntry("created").asNumber.map(_.toDouble).get, 0.001)
  }
}
