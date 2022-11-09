package org.openeo.logging

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode
import org.apache.log4j.spi.{LocationInfo, LoggingEvent}
import org.apache.log4j.{Layout, Level, Logger}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}
import org.slf4j.{LoggerFactory, MDC}

class JsonLayoutTest {
  private val jsonLayout: Layout = new JsonLayout

  @Before
  def initializeMdc(): Unit = {
    MDC.put(JsonLayout.UserId, "vdboschj")
    MDC.put(JsonLayout.RequestId, "r-def456")
    MDC.put(JsonLayout.JobId, "j-abc123")
  }

  @After
  def clearMdc(): Unit = MDC.clear()

  @Test
  def testLogger(): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    logger.error(s"It was the best of times.", new IllegalArgumentException("expected"))
  }

  @Test
  def testFormat(): Unit = {
    val logger = Logger.getLogger(getClass)
    val timestamp = 1638526627123L
    val message = "It was the blorst of times."

    val fqnOfCategoryClass = null
    val throwable = null
    val threadName = null
    val ndc = null
    val locationInfo = new LocationInfo("JsonLayoutTest.scala", "org.openeo.logging.JsonLayoutTest", "testFormat", "33")
    val properties = null

    val loggingEvent = new LoggingEvent(fqnOfCategoryClass, logger, timestamp, Level.INFO, message, threadName,
      throwable, ndc, locationInfo, properties)

    val logLine = jsonLayout.format(loggingEvent)

    assertTrue(s"$logLine doesn't end with a newline", logLine.endsWith(System.lineSeparator()))
    assertTrue(s"""$logLine uses scientific notation for "created"""", logLine contains "1638526627.123")

    print(logLine)
    val logEntry = decode[Map[String, Json]](logLine).valueOr(throw _)

    assertEquals("org.openeo.logging.JsonLayoutTest", logEntry("name").asString.get)
    logEntry("process").asNumber.flatMap(_.toInt).get
    assertEquals("INFO", logEntry("levelname").asString.get)
    assertEquals(message, logEntry("message").asString.get)
    assertEquals(1638526627.123, logEntry("created").asNumber.map(_.toDouble).get, 0.001)
    assertEquals("JsonLayoutTest.scala", logEntry("filename").asString.get)
    assertEquals(33, logEntry("lineno").asNumber.flatMap(_.toInt).get)
    assertEquals("vdboschj", logEntry("user_id").asString.get)
    assertEquals("r-def456", logEntry("req_id").asString.get)
    assertEquals("j-abc123", logEntry("job_id").asString.get)
  }


  @Test
  def testLevelnameIsAlignedWithPythonLogging(): Unit = {
    def loggingEvent(level: Level) = {
      val logger = Logger.getLogger(getClass)
      val timestamp = 1662106067123L
      val message = level.toString.toLowerCase

      new LoggingEvent(null, logger, timestamp, level, message, null)
    }

    def testLevelname(inputLevel: Level, outputLevelname: String): Unit = {
      val logLine = jsonLayout.format(loggingEvent(inputLevel))
      val logEntry = decode[Map[String, Json]](logLine).valueOr(throw _)

      assertEquals(outputLevelname, logEntry("levelname").asString.get)
    }

    testLevelname(Level.FATAL, "CRITICAL")
    testLevelname(Level.ERROR, "ERROR")
    testLevelname(Level.WARN, "WARNING")
    testLevelname(Level.INFO, "INFO")
    testLevelname(Level.DEBUG, "DEBUG")
    testLevelname(Level.TRACE, "DEBUG")
  }
}
