package org.openeo.logging

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode
import org.apache.logging.log4j.Level._
import org.apache.logging.log4j.LogManager
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.{After, Before, Rule, Test}
import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import scala.annotation.meta.getter
import scala.io.Source

class OpenEOJsonLogLayoutTest {
  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder
  private def tempLogFile: File = new File(temporaryFolder.getRoot, "openeo.log")

  private val logger = LoggerFactory.getLogger(getClass)

  @Before
  def setupLoggingContext(): Unit = {
    MDC.put("logFile", tempLogFile.getAbsolutePath)
    MDC.put("mdc.user_id", "vdboschj")
    MDC.put("mdc.req_id", "r-def456")
    MDC.put("mdc.job_id", "j-abc123")
  }

  @After
  def clearLoggingContext(): Unit = MDC.clear()

  @Test
  def testJsonLogging(): Unit = {
    val now = System.currentTimeMillis() / 1000.0
    val message = "It was the best of times."

    logger.warn(message, new Exception("It was the blorst of times."))

    val in = Source.fromFile(tempLogFile)

    try {
      val Seq(logLine) = in.getLines().toSeq

      val logEntry = decode[Map[String, Json]](logLine).valueOr(throw _)

      assertEquals(getClass.getName, logEntry("name").asString.get)
      // TODO: support PID (it's an int but a "pattern" resolver with pattern "%pid" will return a string)
      assertEquals("WARNING", logEntry("levelname").asString.get)
      assertEquals(message, logEntry("message").asString.get)
      assertEquals(now, logEntry("created").asNumber.map(_.toDouble).get, 1.0)
      assertEquals("OpenEOJsonLogLayoutTest.scala", logEntry("filename").asString.get)
      assertEquals(40, logEntry("lineno").asNumber.flatMap(_.toInt).get)
      val stackTrace = logEntry("exc_info").asString.get
      assertTrue(stackTrace.contains("java.lang.Exception: It was the blorst of times"))
      assertTrue(stackTrace.contains(getClass.getName))
      assertEquals("vdboschj", logEntry("user_id").asString.get)
      assertEquals("r-def456", logEntry("req_id").asString.get)
      assertEquals("j-abc123", logEntry("job_id").asString.get)
    } finally in.close()
  }

  @Test
  def testLevelnameIsAlignedWithPythonLogging(): Unit = {
    val log4jLogger = LogManager.getLogger(getClass)

    val log4jToPythonMapping = Seq(
      FATAL -> "CRITICAL",
      ERROR -> "ERROR",
      WARN -> "WARNING",
      INFO -> "INFO",
      DEBUG -> "DEBUG",
      TRACE -> "DEBUG"
    )

    for ((log4jLevel, _) <- log4jToPythonMapping) {
      log4jLogger.log(log4jLevel, log4jLevel.toString.toLowerCase())
    }

    val in = Source.fromFile(tempLogFile)

    try {
      val actualLevelNames = for {
        line <- in.getLines().toSeq
        logEntry = decode[Map[String, Json]](line).valueOr(throw _)
        levelname <- logEntry("levelname").asString
      } yield levelname

      val expectedLevelNames = log4jToPythonMapping.map { case (_, levelname) => levelname }

      assertEquals(expectedLevelNames, actualLevelNames)
    } finally in.close()
  }
}
