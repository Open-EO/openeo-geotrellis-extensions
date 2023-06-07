package org.openeo.logging

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode
import org.apache.logging.log4j.Level._
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.junit.rules.TemporaryFolder
import org.junit.{After, AfterClass, Before, BeforeClass, Rule, Test}
import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import scala.annotation.meta.getter
import scala.io.Source

object OpenEOJsonLogLayoutTest {
  private var loggerContext: LoggerContext = _
  private val logger = LoggerFactory.getLogger(classOf[OpenEOJsonLogLayoutTest])

  @BeforeClass
  def initializeLog4j(): Unit = loggerContext = Configurator.initialize(null, "classpath:log4j2-sync.xml")

  @AfterClass
  def shutDownLog4j(): Unit = Configurator.shutdown(loggerContext)
}

class OpenEOJsonLogLayoutTest {
  import OpenEOJsonLogLayoutTest._

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder
  private def tempLogFile: File = new File(temporaryFolder.getRoot, "openeo.log")

  @(Rule @getter)
  val environmentVariables = new EnvironmentVariables

  @Before
  def setupLogFile(): Unit = environmentVariables.set("LOG_FILE", tempLogFile.getAbsolutePath)

  @Before
  def setupLoggingContext(): Unit = {
    MDC.put(JsonLayout.UserId, "vdboschj")
    MDC.put(JsonLayout.RequestId, "r-def456")
    MDC.put(JsonLayout.JobId, "j-abc123")
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
      assertEquals(60, logEntry("lineno").asNumber.flatMap(_.toInt).get) // Line number can change
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


    val actualLevelNames = for {
      logEntry <- Helpers.logEntries(tempLogFile)
      levelname <- logEntry("levelname").asString
    } yield levelname

    val expectedLevelNames = log4jToPythonMapping.map { case (_, levelname) => levelname }

    assertEquals(expectedLevelNames, actualLevelNames)
  }

  @Test
  def testLoggingContextPropagationToExecutors(): Unit = {
    val logFile = tempLogFile

    val sc = new SparkContext(master="local[1]", appName=getClass.getName, conf=new SparkConf())

    try {
      val userId = MDC.get(JsonLayout.UserId) // the driver's user ID
      sc.setLocalProperty(JsonLayout.UserId, userId) // this does the propagation

      sc.range(1, 100)
        .mapPartitions { is =>
          MDC.put("logFile", logFile.getAbsolutePath)
          logger.info("some executor log")
          is
        }
        .sum()
    } finally sc.stop()

    val executorLogEntries = Helpers.logEntries(logFile)
      .filter(logEntry => logEntry("message").asString contains "some executor log")

    assertTrue(s"${executorLogEntries.size}", executorLogEntries.nonEmpty)
    assertTrue(executorLogEntries.forall(logEntry => logEntry("user_id").asString contains "vdboschj"))
  }
}
