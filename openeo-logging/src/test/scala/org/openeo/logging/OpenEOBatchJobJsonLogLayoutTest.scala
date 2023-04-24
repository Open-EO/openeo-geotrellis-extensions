package org.openeo.logging

import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.junit.Assert.assertTrue
import org.junit.rules.TemporaryFolder
import org.junit.{AfterClass, Before, BeforeClass, Rule, Test}
import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import scala.annotation.meta.getter

object OpenEOBatchJobJsonLogLayoutTest {
  private var loggerContext: LoggerContext = _
  private val logger = LoggerFactory.getLogger(classOf[OpenEOBatchJobJsonLogLayoutTest])

  @BeforeClass
  def initializeLog4j(): Unit = loggerContext = Configurator.initialize(null, "classpath:log4j2-batch.xml")

  @AfterClass
  def shutDownLog4j(): Unit = Configurator.shutdown(loggerContext)
}

class OpenEOBatchJobJsonLogLayoutTest {
  import OpenEOBatchJobJsonLogLayoutTest._

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder

  private def tempLogFile: File = new File(temporaryFolder.getRoot, "openeo.log")

  @Before
  def setupLogFile(): Unit = MDC.put("logFile", tempLogFile.getAbsolutePath)

  @Test
  def testJsonLogging(): Unit = {
    logger.info("some batch job log")

    val logEntries = Helpers.logEntries(tempLogFile)

    assertTrue(logEntries.exists { logEntry => logEntry("message").asString.contains("some batch job log") })
    assertTrue(logEntries.forall { logEntry =>
      logEntry("user_id").asString.contains("vdboschj") && logEntry("job_id").asString.contains("j-abc123")
    })
  }

  @Test
  def testLoggingContextPropagationToExecutors(): Unit = {
    val logFile = tempLogFile

    val sc = new SparkContext(master = "local[1]", appName = getClass.getName)

    try {
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
    assertTrue(executorLogEntries.forall { logEntry =>
      logEntry("user_id").asString.contains("vdboschj") && logEntry("job_id").asString.contains("j-abc123")
    })
  }
}
