package org.openeo.logging

import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.junit.Assert.assertTrue
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.junit.rules.TemporaryFolder
import org.junit._
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

  @(Rule @getter)
  val environmentVariables = new EnvironmentVariables

  @Before
  def setupLogFile(): Unit = environmentVariables.set("LOG_FILE", tempLogFile.getAbsolutePath)

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

  @Test
  def testErrorLoggedFromDifferentThread(): Unit = {
    val sc = new SparkContext(master = "local[1]", appName = getClass.getName)
    val data = sc.parallelize(Seq(1, 2, 3, 4, 5))
    try {
      data.map(x => {
        if (x == 3) {
          throw new Exception("Intentional exception")
        } else {
          x * 2
        }
      }).collect()
    } catch {
      case e: Exception =>
        println(e) // Ignore error
    }
    finally sc.stop()

    val executorLogEntries = Helpers.logEntries(tempLogFile)
    assertTrue(executorLogEntries.forall { logEntry =>
      logEntry("user_id").asString.contains("vdboschj") && logEntry("job_id").asString.contains("j-abc123")
    })
    assertTrue(executorLogEntries.exists { logEntry =>
      logEntry("name").asString.contains("org.apache.spark.scheduler.TaskSetManager")
    })
  }
}
