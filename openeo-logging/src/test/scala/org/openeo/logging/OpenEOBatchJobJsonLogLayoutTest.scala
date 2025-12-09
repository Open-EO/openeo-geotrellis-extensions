package org.openeo.logging

import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, BeforeAll, BeforeEach, Test}
import org.slf4j.{LoggerFactory, MDC}
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables
import uk.org.webcompere.systemstubs.jupiter.{SystemStub, SystemStubsExtension}

import java.io.File
import java.nio.file.{Files, Path}

object OpenEOBatchJobJsonLogLayoutTest {
  private var loggerContext: LoggerContext = _
  private val logger = LoggerFactory.getLogger(classOf[OpenEOBatchJobJsonLogLayoutTest])

  @BeforeAll
  def initializeLog4j(): Unit = loggerContext = Configurator.initialize(null, "classpath:log4j2-batch.xml")

  @AfterAll
  def shutDownLog4j(): Unit = Configurator.shutdown(loggerContext)
}

@ExtendWith(Array(classOf[SystemStubsExtension]))
class OpenEOBatchJobJsonLogLayoutTest {
  import OpenEOBatchJobJsonLogLayoutTest._

  var tempLogFile: File = _

  @SystemStub
  val environmentVariables = new EnvironmentVariables

  @BeforeEach
  def setupLogFile(@TempDir temporaryFolder: Path): Unit = {
    tempLogFile = new File(Files.createTempDirectory("logs").toFile, "openeo.log")
    environmentVariables.set("LOG_FILE", tempLogFile.getAbsolutePath)
  }

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

    assertTrue(executorLogEntries.nonEmpty, s"${executorLogEntries.size}")
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
