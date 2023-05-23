package org.openeo.logging

import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.junit.Assert.assertTrue
import org.junit.{AfterClass, Before, BeforeClass, Test}
import org.slf4j.{LoggerFactory, MDC}

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.io.Source

object OpenEOBatchJobJsonLogLayoutTest {
  private var loggerContext: LoggerContext = _
  private val logger = LoggerFactory.getLogger(classOf[OpenEOBatchJobJsonLogLayoutTest])
  private lazy val temporaryFolder: String = {
    val _temporaryFolder = new File("tmp/" + UUID.randomUUID().toString).getAbsolutePath
    Files.createDirectories(Paths.get(_temporaryFolder))
    _temporaryFolder
  }

  @BeforeClass
  def initializeLog4j(): Unit = loggerContext = {
    // Do a hard replacement of variables in the XML to avoid influence of thread context
    val origXmlPath = getClass.getResource("/log4j2-batch.xml").getPath
    val newXmlPath = new File(temporaryFolder, "log4j2-batch-tmp.xml").toString

    val fileSource = Source.fromFile(origXmlPath)
    val origXmlString = try fileSource.mkString
    finally fileSource.close()

    val newXmlString = origXmlString.replace("${ctx:logFile}", new File(temporaryFolder, "openeo.log").toString)

    val out = new FileWriter(newXmlPath)
    try out.write(newXmlString)
    finally out.close()

    Configurator.initialize(null, newXmlPath)
  }

  @AfterClass
  def shutDownLog4j(): Unit = Configurator.shutdown(loggerContext)
}

class OpenEOBatchJobJsonLogLayoutTest {
  import OpenEOBatchJobJsonLogLayoutTest._

  private def tempLogFile: File = new File(temporaryFolder, "openeo.log")

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

  @Test
  def testError(): Unit = {
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
