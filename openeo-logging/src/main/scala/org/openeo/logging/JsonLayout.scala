package org.openeo.logging

import io.circe.syntax._
import org.apache.log4j.Layout
import org.apache.log4j.spi.LocationInfo.NA
import org.apache.log4j.spi.LoggingEvent

import java.io.IOException
import java.lang.management.ManagementFactory
import java.nio.file.Paths

object JsonLayout {
  private lazy val pid =
    try Some(ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toInt)
    catch {
      case _: NumberFormatException =>
        try Some(Paths.get("/proc/self").toRealPath().getFileName.toString.toInt)
        catch {
          case _: IOException => None
        }
    }

  private lazy val userId = Option(System.getenv("OPENEO_USER_ID"))
  private lazy val batchJobId = Option(System.getenv("OPENEO_BATCH_JOB_ID"))
}

class JsonLayout extends Layout {
  import JsonLayout._

  override def format(event: LoggingEvent): String = {
    val baseLogEntry = Map(
      "created" -> (event.getTimeStamp / 1000).asJson,
      "name" -> event.getLoggerName.asJson,
      "filename" -> event.getLocationInformation.getFileName.asJson,
      "levelname" -> event.getLevel.toString.asJson,
      "message" -> event.getRenderedMessage.asJson
    )

    val withPossibleLineNumber =
      if (event.getLocationInformation.getLineNumber == NA) baseLogEntry
      else baseLogEntry + ("lineno" -> event.getLocationInformation.getLineNumber.toInt.asJson)

    val withPossiblePid = pid.foldLeft(withPossibleLineNumber) { case (logEntry, pid) =>
      logEntry + ("process" -> pid.asJson)
    }

    val withPossibleException = Option(event.getThrowableStrRep).foldLeft(withPossiblePid) {
      case (logEntry, stackTrace) => logEntry + ("exc_info" -> stackTrace.mkString("\n").asJson)
    }

    val withPossibleUserId = userId.foldLeft(withPossibleException) { case (logEntry, id) =>
      logEntry + ("user_id" -> id.asJson)
    }

    val withBatchJobId = batchJobId.foldLeft(withPossibleUserId) { case (logEntry, id) =>
      logEntry + ("job_id" -> id.asJson)
    }

    withBatchJobId
      .asJson.noSpaces
  }

  override def ignoresThrowable(): Boolean = false

  override def activateOptions(): Unit = ()
}
