package org.openeo.logging

import io.circe.{Encoder, Json, JsonNumber}
import io.circe.syntax._
import org.apache.log4j.{Layout, Level}
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

  private implicit val encodeDouble: Encoder[Double] = (d: Double) =>
    Json.fromJsonNumber(JsonNumber.fromDecimalStringUnsafe(f"$d%.3f"))
}

class JsonLayout extends Layout {
  import JsonLayout._

  override def format(event: LoggingEvent): String = {
    def pythonLevel(level: Level): String = level match {
      case Level.FATAL => "CRITICAL"
      case Level.WARN => "WARNING"
      case Level.TRACE => "DEBUG"
      case _ => level.toString
    }

    val baseLogEntry = Map(
      "created" -> (event.getTimeStamp / 1000.0).asJson,
      "name" -> event.getLoggerName.asJson,
      "filename" -> event.getLocationInformation.getFileName.asJson,
      "levelname" -> pythonLevel(event.getLevel).asJson,
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
      .asJson.noSpaces + System.lineSeparator()
  }

  override def ignoresThrowable(): Boolean = false

  override def activateOptions(): Unit = ()
}
