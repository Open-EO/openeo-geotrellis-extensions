package org.openeo.logging

import cats.syntax.either._
import io.circe.Json
import io.circe.parser.decode

import java.io.File
import scala.io.Source

object Helpers {
  def logEntries(logFile: File): Vector[Map[String, Json]] = {
    val in = Source.fromFile(logFile)

    try {
      for {
        line <- in.getLines().toVector
        logEntry = decode[Map[String, Json]](line).valueOr(throw _)
      } yield logEntry
    } finally in.close()
  }
}
