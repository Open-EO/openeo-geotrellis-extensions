package org.openeo.geotrellis

import java.time.LocalDate

import _root_.io.circe.parser.decode
import _root_.io.circe.generic.auto._
import _root_.io.circe._
import cats.syntax.either._

object TimeSeriesServiceResponses {
  private implicit val decodeLocalDate: Decoder[LocalDate] = Decoder.decodeString.map(LocalDate.parse)
  private implicit val decodeAverage: Decoder[Double] = Decoder.decodeJson.map(json => json.asNumber.map(_.toDouble).getOrElse(Double.NaN)) // average can be "NaN" as well
  private implicit val decodeLocalDateKey: KeyDecoder[LocalDate] = KeyDecoder.decodeKeyString.map(LocalDate.parse)

  object GeometryMeans {
    case class Result(average: Double)
    case class DailyResult(date: LocalDate, result: Result)
    case class Response(results: Seq[DailyResult])

    def parse(json: String): Response = decode[Response](json).valueOr(throw _)
  }

  object GeometriesMeans {
    case class Result(average: Double)
    case class Response(results: Map[LocalDate, Seq[Result]])

    def parse(json: String): Response = decode[Response](json).valueOr(throw _)
  }

  object GeometriesHistograms {
    case class Bin(value: Double, count: Long)
    type Histogram = Array[Bin]
    case class Response(results: Map[LocalDate, Array[Histogram]])

    def parse(json: String): Response = decode[Response](json).valueOr(throw _)
  }
}
