package org.openeo.geotrellissentinelhub

import _root_.io.circe._
import _root_.io.circe.generic.auto._
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import cats.syntax.either._
import io.circe.Decoder.Result

import java.time.{Instant, ZoneOffset}
import geotrellis.vector._

import java.time.ZonedDateTime

object BatchProcessContext {
  // TODO: why does main code require these homemade codecs but unit tests don't?
  private implicit object zonedDateTimeEncoder extends Encoder[ZonedDateTime] {
    override def apply(date: ZonedDateTime): Json = Json.fromLong(date.toInstant.toEpochMilli)
  }

  private implicit object zonedDateTimeDecoder extends Decoder[ZonedDateTime] {
    override def apply(c: HCursor): Result[ZonedDateTime] = c.as[Long]
        .map(millis => ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
  }

  def fromJson(json: String): BatchProcessContext = decode[BatchProcessContext](json).valueOr(throw _)
}

case class BatchProcessContext(bandNames: Seq[String], incompleteTiles: Option[Seq[(String, Geometry)]],
                               lower: Option[ZonedDateTime], upper: Option[ZonedDateTime],
                               missingBandNames: Option[Seq[String]]) {
  import BatchProcessContext._ // TODO: and what's the deal with this?

  def toJson: String  = this.asJson.noSpaces

  def includesNarrowRequest: Boolean = {
    val result = incompleteTiles.isDefined

    assert(lower.isDefined == result)
    assert(upper.isDefined == result)
    assert(missingBandNames.isDefined == result)

    result
  }
}
