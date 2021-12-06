package org.openeo.geotrellissentinelhub

import _root_.io.circe._
import _root_.io.circe.generic.auto._
import _root_.io.circe.parser._
import _root_.io.circe.syntax._
import cats.syntax.either._

import geotrellis.vector._

import java.time.ZonedDateTime

object BatchProcessContext {
  private implicit val batchProcessContextEncoder: Encoder[BatchProcessContext] =
    Encoder.encodeJson.contramap[BatchProcessContext] {
      case context: Sentinel2L2aBatchProcessContext => context.asJson
      case context: Sentinel1GrdBatchProcessContext => context.asJson
      case context =>
        throw new IllegalStateException(s"unknown BatchProcessContext type ${context.getClass.getName}: $context")
    }

  // TODO: does this method make sense?
  def fromJson[T: Decoder](json: String): T = decode[T](json).valueOr(throw _)
}

trait BatchProcessContext {
  def toJson: String = this.asJson.noSpaces
  def includesNarrowRequest: Boolean
}

case class Sentinel2L2aBatchProcessContext(bandNames: Seq[String], incompleteTiles: Option[Seq[(String, Geometry)]],
                                           lower: Option[ZonedDateTime], upper: Option[ZonedDateTime],
                                           missingBandNames: Option[Seq[String]]) extends BatchProcessContext {
  override def includesNarrowRequest: Boolean = {
    val result = incompleteTiles.isDefined

    assert(lower.isDefined == result)
    assert(upper.isDefined == result)
    assert(missingBandNames.isDefined == result)

    result
  }
}

// TODO: reduce code duplication with BatchProcessContext
case class Sentinel1GrdBatchProcessContext(bandNames: Seq[String], backCoeff: String, orthorectify: Boolean,
                                           demInstance: String, incompleteTiles: Option[Seq[(String, Geometry)]],
                                           lower: Option[ZonedDateTime], upper: Option[ZonedDateTime],
                                           missingBandNames: Option[Seq[String]]) extends BatchProcessContext {
  override def includesNarrowRequest: Boolean = {
    val result = incompleteTiles.isDefined

    assert(lower.isDefined == result)
    assert(upper.isDefined == result)
    assert(missingBandNames.isDefined == result)

    result
  }
}
