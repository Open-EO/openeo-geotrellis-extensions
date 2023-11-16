package geopyspark.geotrellis

import _root_.io.circe.parser.parse
import _root_.io.circe.syntax._
import cats.syntax.either._
import geotrellis.raster.histogram._

object Json {

  def writeHistogram(hist: Histogram[_]): String = hist match {
    case h: FastMapHistogram =>
      h.asInstanceOf[Histogram[Int]].asJson.noSpaces
    case h: StreamingHistogram =>
      h.asInstanceOf[Histogram[Double]].asJson.noSpaces
    case _ =>
      throw new IllegalArgumentException(s"Unable to write $hist as JSON.")
  }

  def readHistogram(hist: String): Histogram[_] = {
    val json = parse(hist).valueOr(throw _)
    if (json.isObject) json.as[Histogram[Double]].valueOr(throw _)
    else if (json.isArray) json.as[Histogram[Int]].valueOr(throw _)
    else throw new AssertionError(hist)
  }
}
