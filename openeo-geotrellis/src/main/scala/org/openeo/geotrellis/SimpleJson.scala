package org.openeo.geotrellis

import org.json4s._
import org.json4s.jackson.JsonMethods._

object SimpleJson {

  /**
   * Note: This will not keep the order of elements
   */
  def parse(jsonStr: String): scala.collection.Map[String, Any] = {

    implicit val formats: Formats = org.json4s.DefaultFormats.lossless

    org.json4s.jackson.JsonMethods.parse(jsonStr).extract[scala.collection.Map[String, Any]]
  }

  /**
   * Note: Numbers wil be saved in scientific notation. 0.000005 becomes 5E-6
   */
  def serialize(map: scala.collection.Map[String, Any]): String = {
    implicit val formats: Formats = org.json4s.DefaultFormats.lossless

    pretty(render(Extraction.decompose(map)))
  }
}
