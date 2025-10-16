package org.openeo.geotrellis

import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, Formats, StringInput}

object SimpleJson {

  /**
   * Note: This will not keep the order of elements
   */
  def parse(jsonStr: String): scala.collection.Map[String, Any] = {

    implicit val formats: Formats = org.json4s.DefaultFormats.lossless

    org.json4s.jackson.JsonMethods.parse(StringInput(jsonStr)).extract[scala.collection.Map[String, Any]]
  }

  /**
   * Note: Numbers wil be saved in scientific notation. 0.000005 becomes 5E-6
   */
  def serialize(map: scala.collection.Map[String, Any]): String = {
    implicit val formats: Formats = org.json4s.DefaultFormats.lossless

    JsonMethods.pretty(JsonMethods.render(Extraction.decompose(map)))
  }
}
