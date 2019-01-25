package org.openeo.geotrellisvlm

import geotrellis.raster.render.{ColorMap, RGBA}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object ColorMapParser {

  def parse(fileName: String): ColorMap = {
    val elemList = new ListBuffer[(Int, Int)]
    
    var opacity = "1.0"

    val opacityRegex = """<Opacity>(.*)</Opacity>""".r
    val colorRegex = """<ColorMapEntry color="#([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})" quantity="(\d*)"(?:.*opacity="(.*)")?.*""".r
    
    for (line <- Source.fromFile(fileName).getLines()) {
      line.trim match {
        case opacityRegex(o) => opacity = o  
        case colorRegex(r, g, b, value, o) => 
          val a = Option(o).getOrElse(opacity).toDouble * 100
          elemList += ((value.toInt, RGBA(hexToInt(r), hexToInt(g), hexToInt(b), a)))
        case _ =>
      }
    }

    def hexToInt(hex: String): Int = {
      Integer.parseInt(hex, 16)
    }
    
    ColorMap(Map(elemList:_*))
  }
}
