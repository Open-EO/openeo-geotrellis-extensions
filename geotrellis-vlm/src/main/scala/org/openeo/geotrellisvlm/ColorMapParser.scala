package org.openeo.geotrellisvlm

import java.io.InputStream
import java.lang.Integer.parseInt

import geotrellis.raster.render.{ColorMap, ColorRamp, RGBA}

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scala.io.Source.{fromFile, fromInputStream}

object ColorMapParser {

  def parse(fileName: String): ColorMap = {
    parse(fromFile(fileName))
  }
  
  def parse(input: InputStream): ColorMap = {
    parse(fromInputStream(input))
  }
  
  private def parse(source: BufferedSource) = {
    val colorBuffer = new ListBuffer[(Double, Int)]
    
    var opacity = "1.0"

    val opacityRegex = """<Opacity>(.*)</Opacity>""".r
    val colorRegex = """<ColorMapEntry color="#([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})([0-9A-Fa-f]{2})" quantity="(\d*)"(?:.*opacity="(.*)")?.*""".r
    
    var previousVal: Option[Int] = None
    var previousColor: Option[Int] = None
    
    for (line <- source.getLines()) {
      line.trim match {
        case opacityRegex(o) => opacity = o  
        case colorRegex(r, g, b, q, o) => addColorsToBuffer(r, g, b, q, o)
        case _ =>
      }
    }

    def addColorsToBuffer(r: String, g: String, b: String, q: String, o: String) {
      val value = q.toInt
      val a = Option(o).getOrElse(opacity).toDouble * 100
      val color = RGBA(hexToInt(r), hexToInt(g), hexToInt(b), a)
      
      previousVal match {
        case None => colorBuffer += ((value, color))
        case Some(v) =>
          val stops = value - v + 1
          val colors = ColorRamp(previousColor.get, color).stops(stops).colors

          for ((c, i) <- colors.zipWithIndex if i > 0) {
            colorBuffer += ((v + i, c))
          }
      }

      previousVal = Some(value)
      previousColor = Some(color)
    }

    def hexToInt(hex: String): Int = {
      parseInt(hex, 16)
    }
    
    ColorMap(colorBuffer:_*)
  }
}
