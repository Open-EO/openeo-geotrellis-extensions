package org.openeo.geotrellis

import _root_.geotrellis.raster._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.IOUtils

import java.net.URL
import java.nio.charset.Charset
import java.util


package object testutil {

  def fromUrl(url: URL): Seq[Tile] => Seq[Tile] = {
    val graphPath = IOUtils.toString(url, Charset.defaultCharset())
    fromString(graphPath)
  }

  def fromString(graphPath: String): Seq[Tile] => Seq[Tile] = {
    val visitor = (new GeotrellisTileProcessGraphVisitor).create()
    val graph = new ObjectMapper().readValue(graphPath, classOf[util.Map[String, Object]])
    visitor.acceptProcessGraph(graph)
    val transformation = visitor.builder.generateFunction()
    transformation
  }
}
