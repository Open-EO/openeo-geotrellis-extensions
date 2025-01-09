package org.openeo.geotrellis

import org.apache.commons.io.IOUtils
import org.junit.Test
import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.raster.{ByteArrayTile, Tile}

import java.util
import java.util.{Arrays, Collections, HashMap, Map}
import scala.collection.JavaConversions

class OpenEOProcessGraphBuilderTest {

  private def dummyMap(keys: String*) = {
    val m = new util.HashMap[String, AnyRef]
    for (key <- keys) {
      m.put(key, "dummy")
    }
    m
  }

  @Test
  def testDereferenceNode(): Unit = {
    val graphPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderGraph.json"))
    val expectedPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderDereference.json"))
    val graph = new ObjectMapper().readValue(graphPath, classOf[util.Map[String, Any]])
    val expected = new ObjectMapper().readValue(expectedPath, classOf[util.Map[String, Any]])
    val visitor = (new GeotrellisTileProcessGraphVisitor).create()
    val topNodeName = visitor.dereferenceFromNodeArguments(graph)
    assert(topNodeName == "divide1")
    val topNode:java.util.Map[String,Any] = graph.get(topNodeName).asInstanceOf[util.Map[String,Any]]
    assert(topNode.getOrDefault("result",false) == true)
    val result = graph == expected
    assert(result)
  }



  @Test
  def testAcceptNode(): Unit ={
    val graphPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderDereference.json"))
    val visitor = (new GeotrellisTileProcessGraphVisitor).create()
    val graph = new ObjectMapper().readValue(graphPath,classOf[util.Map[String,Any]])
    visitor.acceptNode(graph.get("divide1").asInstanceOf[util.Map[String,Any]])
  }

}
