package org.openeo.geotrellis

import org.apache.commons.io.IOUtils
import org.junit.Test
import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, ShortArrayTile, ShortConstantNoDataCellType$, Tile}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}

import java.util
import java.util.{Arrays, Collections, HashMap, Map}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

class OpenEOProcessGraphBuilderTest {

  private def dummyMap(keys: String*): util.HashMap[String, AnyRef] = {
    val m = new util.HashMap[String, AnyRef]
    for (key <- keys) {
      m.put(key, "dummy")
    }
    m
  }


  @Test
  def createMultiply(): Unit = {
    val visitor = new GeotrellisTileProcessGraphVisitor().create()
    val builder = new OpenEOProcessScriptBuilder
    builder.defaultDataParameterName_$eq("data")
    builder.defaultInputDataType_$eq(ByteConstantNoDataCellType.toString)
    visitor.builder.defaultDataParameterName_$eq("data")
    visitor.builder.defaultInputDataType_$eq(ByteConstantNoDataCellType.toString)
    val args = dummyMap("x", "y")
    val operator = "multiply"
    builder.expressionStart(operator, args)
    visitor.enterProcess(operator,args)
    builder.argumentStart("x")
    visitor.enterArgument("x")
    builder.fromParameter("data")
    visitor.fromParameter("data")
    builder.argumentEnd()
    visitor.leaveArgument()
    builder.constantArgument("y", 10)
    visitor.constantArgument("y", 10)
    assertEquals(visitor.builder.processStack, builder.processStack)
    assertEquals(visitor.builder.typeStack.head("x"), builder.typeStack.head("x"))
    val yTypeBuilder = builder.typeStack.head("y")
    assertNotNull(yTypeBuilder)
    val yTypeVisitor = visitor.builder.typeStack.head("y")
    assertNotNull(yTypeVisitor)
    builder.expressionEnd(operator, args)
    visitor.leaveProcess(operator, args)
    val transformationBuilder = builder.generateFunction()
    val transformationVisitor = visitor.builder.generateFunction()
    val tile1 = fillByteArrayTile(3, 3, 9, -10, 11, 12)
    val tile2 = fillByteArrayTile(3, 3, 5, 6, 7, 8)
    val tiles = ArrayBuffer(tile1,tile2)
    val resultBuilder = transformationBuilder.apply(tiles)
    val resultVisitor = transformationVisitor.apply(tiles)
    assertTileEquals(resultBuilder.head,resultVisitor.head)
    assertTileEquals(resultVisitor.apply(1),resultBuilder.apply(1))
  }

  def fillByteArrayTile(cols: Int, rows: Int, values: Int *):ByteArrayTile = {
    val tile = ByteArrayTile.ofDim(cols, rows);
    for (i <- 0 until  Math.min(cols * rows, values.length)){
      tile.set(i % cols, i / cols, values(i));
    }
    tile;
  }

  def assertTileEquals(expected: Tile, actual: Tile):Unit = {
    assertEquals(1,1)
    assertEquals(expected.cols, actual.cols)
    assertEquals(expected.rows, actual.rows)
    assertEquals(expected.cellType, actual.cellType)
    assert(expected.toArray sameElements  actual.toArray)
  }

  def fillShortArrayTile(cols: Int, rows: Int, values: Int *)= {
    val tile: ShortArrayTile = ShortArrayTile.ofDim(cols, rows)
    for (i <- 0 until Math.min(cols * rows, values.length)) {
      tile.set(i % cols, i / cols, values(i))
    }
    tile
  }


  @Test
  def testDereferenceNode(): Unit = {
    val graphPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderGraph.json"))
    val expectedPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderDereference.json"))
    val processgraph = new ObjectMapper().readValue(graphPath, classOf[util.Map[String, Any]])
    val expected = new ObjectMapper().readValue(expectedPath, classOf[util.Map[String, Any]])
    val visitor = (new GeotrellisTileProcessGraphVisitor).create()
    val graph = processgraph.get("process_graph") match{
      case m:util.Map[String,Object] => m
    }
    val topNodeName = visitor.dereferenceFromNodeArguments(graph)
    assert(topNodeName == "divide1")
    val topNode = graph.get(topNodeName).asInstanceOf[util.Map[String,Any]]
    assert(topNode.getOrDefault("result",false) == true)
    val result = graph == expected
    assert(result)
  }



  @Test
  def testAcceptDict(): Unit ={
    val graphPath = IOUtils.toString(getClass.getResource("/org/openeo/geotrellis/ProcessGraphBuilderGraph.json"))
    val visitor = (new GeotrellisTileProcessGraphVisitor).create()
    val graph = new ObjectMapper().readValue(graphPath,classOf[util.Map[String,Object]])
    visitor._acceptDict(graph)
    val processes = visitor.processes
    assert(processes.size==4)
    for (process <- processes)
      print(process)


    assert(true)
  }

}
