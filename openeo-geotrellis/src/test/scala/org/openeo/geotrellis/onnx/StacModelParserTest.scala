package org.openeo.geotrellis.onnx

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class StacModelParserTest {


  private val validStac =
    """{
      |  "stac_version": "1.0.0",
      |  "stac_extensions": ["https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"],
      |  "type": "Feature",
      |  "id": "roadmap-segmentation-model-onnx",
      |  "bbox": [-180.0, -90.0, 180.0, 90.0],
      |  "geometry": null,
      |  "properties": {
      |    "datetime": "2026-06-29T00:00:00Z",
      |    "mlm:name": "Road Map Segmentation Model",
      |    "mlm:framework": [{"name": "onnx", "version": "1.15.0"}],
      |    "mlm:tasks": ["semantic-segmentation"],
      |    "mlm:input": [
      |      {
      |        "name": "input",
      |        "data_type": "float32",
      |        "shape": [25, 3, 64, 64],
      |        "axes": ["batch", "bands", "height", "width"]
      |      }
      |    ],
      |    "mlm:output": [
      |      {
      |        "name": "output",
      |        "data_type": "float32",
      |        "shape": [25, 10]
      |      }
      |    ],
      |    "mlm:classes": [
      |      {"value": 0, "name": "background"},
      |      {"value": 1, "name": "road"}
      |    ]
      |  },
      |  "links": [],
      |  "assets": {
      |    "model": {
      |      "href": "roadMapSegmentationModel.onnx",
      |      "type": "application/x-onnx",
      |      "roles": ["mlm:model"]
      |    }
      |  }
      |}""".stripMargin

  @Test
  def testParseModelName(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals("Road Map Segmentation Model", desc.modelName)
  }

  @Test
  def testParseFramework(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals(Some("onnx"), desc.framework)
  }

  @Test
  def testParseTasks(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals(Seq("semantic-segmentation"), desc.tasks)
  }

  @Test
  def testParseInput(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals(1, desc.inputs.size)
    val input = desc.inputs.head
    assertEquals("input", input.name)
    assertEquals("float32", input.dataType)
    assertEquals(Seq(25L, 3L, 64L, 64L), input.shape)
    assertEquals(Seq("batch", "bands", "height", "width"), input.axes)
  }

  @Test
  def testParseOutput(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals(1, desc.outputs.size)
    val output = desc.outputs.head
    assertEquals("output", output.name)
    assertEquals("float32", output.dataType)
    assertEquals(Seq(25L, 10L), output.shape)
  }

  @Test
  def testParseClasses(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals(2, desc.classes.size)
    assertEquals(MlmClass(0, "background"), desc.classes(0))
    assertEquals(MlmClass(1, "road"), desc.classes(1))
  }

  @Test
  def testParseModelAssetHref(): Unit = {
    val desc = StacModelParser.parse(validStac)
    assertEquals("roadMapSegmentationModel.onnx", desc.modelAssetHref)
  }

  @Test
  def testMissingMlmNameThrows(): Unit = {
    val badStac = validStac.replace(""""mlm:name": "Road Map Segmentation Model",""", "")
    assertThrows(classOf[IllegalArgumentException], () => StacModelParser.parse(badStac))
  }

  @Test
  def testMissingModelAssetThrows(): Unit = {
    val badStac = validStac.replace(""""roles": ["mlm:model"]""", """"roles": []""")
    assertThrows(classOf[IllegalArgumentException], () => StacModelParser.parse(badStac))
  }

  @Test
  def testDynamicShape(): Unit = {
    val stacDynamic = validStac.replace(
      """"shape": [25, 3, 64, 64]""",
      """"shape": [-1, 3, 64, 64]"""
    )
    val desc = StacModelParser.parse(stacDynamic)
    assertEquals(-1L, desc.inputs.head.shape.head)
  }
}
