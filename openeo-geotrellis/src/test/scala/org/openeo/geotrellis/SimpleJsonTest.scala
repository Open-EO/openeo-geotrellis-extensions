package org.openeo.geotrellis

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SimpleJsonTest {
  @Test
  def testParse(): Unit = {
    val jsonStr = "{\"key1\": \"value1\", \"key2\": 2}"
    val map = SimpleJson.parse(jsonStr)
    assert(map("key1") == "value1")
    assert(map("key2") == 2)
  }

  @Test
  def testSerialize(): Unit = {
    val map = Map("key1" -> "value1", "key2" -> 2)
    val jsonStr = SimpleJson.serialize(map)
        assertEquals(jsonStr, "{\n  \"key1\" : \"value1\",\n  \"key2\" : 2\n}")
  }
}
