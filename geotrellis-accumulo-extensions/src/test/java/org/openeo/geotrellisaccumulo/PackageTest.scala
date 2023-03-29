package org.openeo.geotrellisaccumulo

import org.junit.{Assert, Test}


class PackageTest {

  @Test
  def testTtlCache(): Unit = {

    val logs = collection.mutable.ArrayBuffer[Int]()
    val cache = new TtlCache[String, Int](ttl = 2)

    // Do calculation with side effect (for inspection)
    def calculate(x: Int): Int = {
      logs += x
      x
    }

    Assert.assertEquals(111, cache.getOrElseUpdate("foo", calculate(111)))
    Assert.assertArrayEquals(List(111).toArray, logs.toArray)

    Assert.assertEquals(111, cache.getOrElseUpdate("foo", calculate(222)))
    Assert.assertArrayEquals(List(111).toArray, logs.toArray)

    Assert.assertEquals(333, cache.getOrElseUpdate("bar", calculate(333)))
    Assert.assertArrayEquals(List(111, 333).toArray, logs.toArray)

    Assert.assertEquals(111, cache.getOrElseUpdate("foo", calculate(444)))
    Assert.assertArrayEquals(List(111, 333).toArray, logs.toArray)

    Thread.sleep(2100)

    Assert.assertEquals(555, cache.getOrElseUpdate("foo", calculate(555)))
    Assert.assertArrayEquals(List(111, 333, 555).toArray, logs.toArray)
  }
}