package org.openeo.geotrelliscommon

import io.circe.generic.auto._
import org.junit.Assert.assertEquals
import org.junit.Test
import org.openeo.geotrelliscommon.CirceException.decode

object CirceExceptionTest {
  case class SomeObject(someProperty: String)
}

class CirceExceptionTest {
  import CirceExceptionTest._

  @Test
  def canDecode(): Unit = {
    val Right(someObject) = decode[SomeObject](json = """{"someProperty": "someValue"}""")
    assertEquals("someValue", someObject.someProperty)
  }
}
