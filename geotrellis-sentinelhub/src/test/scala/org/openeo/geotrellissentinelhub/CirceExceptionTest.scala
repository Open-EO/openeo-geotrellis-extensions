package org.openeo.geotrellissentinelhub

import io.circe.generic.auto._
import io.circe.parser.{decode => circeDecode}
import io.circe.{DecodingFailure, ParsingFailure}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.openeo.geotrellissentinelhub.BatchProcessingApi.GetBatchProcessResponse
import org.openeo.geotrelliscommon.CirceException
import org.openeo.geotrelliscommon.CirceException.decode
import org.slf4j.LoggerFactory

class CirceExceptionTest {
  private implicit val logger = LoggerFactory.getLogger(getClass)

  @Test
  def assertCirceDecodeContainsNoStackTrace(): Unit = {
    val Left(circeError) = circeDecode[GetBatchProcessResponse](input = "")
    circeError.printStackTrace()

    assertTrue("expected an empty stack trace in the Circe error", circeError.getStackTrace.isEmpty)
  }

  @Test
  def testParsingExceptionContainsErrorContext(): Unit = {
    val Left(circeException) = decode[GetBatchProcessResponse](json = "")

    circeException.printStackTrace()

    assertTrue("expected this test class in the stack trace",
      circeException.getStackTrace.exists(stackTraceElement => stackTraceElement.getClassName == this.getClass.getName))

    val parsingFailure = circeException.getCause.asInstanceOf[ParsingFailure]
    val rootCause = parsingFailure.getCause

    assertEquals("exhausted input", rootCause.getMessage)
  }

  @Test
  def testDecodingExceptionContainsErrorContext(): Unit = {
    val Left(circeException) = decode[GetBatchProcessResponse](json = "{}")

    circeException.printStackTrace()

    assertTrue("expected this test class in the stack trace",
      circeException.getStackTrace.exists(stackTraceElement => stackTraceElement.getClassName == this.getClass.getName))

    assertTrue(s"expected a ${classOf[DecodingFailure].getName}", circeException.getCause.isInstanceOf[DecodingFailure])
  }

  @Test(expected = classOf[CirceException])
  def testFailingRetriesReturnOriginalExceptionInsteadOfFailsafeException(): Unit = {
    withRetries(context = "testFailingRetriesReturnOriginalExceptionInsteadOfFailsafeException") {
      throw new CirceException("expected", cause = null)
    }
  }
}
