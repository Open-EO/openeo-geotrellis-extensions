package org.openeo.geotrellissentinelhub

import io.circe.generic.auto._
import io.circe.parser.{decode => circeDecode}
import io.circe.{DecodingFailure, ParsingFailure}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.geotrellissentinelhub.BatchProcessingApi.GetBatchProcessResponse
import org.openeo.geotrelliscommon.CirceException
import org.openeo.geotrelliscommon.CirceException.decode
import org.slf4j.{Logger, LoggerFactory}

class CirceExceptionTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  @Test
  def assertCirceDecodeContainsNoStackTrace(): Unit = {
    val Left(circeError) = circeDecode[GetBatchProcessResponse](input = "")
    circeError.printStackTrace()

    assertTrue(circeError.getStackTrace.isEmpty, "expected an empty stack trace in the Circe error")
  }

  @Test
  def testParsingExceptionContainsErrorContext(): Unit = {
    val Left(circeException) = decode[GetBatchProcessResponse](json = "")

    circeException.printStackTrace()

    assertTrue(circeException.getStackTrace.exists(stackTraceElement => stackTraceElement.getClassName == this.getClass.getName), "expected this test class in the stack trace")

    val parsingFailure = circeException.getCause.asInstanceOf[ParsingFailure]
    val rootCause = parsingFailure.getCause

    assertEquals("exhausted input", rootCause.getMessage)
  }

  @Test
  def testDecodingExceptionContainsErrorContext(): Unit = {
    val Left(circeException) = decode[GetBatchProcessResponse](json = "{}")

    circeException.printStackTrace()

    assertTrue(
      circeException.getStackTrace.exists(stackTraceElement => stackTraceElement.getClassName == this.getClass.getName), "expected this test class in the stack trace")

    assertTrue(circeException.getCause.isInstanceOf[DecodingFailure], s"expected a ${classOf[DecodingFailure].getName}")
  }

  @Test
  def testFailingRetriesReturnOriginalExceptionInsteadOfFailsafeException(): Unit = {
    assertThrows(classOf[CirceException], () =>
      withRetries(context = "testFailingRetriesReturnOriginalExceptionInsteadOfFailsafeException") {
        throw new CirceException("expected", cause = null)
      })
  }
}
