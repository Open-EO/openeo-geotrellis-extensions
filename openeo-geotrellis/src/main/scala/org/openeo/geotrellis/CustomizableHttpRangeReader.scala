package org.openeo.geotrellis

import geotrellis.util.RangeReader
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.HttpRequest

import scala.util.Try

object CustomizableHttpRangeReader {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[CustomizableHttpRangeReader])
}

// copied from geotrellis.util.HttpRangeReader as that one is not extensible
class CustomizableHttpRangeReader(request: HttpRequest, useHeadRequest: Boolean) extends RangeReader {
  import CustomizableHttpRangeReader._

  override lazy val totalLength: Long = {
    val headers = withRetryAfterRetries("totalLength") {
      if (useHeadRequest) {
        request.method("HEAD").asString
      } else {
        request.method("GET").execute { _ => "" }
      }
    }
    val contentLength = headers
      .header("Content-Length")
      .flatMap(cl => Try(cl.toLong).toOption) match {
      case Some(num) => num
      case None => -1L
    }
    headers.throwError

    /**
     * "The Accept-Ranges response HTTP header is a marker used by the server
     * to advertise its support of partial requests. The value of this field
     * indicates the unit that can be used to define a range."
     * https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept-Ranges
     */
    require(headers.header("Accept-Ranges").contains("bytes"),
      "Server doesn't support ranged byte reads")

    require(contentLength > 0,
      "Server didn't provide (required) \"Content-Length\" headers, unable to do range-based read")

    contentLength
  }

  def readClippedRange(start: Long, length: Int): Array[Byte] = {
    val res = withRetryAfterRetries("readClippedRange") {
      request
        .method("GET")
        .header("Range", s"bytes=${start}-${start + length}")
        .asBytes
    }
    res.throwError.body
  }
}
