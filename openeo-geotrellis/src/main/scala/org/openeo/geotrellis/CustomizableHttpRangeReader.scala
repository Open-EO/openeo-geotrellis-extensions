package org.openeo.geotrellis

import geotrellis.util.RangeReader
import scalaj.http.HttpRequest

import scala.util.Try

// copied from geotrellis.util.HttpRangeReader as that one is not extensible
class CustomizableHttpRangeReader(request: HttpRequest, useHeadRequest: Boolean) extends RangeReader {
  override lazy val totalLength: Long = {
    val headers = if (useHeadRequest) {
      withRetries { request.method("HEAD").asString }
    } else {
      withRetries { request.method("GET").execute { _ => "" } }
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
    val res = withRetries {
      request
        .method("GET")
        .header("Range", s"bytes=${start}-${start + length}")
        .asBytes
    }
    /**
     * "If the byte-range-set is unsatisfiable, the server SHOULD return
     * a response with a status of 416 (Requested range not satisfiable).
     * Otherwise, the server SHOULD return a response with a status of 206
     * (Partial Content) containing the satisfiable ranges of the entity-body."
     * https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
     */
    require(res.code != 416,
      s"Server unable to generate the byte range between ${start} and ${start + length} for ${request.url}")

    require(res.is2xx,
      s"While reading ${request.url}, server returned status code ${res.code} with type ${res.contentType} and body ${new String(res.body)}")

    res.body
  }
}
