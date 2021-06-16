package org.openeo.geotrellissentinelhub

import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, response: HttpResponse[_]): SentinelHubException = {
    val queryString = request.urlBuilder(request)

    val message: String =
      s"""Sentinel Hub returned an error
          |response: ${response.statusLine} with body: ${response.body}
          |request: ${request.method} $queryString with body: $requestBody""".stripMargin

    new SentinelHubException(message)
  }
}

class SentinelHubException(message: String) extends Exception(message)
