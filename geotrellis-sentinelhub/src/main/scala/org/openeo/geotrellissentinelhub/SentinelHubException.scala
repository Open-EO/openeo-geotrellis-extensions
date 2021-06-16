package org.openeo.geotrellissentinelhub

import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, response: HttpResponse[String]): SentinelHubException = {
    val queryString = request.urlBuilder(request)

    val message: String =
      s"""
         |response: ${response.statusLine} with body: ${response.body.trim}
         |request: ${request.method} $queryString with body: ${requestBody.trim}""".stripMargin

    new SentinelHubException(message)
  }
}

class SentinelHubException(message: String) extends Exception(message)
