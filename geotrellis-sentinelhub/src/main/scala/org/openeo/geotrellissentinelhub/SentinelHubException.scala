package org.openeo.geotrellissentinelhub

import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, response: HttpResponse[_]): SentinelHubException =
    this(request, requestBody, response.code, response.statusLine, response.body)

  def apply(request: HttpRequest, requestBody: String, statusCode: Int, statusLine: String,
            responseBody: Any): SentinelHubException = {
    val queryString = request.urlBuilder(request)

    val message: String = {
      s"""Sentinel Hub returned an error
         |response: $statusLine with body: $responseBody
         |request: ${request.method} $queryString with body: $requestBody""".stripMargin
    }

    new SentinelHubException(message, statusCode)
  }
}

class SentinelHubException(message: String, val statusCode: Int) extends Exception(message)
