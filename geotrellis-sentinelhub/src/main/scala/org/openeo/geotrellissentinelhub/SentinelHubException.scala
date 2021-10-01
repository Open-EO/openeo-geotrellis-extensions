package org.openeo.geotrellissentinelhub

import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, textResponse: HttpResponse[String]): SentinelHubException =
    this(request, requestBody, textResponse.code, textResponse.statusLine, textResponse.body)

  def apply[R](request: HttpRequest, requestBody: String, statusCode: Int, statusLine: String,
            responseBody: String): SentinelHubException = {
    val queryString = request.urlBuilder(request)

    val message: String = {
      s"""Sentinel Hub returned an error
         |response: $statusLine with body: $responseBody
         |request: ${request.method} $queryString with body: $requestBody""".stripMargin
    }

    SentinelHubException(message, statusCode, responseBody)
  }
}

case class SentinelHubException(message: String, statusCode: Int, responseBody: String)
  extends RuntimeException(message)
