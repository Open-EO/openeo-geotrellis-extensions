package org.openeo.geotrellissentinelhub

import scalaj.http.{HttpRequest, HttpResponse}

object SentinelHubException {
  def apply(request: HttpRequest, requestBody: String, textResponse: HttpResponse[String]): SentinelHubException =
    this(request, requestBody, textResponse.code, textResponse.headers, textResponse.body)

  def apply(request: HttpRequest, requestBody: String, statusCode: Int, responseHeaders: collection.Map[String, Seq[String]],
            responseBody: String): SentinelHubException = {
    val queryString = request.urlBuilder(request)
    val statusLine = responseHeaders.get("Status").flatMap(_.headOption).getOrElse("UNKNOWN")

    val message: String = {
      s"""Sentinel Hub returned an error
         |response: $statusLine with body: $responseBody
         |request: ${request.method} $queryString with body: $requestBody""".stripMargin
    }

    SentinelHubException(message, statusCode, responseHeaders, responseBody)
  }
}

case class SentinelHubException(private val message: String, statusCode: Int,
                                responseHeaders: collection.Map[String, Seq[String]], responseBody: String)
  extends RuntimeException(message)
