package org.openeo.geotrellissentinelhub

import CirceException.decode
import cats.syntax.either._
import io.circe.Decoder
import io.circe.generic.auto._
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.Duration

object AuthApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[AuthApi])

  // TODO: snake case to camel case
  private[geotrellissentinelhub] case class AuthResponse(access_token: String, expires_in: Duration)
}

class AuthApi {
  import AuthApi._

  def authenticate(clientId: String, clientSecret: String): AuthResponse =
    withRetries(context = s"authenticate $clientId") {
      val params = Seq(
        "grant_type" -> "client_credentials",
        "client_id" -> clientId,
        "client_secret" -> clientSecret
      )

      val getAuthToken = http("https://services.sentinel-hub.com/oauth/token")
        .postForm(params)

      val response = getAuthToken
        .asString

      if (response.isError) throw SentinelHubException(getAuthToken, params.toString(), response)

      implicit val decodeDuration: Decoder[Duration] = Decoder.decodeLong.map(Duration.ofSeconds)

      decode[AuthResponse](response.body)
        .valueOr(throw _)
  }

  private def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))
}
