package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import io.circe.Decoder
import io.circe.generic.auto._
import io.circe.parser.decode
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.Duration

object AuthApi {
  // TODO: snake case to camel case
  private[geotrellissentinelhub] case class AuthResponse(access_token: String, expires_in: Duration)
}

class AuthApi {
  import AuthApi._

  def authenticate(clientId: String, clientSecret: String): AuthResponse = {
    // FIXME: cache token for duration/2
    val getAuthToken = http("https://services.sentinel-hub.com/oauth/token")
      .postForm(Seq(
        "grant_type" -> "client_credentials",
        "client_id" -> clientId,
        "client_secret" -> clientSecret
      ))
      .asString
      .throwError

    implicit val decodeDuration: Decoder[Duration] = Decoder.decodeLong.map(Duration.ofSeconds)

    decode[AuthResponse](getAuthToken.body)
      .valueOr(throw _)
  }

  private def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))
}
