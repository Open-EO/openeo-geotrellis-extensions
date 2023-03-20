package org.openeo.geotrellissentinelhub

import org.openeo.geotrelliscommon.CirceException.decode
import cats.syntax.either._
import io.circe.Decoder
import io.circe.generic.auto._
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpConstants, HttpOptions, HttpRequest}

import java.time.Duration

object AuthApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[AuthApi])

  //noinspection ScalaUnusedSymbol
  private implicit val decodeDuration: Decoder[Duration] = Decoder.decodeLong.map(Duration.ofSeconds)

  // TODO: snake case to camel case
  private[geotrellissentinelhub] case class AuthResponse(access_token: String, expires_in: Duration)
}

class AuthApi {
  import AuthApi._

  def authenticate(clientId: String, clientSecret: String): AuthResponse =
    withRetries(context = s"authenticate $clientId") {
      val safeParams = Seq(
        "grant_type" -> "client_credentials",
        "client_id" -> clientId
      )

      val getAuthToken = http("https://services.sentinel-hub.com/oauth/token")
        .postForm(safeParams :+ ("client_secret" -> clientSecret))

      logger.debug(s"requesting new access token for client ID $clientId")

      val response = getAuthToken
        .asString

      if (response.isError) throw SentinelHubException(getAuthToken,
        HttpConstants.toQs(safeParams, getAuthToken.charset), response)

      decode[AuthResponse](response.body)
        .valueOr(throw _)
    }

  private def http(url: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 40000)
}
