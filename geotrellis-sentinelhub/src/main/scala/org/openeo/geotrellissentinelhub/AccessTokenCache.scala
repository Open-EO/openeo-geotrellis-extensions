package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.apache.commons.math3.random.RandomDataGenerator
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime
import java.time.Duration.between
import scala.concurrent.duration._

object AccessTokenCache {
  private val logger = LoggerFactory.getLogger(getClass)
  private val rnd = new RandomDataGenerator

  private val accessTokenCache: LoadingCache[(String, String), AuthResponse] = {
    def expiresIn(authResponse: AuthResponse): FiniteDuration = {
      val expiresInMillis = authResponse.expires_in.toMillis
      val (lower, upper) = (expiresInMillis / 4, expiresInMillis * 3/4)
      val expiresInMillisWithJitter = rnd.nextLong(lower, upper)
      val duration = expiresInMillisWithJitter.millis
      logger.debug(s"access token that expires within ${authResponse.expires_in} is scheduled for eviction from the " +
        s"cache within $duration")
      duration
    }

    Scaffeine()
      .expireAfter[(String, String), AuthResponse](
        create = { case (_, authResponse) => expiresIn(authResponse) },
        update = { case (_, authResponse, _) => expiresIn(authResponse) },
        read = { case (_, _, currentDuration) => currentDuration }
      )
      .build { case (clientId, clientSecret) =>
        new RlGuardAdapter().accessToken
          .flatMap { accessToken => // TODO: replace with filter and map?
            val now = ZonedDateTime.now()
            val valid = accessToken.isValid(now)

            logger.debug(s"Zookeeper access token for client ID $clientId expires at ${accessToken.expires_at}: " +
              s"${if (valid) "valid" else "invalid"}")

            if (valid) // TODO: add a margin?
              Some(AuthResponse(accessToken.token, expires_in = between(now, accessToken.expires_at)))
            else
              None
          }
          .getOrElse {
            val accessToken = new AuthApi().authenticate(clientId, clientSecret)
            logger.debug(s"Auth API access token for clientID $clientId expires within ${accessToken.expires_in}")
            accessToken
          }
      }
  }

  def get(clientId: String, clientSecret: String): String = accessTokenCache.get((clientId, clientSecret)).access_token
  def invalidate(clientId: String, clientSecret: String): Unit = accessTokenCache.invalidate((clientId, clientSecret))
}
