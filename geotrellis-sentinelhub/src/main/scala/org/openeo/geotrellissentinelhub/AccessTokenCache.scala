package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.apache.commons.math3.random.RandomDataGenerator
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse

import java.time.ZonedDateTime
import java.time.Duration.between
import scala.concurrent.duration._

object AccessTokenCache {
  private val rlGuardAdapter = new RlGuardAdapter
  private val rnd = new RandomDataGenerator

  private val accessTokenCache: LoadingCache[(String, String), AuthResponse] = {
    def expiresIn(authResponse: AuthResponse): FiniteDuration = {
      val expiresInMillis = authResponse.expires_in.toMillis
      val (lower, upper) = (expiresInMillis / 4, expiresInMillis * 3/4)
      val expiresInMillisWithJitter = rnd.nextLong(lower, upper)
      expiresInMillisWithJitter.millis
    }

    Scaffeine()
      .expireAfter[(String, String), AuthResponse](
        create = { case (_, authResponse) => expiresIn(authResponse) },
        update = { case (_, authResponse, _) => expiresIn(authResponse) },
        read = { case (_, _, currentDuration) => currentDuration }
      )
      .build { case (clientId, clientSecret) =>
        rlGuardAdapter.accessToken
          .flatMap { accessToken =>
            val now = ZonedDateTime.now()

            if (accessToken.isValid(now)) // TODO: add a margin?
              Some(AuthResponse(accessToken.token, expires_in = between(now, accessToken.expires_at)))
            else
              None
          }
          .getOrElse(new AuthApi().authenticate(clientId, clientSecret))
      }
  }

  def get(clientId: String, clientSecret: String): String = accessTokenCache.get((clientId, clientSecret)).access_token
  def invalidate(clientId: String, clientSecret: String): Unit = accessTokenCache.invalidate((clientId, clientSecret))
}
