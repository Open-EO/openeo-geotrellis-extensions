package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

object AccessTokenCache {
  private val accessTokenCache: LoadingCache[(String, String), AuthResponse] = {
    def expiresIn(authResponse: AuthResponse): FiniteDuration = authResponse.expires_in.toNanos.nanoseconds - FiniteDuration(10,TimeUnit.SECONDS)

    Scaffeine()
      .expireAfter[(String, String), AuthResponse](
        create = { case (_, authResponse) => expiresIn(authResponse) },
        update = { case (_, authResponse, _) => expiresIn(authResponse) },
        read = { case (_, _, currentDuration) => currentDuration }
      )
      .build { case (clientId, clientSecret) => new AuthApi().authenticate(clientId, clientSecret) }
  }

  def get(clientId: String, clientSecret: String): String = accessTokenCache.get((clientId, clientSecret)).access_token

  def invalidate(clientId: String, clientSecret: String): Unit = accessTokenCache.invalidate((clientId, clientSecret))
}
