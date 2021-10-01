package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse

import scala.concurrent.duration._

object AccessTokenCache {
  // TODO: invalidate key on 401 Unauthorized response from CatalogApi, ProcessApi etc.
  private val accessTokenCache: LoadingCache[(String, String), AuthResponse] =
    Scaffeine()
      .expireAfter[(String, String), AuthResponse](
        create = { case (_, authResponse) => expiresIn(authResponse) },
        update = { case (_, authResponse, _) => expiresIn(authResponse) },
        read = { case (_, _, currentDuration) => currentDuration }
      )
      .build { case (clientId, clientSecret) => new AuthApi().authenticate(clientId, clientSecret) }

  private def expiresIn(authResponse: AuthResponse): FiniteDuration = authResponse.expires_in.toNanos.nanoseconds / 2

  def get(clientId: String, clientSecret: String): String = accessTokenCache.get((clientId, clientSecret)).access_token

  def invalidate(clientId: String, clientSecret: String): Unit = accessTokenCache.invalidate((clientId, clientSecret))
}
