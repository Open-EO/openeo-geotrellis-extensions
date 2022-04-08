package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.apache.commons.math3.random.RandomDataGenerator
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse
import org.slf4j.LoggerFactory

import java.time.Duration.between
import java.time.ZonedDateTime
import scala.concurrent.duration._

trait Authorizer extends Serializable {
  def authorized[R](fn: String => R): R
}

object MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer {
  private val logger =
    LoggerFactory.getLogger(classOf[MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer])
}

class MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId: String, clientSecret: String)
  extends Authorizer {

  import MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer._

  override def authorized[R](fn: String => R): R = {
    def accessToken: String = AccessTokenCache.get(clientId, clientSecret) { case (clientId, clientSecret) =>
      val now = ZonedDateTime.now()

      new RlGuardAdapter().accessToken
        .filter { cachedAccessToken =>
          val valid = cachedAccessToken.isValid(now)
          logger.debug(s"Zookeeper access token for client ID $clientId expires at ${cachedAccessToken.expires_at}: " +
            s"${if (valid) "valid" else "invalid"}")
          valid
        }
        .map(cachedAccessToken =>
          AuthResponse(cachedAccessToken.token, expires_in = between(now, cachedAccessToken.expires_at)))
        .getOrElse {
          val freshAccessToken = new AuthApi().authenticate(clientId, clientSecret)
          logger.debug(s"Auth API access token for clientID $clientId expires within ${freshAccessToken.expires_in}")
          freshAccessToken
        }
    }

    try fn(accessToken)
    catch {
      case SentinelHubException(_, 401, _) =>
        AccessTokenCache.invalidate(clientId, clientSecret)
        fn(accessToken)
    }
  }
}

object MemoizedAuthApiAccessTokenAuthorizer {
  private val logger = LoggerFactory.getLogger(classOf[MemoizedAuthApiAccessTokenAuthorizer])
}

class MemoizedAuthApiAccessTokenAuthorizer(clientId: String, clientSecret: String) extends Authorizer {
  import MemoizedAuthApiAccessTokenAuthorizer._

  override def authorized[R](fn: String => R): R = {
    def accessToken: String = AccessTokenCache.get(clientId, clientSecret) { case (clientId, clientSecret) =>
      val freshAccessToken = new AuthApi().authenticate(clientId, clientSecret)
      logger.debug(s"Auth API access token for clientID $clientId expires within ${freshAccessToken.expires_in}")
      freshAccessToken
    }

    try fn(accessToken)
    catch {
      case SentinelHubException(_, 401, _) =>
        AccessTokenCache.invalidate(clientId, clientSecret)
        fn(accessToken)
    }
  }
}

object AccessTokenCache {
  private val logger = LoggerFactory.getLogger(getClass)
  private val rnd = new RandomDataGenerator

  private def expiresIn(authResponse: AuthResponse): FiniteDuration = {
    val expiresInMillis = authResponse.expires_in.toMillis
    val (lower, upper) = (expiresInMillis / 4, expiresInMillis * 3/4)
    val expiresInMillisWithJitter = rnd.nextLong(lower, upper)
    val duration = expiresInMillisWithJitter.millis
    logger.debug(s"access token that expires within ${authResponse.expires_in} is scheduled for eviction from the " +
      s"cache within $duration")
    duration
  }

  private val cache: Cache[(String, String), AuthResponse] = {
    Scaffeine()
      .expireAfter[(String, String), AuthResponse](
        create = { case (_, authResponse) => expiresIn(authResponse) },
        update = { case (_, authResponse, _) => expiresIn(authResponse) },
        read = { case (_, _, currentDuration) => currentDuration }
      )
      .build()
  }

  private[geotrellissentinelhub] def get(clientId: String, clientSecret: String)(getAuthToken: ((String, String)) => AuthResponse): String =
    cache.get((clientId, clientSecret), getAuthToken).access_token

  private[geotrellissentinelhub] def invalidate(clientId: String, clientSecret: String): Unit = cache.invalidate((clientId, clientSecret))
}
