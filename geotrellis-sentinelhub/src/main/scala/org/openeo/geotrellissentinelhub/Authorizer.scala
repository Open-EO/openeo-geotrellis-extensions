package org.openeo.geotrellissentinelhub

import com.github.blemale.scaffeine.{Cache, Scaffeine}
import io.circe.generic.auto._
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.openeo.geotrelliscommon.CirceException.decode
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse
import org.openeo.geotrellissentinelhub.RlGuardAdapter.AccessToken
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration.between
import java.time.ZonedDateTime
import scala.concurrent.duration._

trait Authorizer extends Serializable {
  def authorized[R](fn: String => R): R
}

object MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer {
  private val logger = LoggerFactory.getLogger(classOf[MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer])
}

class MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(zookeeperConnectionString: String,
                                                                    accessTokenPath: String,
                                                                    clientId: String, clientSecret: String) extends Authorizer {
  import MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer._

  def this(clientId: String, clientSecret: String) = this(
    zookeeperConnectionString = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
    accessTokenPath = "/openeo/rlguard/access_token_default",
    clientId, clientSecret)

  private def zookeeperAccessToken: Option[AccessToken] = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 5)

    val client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
    client.start()

    try {
      val data = new String(client.getData.forPath(accessTokenPath), UTF_8)
      decode[AccessToken](data) match {
        case Right(accessToken) => Some(accessToken)
        case Left(e) => throw e
      }
    } catch {
      case e: Exception /* ugh Curator */ =>
        logger.warn(s"Could not retrieve Zookeeper access token from $accessTokenPath", e)
        None
    } finally client.close()
  }

  // TODO: reduce code duplication with MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer
  override def authorized[R](fn: String => R): R = {
    def accessToken: String = AccessTokenCache.get(clientId, clientSecret) { case (clientId, clientSecret) =>
      val now = ZonedDateTime.now()

      zookeeperAccessToken
        .filter { cachedAccessToken =>
          val valid = cachedAccessToken.isValid(now)
          logger.debug(s"Zookeeper access token at $accessTokenPath expires at ${cachedAccessToken.expires_at}: " +
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
      case SentinelHubException(_, 401, _, _) =>
        AccessTokenCache.invalidate(clientId, clientSecret)
        fn(accessToken)
    }
  }
}

object MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer {
  private val logger =
    LoggerFactory.getLogger(classOf[MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer])
}

/**
 Only supports single access token cached in Zookeeper: /openeo/rlguard/access_token
 */
@deprecated("use MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer instead")
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
      case SentinelHubException(_, 401, _, _) =>
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
      case SentinelHubException(_, 401, _, _) =>
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
