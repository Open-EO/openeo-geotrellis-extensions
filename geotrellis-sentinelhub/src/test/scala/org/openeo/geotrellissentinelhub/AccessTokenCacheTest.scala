package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

import java.time.Duration
import java.util.concurrent.TimeUnit

class AccessTokenCacheTest {

  @Test
  def testAccessTokenAddedManuallyObeysExpiresIn(): Unit = {
    val clientId = "69f3a081-4b54-43bb-a1fa-e89dd1c05b03"
    val clientSecret = "???"

    val initialAccessToken = AuthApi.AuthResponse("4cc3ss_tok3n", expires_in = Duration.ofSeconds(3))

    AccessTokenCache.put(clientId, clientSecret, initialAccessToken)

    // should still be valid
    assertEquals(initialAccessToken.access_token, AccessTokenCache.get(clientId, clientSecret))

    TimeUnit.SECONDS.sleep(5) // let it evict

    try {
      AccessTokenCache.get(clientId, clientSecret)
      fail("access token assumed expired is still available")
    } catch {
      case SentinelHubException(message, 400, _) if message contains "Illegal client_id" =>
        // error is expected; the point is that it reaches out to SHub for a new access token because the old one was
        // evicted from the cache based on its expires_in.
    }
  }
}
