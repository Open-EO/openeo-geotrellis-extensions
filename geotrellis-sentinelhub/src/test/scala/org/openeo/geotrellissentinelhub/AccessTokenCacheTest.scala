package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import java.time.Duration
import java.util.concurrent.TimeUnit

class AccessTokenCacheTest {

  @Test
  def testAccessTokenAddedManuallyObeysExpiresIn(): Unit = {
    val clientId = "69f3a081-4b54-43bb-a1fa-e89dd1c05b03"
    val clientSecret = "???"

    val initialAccessToken = AuthApi.AuthResponse("4cc3ss_tok3n", expires_in = Duration.ofSeconds(3))

    AccessTokenCache.get(clientId, clientSecret) { _ => initialAccessToken }

    // should still be valid
    assertEquals(initialAccessToken.access_token, AccessTokenCache.get(clientId, clientSecret) { _ =>
      throw new Exception("should have returned the valid access token and not fetch a new one")
    })

    TimeUnit.SECONDS.sleep(5) // let it evict

    val newAccessToken = AuthApi.AuthResponse("n3w_4cc3ss_tok3n", expires_in = Duration.ofSeconds(3))

    assertEquals(newAccessToken.access_token, AccessTokenCache.get(clientId, clientSecret) { _ =>
      newAccessToken
    })
  }
}
