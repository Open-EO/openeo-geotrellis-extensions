package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.Test
import org.openeo.geotrellissentinelhub.AuthApi.AuthResponse

import java.time.Duration

class AuthApiTest {
  private val authApi = new AuthApi

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def auth(): Unit = {
    val AuthResponse(_, duration) = authApi.authenticate(clientId, clientSecret)

    assertTrue(duration.compareTo(Duration.ofMinutes(30)) > 0)
    assertTrue(duration.compareTo(Duration.ofMinutes(60)) <= 0)
  }

  @Test
  def authWithWrongClientSecret(): Unit = {
    val wrongClientSecret = "s3cr3t"

    try {
      authApi.authenticate(clientId, wrongClientSecret)
      fail(s"should have thrown a ${classOf[SentinelHubException]}")
    } catch {
      case e: SentinelHubException =>
        assertTrue(Seq(400, 401) contains e.statusCode)
        assertTrue(e.responseBody, e.responseBody contains """"Invalid client or Invalid client credentials"""")
        assertFalse("contains client secret", e.getMessage contains wrongClientSecret)
    }
  }
}
