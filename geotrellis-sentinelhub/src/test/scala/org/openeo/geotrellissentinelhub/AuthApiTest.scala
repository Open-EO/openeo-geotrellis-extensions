package org.openeo.geotrellissentinelhub

import org.junit.Assert.assertTrue
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
}
