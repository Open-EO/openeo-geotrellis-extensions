package org.openeo.geotrellissentinelhub

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test
import scalaj.http.{Http, HttpResponse, HttpStatusException}

class MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizerTest {
  private val sentinelHubClientId = Utils.clientId
  private val sentinelHubClientSecret = Utils.clientSecret

  @Test
  def authorized(): Unit = {
    val authorizer = new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(
      sentinelHubClientId, sentinelHubClientSecret)

    def someAuthProtectedApiCall(accessToken: Option[String]): HttpResponse[String] = {
      val request =  accessToken.foldLeft(Http("https://services.sentinel-hub.com/api/v1/batch/tilinggrids/")) {
        case (httpRequest, token) => httpRequest.header("Authorization", s"Bearer $token")
      }

      request
        .asString
        .throwError
    }

    // sanity check
    try {
      someAuthProtectedApiCall(None)
      fail("should have returned a 401 Unauthorized but succeeded instead")
    } catch {
      case e: HttpStatusException if e.code == 401 => /* expected */
    }

    authorizer.authorized { accessToken =>
      val response = someAuthProtectedApiCall(Some(accessToken))
      assertTrue(response.body contains "20km")
    }
  }
}
