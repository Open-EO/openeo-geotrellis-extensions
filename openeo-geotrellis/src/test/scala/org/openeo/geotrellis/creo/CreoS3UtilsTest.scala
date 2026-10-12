package org.openeo.geotrellis.creo

import geotrellis.store.s3.AmazonS3URI
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junitpioneer.jupiter.SetEnvironmentVariable

import java.nio.file.{Files, Path}

object CreoS3UtilsTest {
  private val dir = Path.of("/tmp/CreoS3UtilsTest")
  private val tokenFile = dir.resolve("token")
  private val bucketConfigFile = dir.resolve("bucket_config.json")

  @BeforeAll
  def writeProxyConfig(): Unit = {
    Files.createDirectories(dir)
    Files.writeString(tokenFile, "dummy-token")
    Files.writeString(
      bucketConfigFile,
      """{"proxy-bucket": {"region": "eu-special-1", "role_arn": "arn:aws:iam::000000000000:role/test"}}"""
    )
  }
}

@SetEnvironmentVariable(key = "OPENEO_WEB_IDENTITY_TOKEN_FILE", value = "/tmp/CreoS3UtilsTest/token")
@SetEnvironmentVariable(key = "OPENEO_BUCKET_CONFIG_FILE", value = "/tmp/CreoS3UtilsTest/bucket_config.json")
@SetEnvironmentVariable(key = "S3PROXY_STS_ENDPOINT_URL", value = "http://sts.proxy.test:9001")
@SetEnvironmentVariable(key = "S3PROXY_S3_ENDPOINT_URL", value = "http://s3.proxy.test:9000")
@SetEnvironmentVariable(key = "SWIFT_URL", value = "http://swift.test:8080")
@SetEnvironmentVariable(key = "SWIFT_ACCESS_KEY_ID", value = "access")
@SetEnvironmentVariable(key = "SWIFT_SECRET_ACCESS_KEY", value = "secret")
class CreoS3UtilsTest {
  private val proxyEndpoint = "http://s3.proxy.test:9000"

  @Test
  def getAsyncClientUsesProxyForConfiguredBucket(): Unit = {
    val client = CreoS3Utils.getAsyncClient(new AmazonS3URI("s3://proxy-bucket/some/key.tif"))
    assertEquals(proxyEndpoint, client.serviceClientConfiguration().endpointOverride().get().toString)
  }

  @Test
  def getS3ClientAndAsyncClientAgreeForProxyBucket(): Unit = {
    val uri = new AmazonS3URI("s3://proxy-bucket/some/key.tif")
    assertEquals(
      CreoS3Utils.getS3Client(uri).serviceClientConfiguration().endpointOverride().get(),
      CreoS3Utils.getAsyncClient(uri).serviceClientConfiguration().endpointOverride().get()
    )
  }

  @Test
  @SetEnvironmentVariable(key = "CREOS3_TEST_MIB_OK", value = "7")
  @SetEnvironmentVariable(key = "CREOS3_TEST_MIB_NAN", value = "abc")
  @SetEnvironmentVariable(key = "CREOS3_TEST_MIB_LOW", value = "2")
  def envMiBParsing(): Unit = {
    assertEquals(100L * 1024 * 1024, CreoS3Utils.envMiB("CREOS3_TEST_MIB_MISSING", 100, 1))
    assertEquals(7L * 1024 * 1024, CreoS3Utils.envMiB("CREOS3_TEST_MIB_OK", 100, 5))
    assertEquals(100L * 1024 * 1024, CreoS3Utils.envMiB("CREOS3_TEST_MIB_NAN", 100, 5))
    assertEquals(100L * 1024 * 1024, CreoS3Utils.envMiB("CREOS3_TEST_MIB_LOW", 100, 5))
  }

  @Test
  @SetEnvironmentVariable(key = "CREOS3_TEST_INT_OK", value = "3")
  @SetEnvironmentVariable(key = "CREOS3_TEST_INT_ZERO", value = "0")
  def envIntParsing(): Unit = {
    assertEquals(10, CreoS3Utils.envInt("CREOS3_TEST_INT_MISSING", 10, 1))
    assertEquals(3, CreoS3Utils.envInt("CREOS3_TEST_INT_OK", 10, 1))
    assertEquals(10, CreoS3Utils.envInt("CREOS3_TEST_INT_ZERO", 10, 1))
  }

  @Test
  def getAsyncClientDoesNotUseProxyForOtherBucket(): Unit = {
    val client = CreoS3Utils.getAsyncClient(new AmazonS3URI("s3://other-bucket/some/key.tif"))
    assertNotEquals(proxyEndpoint, client.serviceClientConfiguration().endpointOverride().get().toString)
  }
}
