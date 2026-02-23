package org.openeo.geotrellis

import geotrellis.store.s3.util.{S3RangeReader, S3RangeReaderProvider}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.s3.S3Client

import java.net.URI

class MultiClientRangeReaderProviderCapturer extends MultiClientRangeReaderProvider {
  var capturedUri: Option[URI] = None
  override lazy val s3Endpoint: String = "https://s3.waw3-1.cloudferro.com"

  override def rangeReader(uri: URI, client: S3Client): S3RangeReader = {
    assert(capturedUri.isEmpty)
    capturedUri = Some(uri)
    // No need to do a real request for this test
    null
  }
}

class MultiClientRangeReaderProviderTest {

  @Test
  def eodataUppercaseBucketIsNormalized(): Unit = {
    val provider = new MultiClientRangeReaderProviderCapturer()
    provider.rangeReader(new URI("s3://EODATA/path/to/file.tif"))
    assertEquals("s3://eodata/path/to/file.tif", provider.capturedUri.get.toString)
  }

  @Test
  def eodataLowercaseBucketIsUnchanged(): Unit = {
    val provider = new MultiClientRangeReaderProviderCapturer()
    provider.rangeReader(new URI("s3://eodata/path/to/file.tif"))
    assertEquals("s3://eodata/path/to/file.tif", provider.capturedUri.get.toString)
  }
}
