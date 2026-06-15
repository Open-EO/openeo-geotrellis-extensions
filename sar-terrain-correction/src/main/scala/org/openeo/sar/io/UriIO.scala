package org.openeo.sar.io

import geotrellis.store.s3.AmazonS3URI
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.s3Client
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import scala.xml.{Elem, XML}

/** Uniform reader for SAR auxiliary files. Dispatches on the URI scheme:
 *
 *  - `s3://bucket/key`            -> [[CreoS3Utils.getS3Client]] (proxy-aware, CDSE-aware)
 *  - `http://` / `https://`       -> standard URL connection
 *  - `file://` / no scheme        -> local file
 *
 *  The S3 path bypasses the JDK's lack of an `s3` URLStreamHandler and reuses
 *  the credentials/endpoint logic that the rest of the openeo-geotrellis
 *  stack already uses for CDSE / CloudFerro / WAW. */
object UriIO {

  /** Open an `InputStream` for the given URI. Caller is responsible for closing. */
  def openInputStream(uri: URI): InputStream = uri.getScheme match {
    case "s3" =>
      val s3Uri  = new AmazonS3URI(uri)
      val client = s3Client(Region.of("RegionOne"), URI.create("https://eodata.dataspace.copernicus.eu"))
      val key    = stripLeading('/', s3Uri.getKey)
      val req    = GetObjectRequest.builder().bucket(s3Uri.getBucket).key(key).build()
      val resp: ResponseInputStream[GetObjectResponse] = client.getObject(req)
      new BufferedInputStream(resp)

    case "http" | "https" | "file" =>
      new BufferedInputStream(uri.toURL.openStream())

    case null =>
      new BufferedInputStream(new java.io.FileInputStream(uri.getPath))

    case other =>
      throw new IllegalArgumentException(s"Unsupported URI scheme: $other ($uri)")
  }

  /** Read the URI fully into memory and parse it as XML. */
  def loadXml(uri: URI): Elem = {
    val in = openInputStream(uri)
    try XML.load(in) finally in.close()
  }

  private def stripLeading(c: Char, s: String): String =
    if (s != null && s.nonEmpty && s.charAt(0) == c) s.substring(1) else s
}
