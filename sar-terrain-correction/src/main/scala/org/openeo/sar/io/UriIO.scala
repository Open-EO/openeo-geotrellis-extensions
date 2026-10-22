package org.openeo.sar.io

import geotrellis.store.s3.AmazonS3URI
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.withRetryAfterRetries
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.Http

import java.io.{BufferedInputStream, ByteArrayInputStream, FileNotFoundException, InputStream}
import java.net.URI
import scala.xml.{Elem, XML}

/** Uniform reader for SAR auxiliary files. Dispatches on the URI scheme:
 *
 *  - `s3://bucket/key`            -> [[CreoS3Utils.readFromS3]] (proxy-aware, CDSE-aware)
 *  - `http://` / `https://`       -> `scalaj-http`, retried via [[withRetryAfterRetries]]
 *  - `file://` / no scheme        -> local file
 *
 *  The S3 path bypasses the JDK's lack of an `s3` URLStreamHandler and reuses
 *  the credentials/endpoint logic that the rest of the openeo-geotrellis
 *  stack already uses for CDSE / CloudFerro / WAW. */
object UriIO {

  private implicit val logger: Logger = LoggerFactory.getLogger(UriIO.getClass)

  /** Open an `InputStream` for the given URI. Caller is responsible for closing. */
  def openInputStream(uri: URI): InputStream = {
    logger.debug(s"sar_backscatter - Opening URI $uri")
    uri.getScheme match {

      case "s3" =>
        val in = CreoS3Utils.readFromS3(new AmazonS3URI(uri))
        if (in == null) throw new FileNotFoundException(s"S3 object not found: $uri")
        new BufferedInputStream(in)

      case "http" | "https" =>
        // withRetryAfterRetries retries 5xx, socket errors and rate limiting
        // responses (429/498, honoring Retry-After) with backoff.
        val response = withRetryAfterRetries(s"sar_backscatter - fetching $uri") {
          Http(uri.toString).asBytes
        }
        new BufferedInputStream(new ByteArrayInputStream(response.throwError.body))

      case "file" =>
        new BufferedInputStream(uri.toURL.openStream())

      case null =>
        new BufferedInputStream(new java.io.FileInputStream(uri.getPath))

      case other =>
        throw new IllegalArgumentException(s"Unsupported URI scheme: $other ($uri)")
    }
  }

  /** Read the URI fully into memory and parse it as XML. */
  def loadXml(uri: URI): Elem = {
    val in = openInputStream(uri)
    try XML.load(in) finally in.close()
  }
}
