package org.openeo.geotrellis

import cats.syntax.either._
import geotrellis.util.{RangeReader, RangeReaderProvider}
import io.circe.generic.auto._
import org.openeo.geotrelliscommon.CirceException.decode
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.Http

import java.io.FileNotFoundException
import java.net.URI
import scala.io.Source

class CustomizableHttpRangeReaderProvider extends RangeReaderProvider {
  import CustomizableHttpRangeReaderProvider._

  private type Host = String

  override def canProcess(uri: URI): Boolean = {
    val scheme = uri.getScheme

    if (scheme == null) false
    else Seq("http", "https").contains(scheme.toLowerCase) && credentialsFromFile.keySet.contains(uri.getHost)
  }

  override def rangeReader(uri: URI): RangeReader = {
    val Credentials(username, password) = credentialsFromFile(uri.getHost)
    val request = Http(uri.toString).auth(username, password)
    new CustomizableHttpRangeReader(request, useHeadRequest = true)
  }

  @transient private lazy val credentialsFromFile: Map[Host, Credentials] = {
    try {
      val source = Source.fromFile(credentialsFile)

      try decode[Map[Host, Credentials]](source.mkString).valueOr(throw _)
      finally source.close()
    } catch {
      case e: FileNotFoundException => logger.warn("JSON file with HTTP credentials not found; setting system " +
        s"property $HttpCredentialsFileSystemProperty allows for a custom location", e)
        Map()
    }
  }
}

object CustomizableHttpRangeReaderProvider {
  private val logger: Logger = LoggerFactory.getLogger(classOf[CustomizableHttpRangeReaderProvider])

  private final val HttpCredentialsFileSystemProperty = "http.credentials.file"
  private val credentialsFile =
    Option(System.getProperty(HttpCredentialsFileSystemProperty)).getOrElse("./http_credentials.json")

  private case class Credentials(username: String, password: String)
}
