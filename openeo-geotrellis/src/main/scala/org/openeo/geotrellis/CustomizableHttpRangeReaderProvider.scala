package org.openeo.geotrellis

import cats.syntax.either._
import geotrellis.util.{RangeReader, RangeReaderProvider}
import io.circe.generic.auto._
import org.openeo.geotrelliscommon.CirceException.decode
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.Http

import java.io.{File, FileNotFoundException}
import java.net.URI
import scala.io.Source

class CustomizableHttpRangeReaderProvider extends RangeReaderProvider {
  import CustomizableHttpRangeReaderProvider._

  override def canProcess(uri: URI): Boolean = {
    val scheme = uri.getScheme

    if (scheme == null) false
    else Seq("http", "https").contains(scheme.toLowerCase)
  }

  override def rangeReader(uri: URI): RangeReader = {
    val credentials = credentialsFromFile.get(uri.getHost)

    val request = credentials.foldLeft(
      Http(uri.toString)
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 40000)
    ) { case (http, Credentials(username, password)) =>
      http.auth(username, password)
    }

    new CustomizableHttpRangeReader(request, useHeadRequest = true)
  }
}

object CustomizableHttpRangeReaderProvider {
  private type Host = String

  private val logger: Logger = LoggerFactory.getLogger(classOf[CustomizableHttpRangeReaderProvider])

  private final val HttpCredentialsFileSystemProperty = "http.credentials.file"

  private lazy val credentialsFromFile: Map[Host, Credentials] = {
    val credentialsFile = {
      val path = Option(System.getProperty(HttpCredentialsFileSystemProperty)).getOrElse("./http_credentials.json")
      new File(path)
    }

    try {
      val source = Source.fromFile(credentialsFile)

      try {
        val credentials = decode[Map[Host, Credentials]](source.mkString).valueOr(throw _)

        logger.info(s"${credentialsFile.getCanonicalPath} contains HTTP credentials for " +
          s"${credentials.map { case (host, Credentials(username, _)) => s"$username@$host" } mkString ", "}")

        credentials
      } finally source.close()
    } catch {
      case e: FileNotFoundException =>
        logger.warn(s"JSON file with HTTP credentials not found at ${credentialsFile.getCanonicalPath}; setting " +
          s"system property $HttpCredentialsFileSystemProperty allows for a custom location", e)
        Map()
    }
  }

  private case class Credentials(username: String, password: String)
}
