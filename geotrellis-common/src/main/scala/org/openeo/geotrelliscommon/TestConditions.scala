package org.openeo.geotrelliscommon

import com.azavea.gdal.GDALWarp

import java.nio.file.Path
import scala.sys.env

object TestConditions {

  // Blank values count as missing, e.g. unset secrets in GitHub Actions are passed as empty strings.
  private def hasEnv(name: String): Boolean = sys.env.get(name).exists(_.trim.nonEmpty)

  def hasSentinelHubCredentials: Boolean = {
    hasEnv("SENTINELHUB_CLIENT_ID") && hasEnv("SENTINELHUB_CLIENT_SECRET")
  }

  def hasAwsCredentials: Boolean = {
    hasEnv("AWS_ACCESS_KEY_ID") && hasEnv("AWS_SECRET_ACCESS_KEY")
  }

  def hasS3Credentials: Boolean = {
    // Same lookup as CreoS3Utils: SWIFT_* takes precedence over AWS_*.
    def credential(swift: String, aws: String) = sys.env.getOrElse(swift, sys.env.getOrElse(aws, ""))
    credential("SWIFT_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID").trim.nonEmpty &&
      credential("SWIFT_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY").trim.nonEmpty
  }

  def hasMTDAData: Boolean = {
    val folder = Path.of("/data/MTDA").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasProjectsData: Boolean = {
    val folder = Path.of("/data/projects").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasMtdaDevData: Boolean = {
    val folder = Path.of("/data/MTDA_DEV").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasMepData: Boolean = {
    val folder = Path.of("/data/MEP").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasEodataData(): Boolean = {
    val folder = Path.of("/eodata").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasHttpCredentials: Boolean = {
    val credentialsFile = Path.of(Option(System.getProperty("http.credentials.file")).getOrElse("./http_credentials.json")).toFile
    credentialsFile.isFile && credentialsFile.exists
  }

  def hasGdalInstalled: Boolean = {
    try {
      GDALWarp.get_version_info("VERSION_NUM")
      true
    } catch {
      case _: Throwable => false
    }
  }

  def hasDockerInstalled: Boolean = {
    try {
      val cmd = Seq("docker", "version")
      val process = new ProcessBuilder(cmd: _*).start()
      val exitCode = process.waitFor()
      exitCode == 0
    } catch {
      case _: Throwable => false
    }
  }

  def runProcessGraphRegressionTests: Boolean = {
    hasDockerInstalled && System.getProperty("RUN_PROCESS_GRAPH_REGRESSION_TESTS") == "true"
  }
}
