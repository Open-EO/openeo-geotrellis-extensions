package org.openeo.geotrelliscommon

import com.azavea.gdal.GDALWarp

import java.nio.file.Path

object TestConditions {

  def hasSentinelHubCredentials: Boolean = {
    (System.getenv("SENTINELHUB_CLIENT_ID") != null) && (System.getenv("SENTINELHUB_CLIENT_SECRET") != null)
  }

  def hasAwsCredentials: Boolean = {
    (System.getenv("AWS_ACCESS_KEY_ID") != null) && (System.getenv("AWS_SECRET_ACCESS_KEY") != null)
  }

  def hasS3Credentials: Boolean = {
    (sys.env.getOrElse("SWIFT_ACCESS_KEY_ID", System.getenv("AWS_ACCESS_KEY_ID")) != null) &&
      (sys.env.getOrElse("SWIFT_SECRET_ACCESS_KEY", System.getenv("AWS_SECRET_ACCESS_KEY")) != null)
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

  def hasHttpCredentials: Boolean = {
    val credentialsFile = Path.of(Option(System.getProperty("http.credentials.file")).getOrElse("./http_credentials.json")).toFile
    credentialsFile.isFile && credentialsFile.exists
  }

  def hasGdalInstalled: Boolean = {
    gdalInstalled
  }

  private lazy val gdalInstalled = {
    try {
      val cacheSize = Integer.valueOf(System.getenv().getOrDefault("GDAL_DATASET_CACHE_SIZE", "32"))
      GDALWarp.init(cacheSize)
      true
    } catch {
      case _: LinkageError =>
        false
    }
  }
}
