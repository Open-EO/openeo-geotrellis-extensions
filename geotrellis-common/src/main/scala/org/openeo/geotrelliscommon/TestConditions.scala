package org.openeo.geotrelliscommon

import java.nio.file.{Files, Path}

object TestConditions {

  def hasSentinelHubCredentials(): Boolean = {
    (System.getenv("SENTINELHUB_CLIENT_ID") != null) && (System.getenv("SENTINELHUB_CLIENT_SECRET") != null)
  }

  def hasAwsCredentials(): Boolean = {
    // TODO dfs
    false && (System.getenv("AWS_ACCESS_KEY_ID") != null) && (System.getenv("AWS_SECRET_ACCESS_KEY") != null)
  }

  def hasMTDAData(): Boolean = {
    val folder = Path.of("/data/MTDA").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasProjectsData(): Boolean = {
    val folder = Path.of("/data/projects").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasMtdaDevData(): Boolean = {
    val folder = Path.of("/data/MTDA_DEV").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasMepData(): Boolean = {
    val folder = Path.of("/data/MEP").toFile
    folder.exists && folder.isDirectory && folder.list() != null && !folder.list().isEmpty
  }

  def hasHttpCredentials(): Boolean = {
    val credentialsFile = Path.of(Option(System.getProperty("http.credentials.file")).getOrElse("./http_credentials.json")).toFile
    credentialsFile.isFile && credentialsFile.exists
  }

  // TODO
  def hasGdalInstalled(): Boolean = {
    false
  }
}
