package org.openeo.geotrellissentinelhub

object Utils {
  def getRequiredSystemProperty(key: String): String =
    Option(System.getProperty(key)).getOrElse(throw new IllegalStateException(s"$key system property not set"))

  def getRequiredEnvironmentVariable(name: String): String =
    Option(System.getenv(name)).getOrElse(throw new IllegalStateException(s"$name environment variable not set"))

  def clientId: String = getRequiredEnvironmentVariable("SENTINELHUB_CLIENT_ID")
  def clientSecret: String = getRequiredEnvironmentVariable("SENTINELHUB_CLIENT_SECRET")
}
