package org.openeo.geotrellissentinelhub

object Utils {
  def getRequiredSystemProperty(key: String): String =
    Option(System.getProperty(key)).getOrElse(throw new IllegalStateException(s"$key system property not set"))
}
