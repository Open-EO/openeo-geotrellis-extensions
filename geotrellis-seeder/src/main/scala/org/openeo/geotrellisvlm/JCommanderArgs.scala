package org.openeo.geotrellisvlm

import com.beust.jcommander.{IStringConverter, Parameter}

class StringOptionConverter extends IStringConverter[Option[String]] {
  override def convert(s: String): Option[String] = Some(s)
}

class JCommanderArgs {
  @Parameter(names = Array("--date", "-d"), required = true, description = "a date (yyyy-MM-ddThh:mm:ss#ssssZ)")
  var date: String = _
  
  @Parameter(names = Array("--productType", "-p"), required = true, description = "product type")
  var productType: String = _
  
  @Parameter(names = Array("--rootPath", "-r"), required = true, description = "root path")
  var rootPath: String = _
  
  @Parameter(names = Array("--colorMap", "-c"), required = false, description = "path to color map file", converter = classOf[StringOptionConverter])
  var colorMap: Option[String] = None
  
  @Parameter(names = Array("--verbose", "-v"), required = false, description = "print debug logs")
  var verbose: Boolean = false

  @Parameter(names = Array("--help", "-h"), description = "display help", help = true)
  var help = false
}
