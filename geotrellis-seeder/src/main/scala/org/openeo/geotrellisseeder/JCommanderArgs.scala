package org.openeo.geotrellisseeder

import com.beust.jcommander.{IStringConverter, Parameter}

class IntConverter extends IStringConverter[Int] {
  override def convert(s: String): Int = s.toInt  
}

class StringOptionConverter extends IStringConverter[Option[String]] {
  override def convert(s: String): Option[String] = Some(s)
}

class BandArrayConverter extends IStringConverter[Option[Array[Band]]] {
  override def convert(s: String): Option[Array[Band]] = {
    Some(s.split(":").map(Band(_)))
  }
}

class JCommanderArgs {
  @Parameter(names = Array("--date", "-d"), required = true, description = "a date (yyyy-MM-ddThh:mm:ss.sssZ)")
  var date: String = _
  
  @Parameter(names = Array("--productType", "-p"), required = true, description = "product type")
  var productType: String = _
  
  @Parameter(names = Array("--layer", "-l"), required = false, description = "layer name", converter = classOf[StringOptionConverter])
  var layer: Option[String] = None
  
  @Parameter(names = Array("--rootPath", "-r"), required = true, description = "root path")
  var rootPath: String = _
  
  @Parameter(names = Array("--zoomLevel", "-z"), required = true, description = "zoom level", converter = classOf[IntConverter])
  var zoomLevel: Int = _
  
  @Parameter(names = Array("--colorMap", "-c"), required = false, description = "path to color map file", converter = classOf[StringOptionConverter])
  var colorMap: Option[String] = None
  
  @Parameter(names = Array("--bands", "-b"), required = false, description = "order of RGB bands", converter = classOf[BandArrayConverter])
  var bands: Option[Array[Band]] = None
  
  @Parameter(names = Array("--verbose", "-v"), required = false, description = "print debug logs")
  var verbose: Boolean = false

  @Parameter(names = Array("--help", "-h"), description = "display help", help = true)
  var help = false
}
