package org.openeo.geotrellisseeder

import com.beust.jcommander.{IStringConverter, Parameter}

class IntConverter extends IStringConverter[Int] {
  override def convert(s: String): Int = s.toInt
}

class StringOptionConverter extends IStringConverter[Option[String]] {
  override def convert(s: String): Option[String] = Some(s)
}

class IntOptionConverter extends IStringConverter[Option[Int]] {
  override def convert(s: String): Option[Int] = Some(s.toInt)
}

class BandArrayConverter extends IStringConverter[Option[Array[Band]]] {
  override def convert(s: String): Option[Array[Band]] = {
    Some(s.split(":").map(Band(_)))
  }
}

class OscarsSearchFilterArrayConverter extends IStringConverter[Option[Map[String, String]]] {
  override def convert(s: String): Option[Map[String, String]] = {
    if (s.nonEmpty) {
      Some(s.split(":")
        .map { s =>
          val split = s.split(",")
          (split(0), split(1))
        }
        .toMap
      )
    } else {
      None
    }
  }
}

class MaskArrayConverter extends IStringConverter[Array[Int]] {
  override def convert(s: String): Array[Int] = {
    s.split(",").map(_.toInt)
  }
}

class JCommanderArgs {
  @Parameter(names = Array("--date", "-d"), required = false, description = "a date (yyyy-MM-ddThh:mm:ss.sssZ)")
  var date: String = ""

  @Parameter(names = Array("--datePattern"), required = false, description = "date pattern to use in product glob", converter = classOf[StringOptionConverter])
  var datePattern: Option[String] = None

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

  @Parameter(names = Array("--productGlob", "-g"), required = false, description = "product glob", converter = classOf[StringOptionConverter])
  var productGlob: Option[String] = None

  @Parameter(names = Array("--maskValues", "-m"), required = false, description = "mask values", converter = classOf[MaskArrayConverter])
  var maskValues: Array[Int] = Array()

  @Parameter(names = Array("--setPermissions"), required = false, description = "set permissions", converter = classOf[StringOptionConverter])
  var setPermissions: Option[String] = None

  @Parameter(names = Array("--tooCloudyFile"), required = false, description = "file with too cloudy products", converter = classOf[StringOptionConverter])
  var tooCloudyFile: Option[String] = None

  @Parameter(names = Array("--oscarsEndpoint"), required = false, description = "oscars endpoint", converter = classOf[StringOptionConverter])
  var oscarsEndpoint: Option[String] = None

  @Parameter(names = Array("--oscarsCollection"), required = false, description = "oscars collection", converter = classOf[StringOptionConverter])
  var oscarsCollection: Option[String] = None

  @Parameter(names = Array("--oscarsSearchFilters"), required = false, description = "oscars search filters", converter = classOf[OscarsSearchFilterArrayConverter])
  var oscarsSearchFilters: Option[Map[String, String]] = None

  @Parameter(names = Array("--partitions"), required = false, description = "number of spark partitions", converter = classOf[IntOptionConverter])
  var partitions: Option[Int] = None

  @Parameter(names = Array("--resampleMethod"), required = false, description = "resample method to use, defaults to nearest neighbour. Values: nearestNeighbor, mode, bilinear", converter = classOf[StringOptionConverter])
  var resampleMethod: Option[String] = None

  @Parameter(names = Array("--selectOverlappingTile"), required = false, description = "use same values from same tile when there is overlap instead of taking max of 2 values")
  var selectOverlappingTile: Boolean = false

  @Parameter(names = Array("--verbose", "-v"), required = false, description = "print debug logs")
  var verbose: Boolean = false

  @Parameter(names = Array("--help", "-h"), description = "display help", help = true)
  var help = false
}
