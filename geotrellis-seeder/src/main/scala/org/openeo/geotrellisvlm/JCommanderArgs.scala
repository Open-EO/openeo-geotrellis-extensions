package org.openeo.geotrellisvlm

import java.nio.file.{Path, Paths}
import java.time.LocalDate
import java.time.format.DateTimeParseException

import com.beust.jcommander.{IStringConverter, Parameter}

import scala.util.Try

class StringOptionConverter extends IStringConverter[Option[String]] {
  override def convert(s: String): Option[String] = Some(s)
}

class DateConverter extends IStringConverter[LocalDate] {
  override def convert(s: String): LocalDate = {
    val tryDate = Try {
      LocalDate.parse(s)
    }

    tryDate.recover {
      case _: DateTimeParseException => throw new IllegalArgumentException(s"unparsable date: $s")
    }.get
  }
}

class JCommanderArgs {
  @Parameter(names = Array("--date", "-d"), required = true, description = "a date (yyyy-MM-dd)", converter = classOf[DateConverter])
  var date: LocalDate = _
  
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
