package org.openeo.geotrellis.layers

import java.nio.file.{Files, Path}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import java.util.stream.Stream

import scala.compat.java8.FunctionConverters._

trait IdentityEquals { // TODO: move this somewhere else?
  override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]
  override def hashCode(): Int = System.identityHashCode(this)
}

trait PathDateExtractor {
  protected def maxDepth: Int
  protected def extractDate(rootPath: Path, child: Path): ZonedDateTime

  def extractDates(rootPath: Path): Array[ZonedDateTime] = {
    val fullDateDirs = asJavaPredicate((path: Path) => path.getNameCount == rootPath.getNameCount + maxDepth)

    val subDirs = Files.walk(rootPath, maxDepth)
      .filter(fullDateDirs)

    val toDate = (child: Path) => this.extractDate(rootPath, child)

    val dates = {
      // compiler needs all kinds of boilerplate for reasons I cannot comprehend
      val dates: Stream[ZonedDateTime] = subDirs
        .sorted()
        .map(toDate.asJava)

      val generator = asJavaIntFunction(new Array[ZonedDateTime](_))
      dates.toArray(generator)
    }

    dates
  }

  override def equals(obj: Any): Boolean = throw new NotImplementedError("equals")
  override def hashCode(): Int = throw new NotImplementedError("hashCode")
}

object SplitYearMonthDayPathDateExtractor extends PathDateExtractor with IdentityEquals {
  override protected val maxDepth = 3

  override def extractDate(rootPath: Path, child: Path): ZonedDateTime = {
    val relativePath = rootPath.relativize(child)
    val Array(year, month, day) = relativePath.toString.split("/")
    ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
  }
}

object ProbaVPathDateExtractor extends PathDateExtractor with IdentityEquals {
  override protected val maxDepth = 2

  override def extractDate(rootPath: Path, child: Path): ZonedDateTime = {
    val relativePath = rootPath.relativize(child)
    ZonedDateTime.of(LocalDate.parse(relativePath.getFileName.toString, DateTimeFormatter.ofPattern("yyyyMMdd")), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
  }
}

object Sentinel5PPathDateExtractor {
  private val date = raw"(\d{4})(\d{2})(\d{2})".r.unanchored

  val Daily = new Sentinel5PPathDateExtractor(maxDepth = 3)
  val Monthly = new Sentinel5PPathDateExtractor(maxDepth = 2)
}

class Sentinel5PPathDateExtractor(override protected val maxDepth: Int) extends PathDateExtractor {
  import Sentinel5PPathDateExtractor._

  override def extractDate(rootPath: Path, child: Path): ZonedDateTime = {
    val relativePath = rootPath.relativize(child)
    val lastPart = relativePath.toString.split("/").last

    lastPart match {
      case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
    }
  }

  override final def equals(other: Any): Boolean = other match {
    case that: Sentinel5PPathDateExtractor => this.maxDepth == that.maxDepth
    case _ => false
  }

  override final def hashCode(): Int = maxDepth.hashCode()
}
