package org.openeo.geotrellis.layers

import java.time.ZonedDateTime

import cats.data.NonEmptyList
import geotrellis.raster.RasterSource
import geotrellis.raster.gdal.GDALRasterSource

import scala.util.matching.Regex

class AgEra5FileLayerProvider(dewPointTemperatureGlob: String, bandFileMarkers: Seq[String],
                              override protected val dateRegex: Regex) extends AbstractGlobFileLayerProvider {
  override protected def dataGlob: String = dewPointTemperatureGlob

  override def queryAll(): Array[(ZonedDateTime, RasterSource)] = {
    val datedPaths = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath)

    datedPaths
      .toArray
      .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }
      .map { case (date, path) =>
        val bandRasterSources: Seq[RasterSource] = getBandFiles(path).map(GDALRasterSource(_))
        date -> new BandCompositeRasterSource(NonEmptyList.of(bandRasterSources.head, bandRasterSources.tail: _*), crs)
      }
  }

  private def getBandFiles(dewPointTemperatureFile: String): Seq[String] = {
    val dewPointTemperatureMarker = "dewpoint-temperature"

    require(dewPointTemperatureFile contains dewPointTemperatureMarker)

    bandFileMarkers
      .map(replacement => dewPointTemperatureFile.replace(dewPointTemperatureMarker, replacement))
  }
}
