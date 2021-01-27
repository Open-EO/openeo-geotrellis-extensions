package org.openeo.geotrellis.layers

import geotrellis.raster.RasterSource
import geotrellis.raster.gdal.GDALRasterSource

import java.time.ZonedDateTime
import scala.util.matching.Regex

class GlobalNetCdfFileLayerProvider(override protected val dataGlob: String, bandName: String,
                                    override protected val dateRegex: Regex)
  extends AbstractGlobFileLayerProvider {

  override protected def queryAll(): Array[(ZonedDateTime, RasterSource)] = {
    val datedPaths = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath)
      .groupBy { case (date, _) => date }
      .mapValues(_.map { case (_, path) => path }.max) // take RTxs into account

    datedPaths
      .toArray
      .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }
      .map { case (date, path) => date -> GDALRasterSource(s"""NETCDF:"$path":$bandName""") }
  }
}
