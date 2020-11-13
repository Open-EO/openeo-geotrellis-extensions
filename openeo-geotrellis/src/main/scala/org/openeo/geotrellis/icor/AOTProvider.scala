package org.openeo.geotrellis.icor

import java.nio.file.{Files, Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.function.Consumer

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.Tile
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.vector._

object AOTProvider{

  def overpasshours = 11.5 //Sentinel-2 local overpass time, can be sensor specific
  def degreesPerHour = 15

  def computeUTCOverPasstime(longitude: Double, productUTCTime: ZonedDateTime) = {
    val utcOffsetHours = longitude / degreesPerHour
    val utcOverPassHours = overpasshours - utcOffsetHours
    val utcOverPasstime = productUTCTime.plusHours(utcOverPassHours.toLong)
    utcOverPasstime
  }


  class ClosestPathFinder(targetTime:ZonedDateTime) extends Consumer[Path]{
    val dateTimeFormat:DateTimeFormatter = DateTimeFormatter.ofPattern("'CAMS_NRT_aod550_'yyyyMMdd'T'HHmmssX'.tif'")

    var closestTime:Long = Long.MaxValue
    var closestPath:Path = null
    override def accept(p: Path): Unit = {
      val name = p.getFileName.toString
      val fileTime = ZonedDateTime.from(dateTimeFormat.parse(name))

      val difference = Math.abs(ChronoUnit.MINUTES.between(fileTime,targetTime))

      if(difference < closestTime){
        closestTime = difference
        closestPath = p
      }
    }
  }
}

class AOTProvider(basePath:String = "/data/MEP/ECMWF/cams/aod550/%1$tY/%1$tm/CAMS_NRT_aod550_%1$tY%1$tm%1$tdT*Z.tif") {

  import AOTProvider._

  def computeAOT(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {
    val targetExtent = layoutDefinition.mapTransform.apply(key)
    val longitude:Double = targetExtent.center.reproject(targetCRS,LatLng).x

    val productUTCTime = key.time

    val utcOverPasstime: ZonedDateTime = computeUTCOverPasstime(longitude, productUTCTime)
    val aotPath = basePath.format(utcOverPasstime)
    val bestAOTProduct = findClosestPath(aotPath,utcOverPasstime)

    val rasterSource = GeoTiffRasterSource(bestAOTProduct.toAbsolutePath.toString)

    val raster = rasterSource.reprojectToRegion(targetCRS,layoutDefinition.toRasterExtent()).read(targetExtent)
    raster.get.tile.band(0)

  }


  def findClosestPath(basePath:String, time:ZonedDateTime) :Path = {
    val paths = Files.newDirectoryStream(Paths.get(basePath).getParent, "CAMS_NRT_aod550_*Z.tif")
    val finder = new ClosestPathFinder(time)
    paths.forEach(finder)
    return finder.closestPath
  }

}
