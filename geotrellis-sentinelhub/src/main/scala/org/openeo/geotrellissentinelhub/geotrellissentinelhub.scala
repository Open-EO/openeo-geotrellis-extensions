package org.openeo


import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{CellSize, FloatCellType, GridBounds, MultibandTile}
import geotrellis.spark.tiling.{LayoutLevel, ZoomedLayoutScheme}
import geotrellis.spark.{KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.vector.{Extent, ProjectedExtent}
import javax.imageio.ImageIO
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scalaj.http.{Http, HttpResponse}

import scala.collection.immutable

package object geotrellissentinelhub {


  def createGeotrellisRDD(extent:ProjectedExtent, date:ZonedDateTime): TileLayerRDD[SpaceTimeKey] ={
    val metadata = createRDDMetadata(extent, date, date)
    metadata.gridBounds.coordsIter.map(t => SpatialKey(t._1,t._2).extent(metadata.layout)).map(extent => extent.reproject(WebMercator,LatLng)).foreach(t => println(t))
    return  null

  }

  private def parseDate(dateString:String) = LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dateString)).atStartOfDay(ZoneId.of("UTC"))


  def createGeotrellisRDD(extent:ProjectedExtent, startDate:ZonedDateTime,endDate:ZonedDateTime): MultibandTileLayerRDD[SpaceTimeKey] ={
    val metadata = createRDDMetadata(extent, startDate, endDate)
    val allDates = fetchDates(extent,startDate,endDate)
    val extents = metadata.gridBounds.coordsIter//.map(t => SpatialKey(t._1,t._2).extent(metadata.layout))
    val sc = SparkContext.getOrCreate()
    val rdd = sc.parallelize(allDates).cartesian(sc.parallelize(extents.toSeq)).map(date_extent => {
      val date = date_extent._1
      val spatialKey = SpatialKey(date_extent._2._1,date_extent._2._2)
      val extent = spatialKey.extent(metadata.layout)
      val tile = retrieveTileFromSentinelHub(date,extent)
      (SpaceTimeKey(spatialKey,TemporalKey(parseDate(date))),tile)
    })
    return  MultibandTileLayerRDD[SpaceTimeKey](rdd, metadata)

  }

  def retrieveTileFromSentinelHub(date:String,extent: Extent): MultibandTile = {
    val url = "https://services.sentinel-hub.com/ogc/wms/ef60cfb1-53db-4766-9069-c5369c3161e6?CmxldCBtaW5WYWwgPSAwLjA7CmxldCBtYXhWYWwgPSAwLjQ7CgpsZXQgdml6ID0gbmV3IEhpZ2hsaWdodENvbXByZXNzVmlzdWFsaXplcihtaW5WYWwsIG1heFZhbCk7CgpmdW5jdGlvbiBldmFsdWF0ZVBpeGVsKHNhbXBsZXMpIHsKICAgIGxldCB2YWwgPSBbc2FtcGxlc1swXS5CMDQsIHNhbXBsZXNbMF0uQjAzLCBzYW1wbGVzWzBdLkIwMl07CiAgICByZXR1cm4gdmFsLm1hcCgodiwgaSkgPT4gdml6LnByb2Nlc3ModiwgaSkpOwp9CgpmdW5jdGlvbiBzZXR1cChkcykgewogICAgc2V0SW5wdXRDb21wb25lbnRzKFtkcy5CMDIsIGRzLkIwMywgZHMuQjA0XSk7CiAgICBzZXRPdXRwdXRDb21wb25lbnRDb3VudCgzKTsKfQo=&service=WMS&request=GetMap&layers=L2A&styles=&format=image%2Fpng&transparent=false&version=1.1.1&evalscript=CmxldCBtaW5WYWwgPSAwLjA7CmxldCBtYXhWYWwgPSAwLjQ7CgpsZXQgdml6ID0gbmV3IEhpZ2hsaWdodENvbXByZXNzVmlzdWFsaXplcihtaW5WYWwsIG1heFZhbCk7CgpmdW5jdGlvbiBldmFsdWF0ZVBpeGVsKHNhbXBsZXMpIHsKICAgIGxldCB2YWwgPSBbc2FtcGxlc1swXS5CMDQsIHNhbXBsZXNbMF0uQjAzLCBzYW1wbGVzWzBdLkIwMl07CiAgICByZXR1cm4gdmFsLm1hcCgodiwgaSkgPT4gdml6LnByb2Nlc3ModiwgaSkpOwp9CgpmdW5jdGlvbiBzZXR1cChkcykgewogICAgc2V0SW5wdXRDb21wb25lbnRzKFtkcy5CMDIsIGRzLkIwMywgZHMuQjA0XSk7CiAgICBzZXRPdXRwdXRDb21wb25lbnRDb3VudCgzKTsKfQo%3D&width=256&height=256"
    val request = Http(url)
      .param("TIME", date + "/" + date)
      .param("BBOX", extent.xmin + "," + extent.ymin + "," + extent.xmax + "," + extent.ymax)
      .param("SRSNAME", "EPSG:3857")
    val bufferedImage = request.execute(parser = { inputStream => ImageIO.read(inputStream) })
    val tile = GridCoverage2DConverters.convertToMultibandTile(bufferedImage.body,GridCoverage2DConverters.getCellType(bufferedImage.body))
    return tile
  }

  private def createRDDMetadata(extent: ProjectedExtent, startDate: ZonedDateTime, endDate: ZonedDateTime): TileLayerMetadata[SpaceTimeKey] = {
    val layerTileSize = 256

    val layoutscheme = ZoomedLayoutScheme(extent.crs, layerTileSize)
    val layoutLevel: LayoutLevel = layoutscheme.levelFor(extent.extent, CellSize(10, 10))
    val targetBounds: GridBounds = layoutLevel.layout.mapTransform.extentToBounds(extent.extent)
    val metadata = new TileLayerMetadata[SpaceTimeKey](FloatCellType, layoutLevel.layout, extent.extent, extent.crs, new KeyBounds[SpaceTimeKey](SpaceTimeKey.apply(targetBounds.colMin, targetBounds.rowMin, startDate), SpaceTimeKey.apply(targetBounds.colMax, targetBounds.rowMax, endDate)))
    return metadata
  }

  def fetchDates(bbox:ProjectedExtent, startDate: ZonedDateTime, endDate:ZonedDateTime, maxCloudCover:Float = 86): immutable.Seq[String] = {

    val url = "https://services.sentinel-hub.com/ogc/wfs/ef60cfb1-53db-4766-9069-c5369c3161e6?REQUEST=GetFeature&TYPENAMES=S2.TILE&OUTPUTFORMAT=application/json"
    val latlon = bbox.reproject(LatLng)
    val request = Http(url)
      .param("MAXCC", maxCloudCover.toString)
      .param("TIME", DateTimeFormatter.ISO_LOCAL_DATE.format(startDate) + "/" + DateTimeFormatter.ISO_LOCAL_DATE.format(endDate))
      .param("BBOX", latlon.ymin + "," + latlon.xmin + "," + latlon.ymax + "," + latlon.xmax)
      .param("SRSNAME", "EPSG:4326" )

    val response: HttpResponse[JValue] = request.execute(parser = {inputStream =>
      parse(inputStream)
    })
    implicit val formats = DefaultFormats
    val dates =(response.body \\ "features" \ "properties" \ "date").extract[List[String]]
    return dates

  }
}
