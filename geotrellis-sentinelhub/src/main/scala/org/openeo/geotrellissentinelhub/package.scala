package org.openeo

import java.awt.image.RenderedImage
import java.io.InputStream
import java.lang.Math.pow
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.{Base64, Scanner}

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{ArrayTile, CellSize, FloatCellType, GridBounds, MultibandTile, Tile}
import geotrellis.spark.tiling.{LayoutLevel, ZoomedLayoutScheme}
import geotrellis.spark.{KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.vector.{Extent, ProjectedExtent}
import javax.imageio.ImageIO
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.openeo.geotrellissentinelhub.Gamma0Bands.Band
import scalaj.http.{Http, HttpResponse}

import scala.annotation.tailrec
import scala.collection.immutable

package object geotrellissentinelhub {

  def createGeotrellisRDD(extent:ProjectedExtent, date:ZonedDateTime): TileLayerRDD[SpaceTimeKey] ={
    val metadata = createRDDMetadata(extent, date, date)
    metadata.gridBounds.coordsIter.map(t => SpatialKey(t._1,t._2).extent(metadata.layout)).map(extent => extent.reproject(WebMercator,LatLng)).foreach(t => println(t))
    return  null

  }

  private def parseDate(dateString:String) = LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dateString)).atStartOfDay(ZoneId.of("UTC"))

  def createSentinelHubScript(bands:Array[String]): String ={
    val customScript =
      """//VERSION=2
 |      function setup(ds) {
 |        return {
 |          components: extractBands(ds),
 |          normalization: false,  // disables input band "normalization" to [0,1], inputs are raw DNs
 |          output: [
 |          {
 |            id: "default",
 |            sampleType: SampleType.UINT8,
 |            componentCount: 3
 |          }
 |          ]
 |        };
 |      }
 |
 |      // samples is array with length 1
 |      // samples[0] properties include bands 'BXX'
 |      //            and 'viewAzimuthMean','viewZenithMean','sunZenithAngles','sunAzimuthAngles'
 |
 |      function evaluatePixel(samples, scenes, metadata) {
 |        const DN_to_reflectance = (a) => (a * metadata.normalizationFactor);
 |        const gain = 5;
 |        const px = extractBands(samples[0]);
 |        return {
        |        default: px.map(a => int8_from_01(DN_to_reflectance(a) * gain))
        |        //default: px.map(a => int8_from_01((a + 180)/360))
        |    };
 |      }
 |
 |      function int8_from_01(a) {
 |        return (a >= 0) ? (a <= 1 ? 255 * a : 255) : 0;
 |      }
 |
 |      function extractBands(a) {
 |        if(a !== undefined)
 |          return [""".stripMargin  + bands.map(b => "a."+b).mkString(",") + """];
 |        else return [0.0,0.0,0.0];
       }
      """.stripMargin


    return customScript
  }

  def createGeotrellisRDD(extent:ProjectedExtent, startDate:ZonedDateTime,endDate:ZonedDateTime, bands:Array[String]=Array("B04","B03","B02")): MultibandTileLayerRDD[SpaceTimeKey] ={
    val metadata = createRDDMetadata(extent, startDate, endDate)
    val allDates = fetchDates(extent,startDate,endDate)
    val extents = metadata.gridBounds.coordsIter//.map(t => SpatialKey(t._1,t._2).extent(metadata.layout))
    val script = new String(Base64.getEncoder.encode(createSentinelHubScript(bands).getBytes))
    val sc = SparkContext.getOrCreate()
    val rdd = sc.parallelize(allDates).cartesian(sc.parallelize(extents.toSeq)).map(date_extent => {
      val date = date_extent._1
      val spatialKey = SpatialKey(date_extent._2._1,date_extent._2._2)
      val extent = spatialKey.extent(metadata.layout)
      val tile = retrieveTileFromSentinelHub(date,extent,Some(script))
      (SpaceTimeKey(spatialKey,TemporalKey(parseDate(date))),tile)
    })
    return  MultibandTileLayerRDD[SpaceTimeKey](rdd, metadata)

  }

  private def toString(is:InputStream):String = {
    val result = new Scanner(is, "utf-8").useDelimiter("\\Z").next
    println(result)
    return result
  }

  def retrieveTileFromSentinelHub(date:String,extent: Extent, script:Option[String] = Option.empty): MultibandTile = {
    val url = "https://services.sentinel-hub.com/ogc/wms/ef60cfb1-53db-4766-9069-c5369c3161e6?service=WMS&request=GetMap&layers=L2A&styles=&format=image%2Fpng&transparent=false&version=1.1.1&width=256&height=256"
    var request = Http(url)
      .param("TIME", date + "/" + date)
      .param("BBOX", extent.xmin + "," + extent.ymin + "," + extent.xmax + "," + extent.ymax)
      .param("SRSNAME", "EPSG:3857")

    if(script.isDefined) {
      request = request.param("evalscript",script.get)
    }
    print(request.urlBuilder(request))
    val bufferedImage = request.exec(parser = { (code: Int, headers: Map[String, IndexedSeq[String]], inputStream: InputStream) => if( code == 200 ) ImageIO.read(inputStream) else  toString(inputStream)})
    bufferedImage.throwError

    val tile = GridCoverage2DConverters.convertToMultibandTile(bufferedImage.body.asInstanceOf[RenderedImage],GridCoverage2DConverters.getCellType(bufferedImage.body.asInstanceOf[RenderedImage]))
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
      .param("TIME", ISO_LOCAL_DATE.format(startDate) + "/" + ISO_LOCAL_DATE.format(endDate))
      .param("BBOX", latlon.ymin + "," + latlon.xmin + "," + latlon.ymax + "," + latlon.xmax)
      .param("SRSNAME", "EPSG:4326" )

    val response: HttpResponse[JValue] = request.execute(parser = {inputStream =>
      parse(inputStream)
    })
    implicit val formats = DefaultFormats
    val dates =(response.body \\ "features" \ "properties" \ "date").extract[List[String]]
    return dates

  }

  def retrieveS1Gamma0TileFromSentinelHub(extent: Extent, date: ZonedDateTime, width: Int, height: Int, bands: Seq[Band]): MultibandTile = {
    MultibandTile.apply(bands.map(retrieveS1Gamma0TileFromSentinelHub(extent, date, width, height, _)))
  }

  def retrieveS1Gamma0TileFromSentinelHub(extent: Extent, date: ZonedDateTime, width: Int, height: Int, band: Band): Tile = {
    val url = "https://services.sentinel-hub.com/ogc/wcs/482b4783-9a62-4275-8728-63ee2baa2087?service=WCS&request=GetCoverage&format=image/tiff;depth=32f"

    val request = Http(url)
      .param("width", width.toString)
      .param("height", height.toString)
      .param("coverage", band.toString)
      .param("time", date.format(ISO_LOCAL_DATE) + "/" + date.format(ISO_LOCAL_DATE))
      .param("bbox", extent.xmin + "," + extent.ymin + "," + extent.xmax + "," + extent.ymax)
      .param("crs", "EPSG:3857")
      .param("maxcc", "20")
    
    try {
      retry(5, s"$date + $extent") {
        val response = request.exec(parser = 
          (code: Int, headers: Map[String, IndexedSeq[String]], inputStream: InputStream) => 
            if (code == 200) GeoTiffReader.singlebandGeoTiffReader.read(IOUtils.toByteArray(inputStream)) else toString(inputStream))

        if (response.isError)
          throw new RetryException(response)

        val tiff = response.body.asInstanceOf[SinglebandGeoTiff]

        tiff.tile
      }
    } catch {
      case e: Exception =>
        println(s"${e.getMessage}")
        ArrayTile.empty(FloatCellType, width, height)
    }
  }

  @tailrec
  private def retry[T](nb: Int, message: String, i: Int = 1)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: RetryException =>
        println(s"Retry $i: $message -> ${e.response.code}: ${e.response.header("Status").getOrElse("UNKNOWN")}")
        if (i < nb) {
          val retryAfter = e.response.header("Retry-After").getOrElse("0").toInt
          val exponentialRetryAfter = retryAfter * pow(2, i - 1).toInt
          Thread.sleep(exponentialRetryAfter)
          retry(nb, message, i + 1)(fn)
        } else throw e
    }
  }

  class RetryException(val response: HttpResponse[Object]) extends Exception
}
