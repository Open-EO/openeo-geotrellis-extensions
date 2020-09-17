package org.openeo

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import _root_.geotrellis.layer._
import _root_.geotrellis.raster.MultibandTile
import _root_.geotrellis.spark.partition.PartitionerIndex
import _root_.geotrellis.spark.pyramid.Pyramid
import _root_.geotrellis.store.index.zcurve.{Z3, ZSpaceTimeKeyIndex}
import geotrellis.store.accumulo.{AccumuloAttributeStore, AccumuloLayerHeader}
import geotrellis.store.{AttributeStore, LayerId}
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector.{Geometry, GeometryCollection, MultiPolygon, Point, Polygon}
import io.circe._
import org.apache.spark.rdd.RDD
import org.openeo.geotrellisaccumulo.zcurve.SfCurveZSpaceTimeKeyIndex

import scala.collection.JavaConverters.mapAsScalaMapConverter

package object geotrellisaccumulo {

    def createGeotrellisPyramid(levels: java.util.Map[Integer,RDD[(SpaceTimeKey,MultibandTile)]  with Metadata[TileLayerMetadata[SpaceTimeKey]]]): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] ={
      val map: Map[Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = levels.asScala.toMap.map(t=>(t._1.toInt,t._2))
      new Pyramid(map)
    }

  def decodeIndexKey(region:BigInt):SpaceTimeKey = {
    val (x,y,t) = new Z3(region.longValue() << 8 ).decode

    new SpaceTimeKey(x,y,t*1000L * 60 * 60 * 24 )
  }
  /*
  def decodeIndexKey(region:BigInt):SpaceTimeKey = {
    val (x,y,t) = new Z3(region.longValue()).decode
    val day = t%31
    val month = ((t-day)/31)%13
    val year = ((t - day)/31-month)/13
    SpaceTimeKey(x <<4,y << 4,ZonedDateTime.of(LocalDate.of(year,month,day+1),LocalTime.MIDNIGHT,ZoneId.of("UTC")))
  }*/

  /*implicit object SpaceTimeByMonthPartitioner extends  PartitionerIndex[SpaceTimeKey] {
    private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col >> 4, key.row >> 4, 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
      Z3.zranges(toZ(keyRange._1), toZ(keyRange._2))
  }*/
  implicit object SpaceTimeByMonthPartitioner extends  PartitionerIndex[SpaceTimeKey] {

    val keyIndex = SfCurveZSpaceTimeKeyIndex.byDay(null)
    //private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col , key.row , 31*13*key.time.getYear + 31*key.time.getMonthValue + key.time.getDayOfMonth-1)

    def toIndex(key: SpaceTimeKey): BigInt = keyIndex.toIndex(key) >> 8

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] = {
      val originalRanges = keyIndex.indexRanges(keyRange)

      val mappedRanges = originalRanges.map(range => (range._1 >> 8,(range._2 >> 8) ))

      val distinct = mappedRanges.distinct
      var previousEnd: BigInt = null

      //filter out regions that only span 1 value, and are already included in another region, so basically duplicates
      var lookAheadIndex = 0
      val filtered = distinct.filter(range => {
        lookAheadIndex +=1
        try{
          if(range._1 == previousEnd && range._1 == range._2) {
            false
          }else if(lookAheadIndex < distinct.size && range._1 == range._2 && distinct(lookAheadIndex)._1 == range._2) {
            false
          }else{
            true
          }
        }finally {
          previousEnd = range._2
        }

      })
      return filtered
    }

  }

  /**
   * Simple in-memory TTL cache
   *
   * TODO: replace this with more advanced cache from some library?
   */
  class TtlCache[K, V](val ttl: Int = 60) {
    private val cache = collection.mutable.Map[K, (Long, V)]()

    def getOrElseUpdate(key: K, op: => V): V = {
      val now = java.time.Instant.now().getEpochSecond
      cache.get(key) match {
        case Some((expiry, value)) if now < expiry => value
        case _ =>
          val value = op
          cache.put(key, (now + ttl, value))
          value
      }
    }
  }

  @throws[ParsingFailure]("if the JSON is invalid")
  @throws[DecodingFailure]("if the JSON is valid but can't be decoded to the requested Geometry type")
  def parseGeometry(geometry: String): (String, Geometry) = {
    import geotrellis.vector.io.json.GeometryFormats._

    GeoJson.parse[Geometry](geometry) match {
      case geom: Point => ("Point", geom)
      case geom: Polygon => ("Polygon", geom)
      case geom: MultiPolygon => ("MultiPolygon", geom)
      case geom: GeometryCollection => ("GeometryCollection", geom)
      case geom => ("Unknown", geom)
    }
  }


  implicit class AttributeStoreExtensions(attributeStore: AttributeStore) {
    def baseLayer(layerName: String): LayerId =
      attributeStore.layerIds
        .filter(_.name == layerName)
        .maxBy(_.zoom)

    def baseLayer(layerName: String, suggestedZoom: Int): LayerId = {
      val zoomLevels = for {
        LayerId(name, zoom) <- attributeStore.layerIds
        if name == layerName
      } yield zoom

      val maxZoom = zoomLevels
        .find(_ == suggestedZoom)
        .getOrElse(zoomLevels.max)

      LayerId(layerName, maxZoom)
    }
  }

  implicit class AccumuloAttributeStoreExtensions(attributeStore: AccumuloAttributeStore) {
    def isMultiBand(layerId: LayerId): Boolean = {
      val valueClass = attributeStore.readHeader[AccumuloLayerHeader](layerId).valueClass
      valueClass == classOf[MultibandTile].getName
    }
  }


  implicit def zonedDateTimeEncoder: Encoder[ZonedDateTime] = new Encoder[ZonedDateTime] {
    override def apply(date: ZonedDateTime): Json = Json.fromLong(date.toInstant.toEpochMilli)
  }

  implicit def zonedDateTimeDecoder: Decoder[ZonedDateTime] = Decoder.decodeLong.map { millis =>
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
  }

}
