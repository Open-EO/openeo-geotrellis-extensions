package org.openeo.geotrellissentinelhub

import _root_.io.circe.parser.parse
import cats.syntax.either._
import com.sksamuel.elastic4s
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Hit, HitReader, TimestampElasticDate}
import geotrellis.vector._

import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_OFFSET_DATE_TIME}
import scala.util.{Failure, Success, Try}

object ElasticsearchRepository {
  case class GridTile(_id: String, location: Geometry)

  private implicit object GridTileHitReader extends HitReader[GridTile] {
    override def read(hit: Hit): Try[GridTile] = {
      val tileId = hit.id

      val location = parse(hit.sourceAsString)
        .flatMap(source => source.hcursor.downField("location").as[MultiPolygon])

      location match {
        case Right(location) => Success(GridTile(tileId, location))
        case Left(e) => Failure(e)
      }
    }
  }

  case class CacheEntry(tileId: String, date: ZonedDateTime, bandName: String, location: Geometry = null,
                        empty: Boolean = false) {
    def filePath: Option[Path] =
      if (empty) None
      else Some(Paths.get(s"/data/projects/OpenEO/sentinel-hub-s2l2a-cache/${BASIC_ISO_DATE format date.toLocalDate}/$tileId/$bandName.tif"))
  }

  private implicit object CacheEntryHitReader extends HitReader[CacheEntry] {
    override def read(hit: elastic4s.Hit): Try[CacheEntry] = Try {
      val source = hit.sourceAsMap

      CacheEntry(
        tileId = source("tileId").asInstanceOf[String],
        date = ZonedDateTime parse source("date").asInstanceOf[String],
        bandName = source("bandName").asInstanceOf[String],
        empty = source("filePath") == null // TODO: get actual value
      )
    }
  }
}

// TODO: split up in Cache and TilingGrid repositories
class ElasticsearchRepository(uri: String) {
  // TODO: use paging instead of size(10000)
  import ElasticsearchRepository._

  private def elasticClient: ElasticClient = ElasticClient(JavaClient(ElasticProperties(uri)))

  def intersectingGridTiles(tilingGridIndex: String, geometry: Geometry): Iterable[GridTile] = {
    val client = elasticClient

    try {
      val resp = client.execute {
        search(tilingGridIndex)
          .rawQuery(
            s"""
               |{
               |  "geo_shape": {
               |    "location": {
               |      "shape": ${geometry.toGeoJson()},
               |      "relation": "intersects"
               |    }
               |  }
               |}""".stripMargin)
          .size(10000)
      }.await

      resp.result
        .safeTo[GridTile]
        .map(_.get)
    } finally client.close()
  }

  def getGeometry(tilingGridIndex: String, tileId: String): Geometry = {
    val client = elasticClient

    try {
      val resp = client.execute {
        get(tileId).from(tilingGridIndex)
      }.await

      resp.result
        .safeTo[GridTile]
        .map(_.location)
        .get
    } finally client.close()
  }

  def queryCache(cacheIndex: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                 bandNames: Seq[String]): Iterable[CacheEntry] = {
    val client = elasticClient

    try {
      val resp = client.execute {
        search(cacheIndex)
          .query(
            boolQuery().filter(
              rawQuery(
                s"""
                   |{
                   |  "geo_shape": {
                   |    "location": {
                   |      "shape": ${geometry.toGeoJson()},
                   |      "relation": "intersects"
                   |    }
                   |  }
                   |}""".stripMargin),
              rangeQuery("date")
                .gte(TimestampElasticDate(from.toInstant.toEpochMilli))
                .lte(TimestampElasticDate(to.toInstant.toEpochMilli)),
              termsQuery("bandName", bandNames)
            ))
          .size(10000)
      }.await

      resp.result
        .safeTo[CacheEntry]
        .collect {
          case Success(cacheEntry) => cacheEntry
          // faulty CacheEntries will be considered not cached, requested again and updated/fixed
        }
    } finally client.close()
  }

  def cache(cacheIndex: String, entry: CacheEntry): Unit = {
    val client = elasticClient

    try {
      val docId = s"${BASIC_ISO_DATE format entry.date.toLocalDate}-${entry.tileId}-${entry.bandName}"

      client.execute {
        indexInto(cacheIndex).id(docId).doc(
          s"""
             |{
             |  "tileId": "${entry.tileId}",
             |  "date": "${ISO_OFFSET_DATE_TIME format entry.date}",
             |  "bandName": "${entry.bandName}",
             |  "location": ${entry.location.toGeoJson()},
             |  "filePath": ${entry.filePath.map(path => s""""$path"""").orNull}
             |}""".stripMargin)
      }.await
    } finally client.close()
  }
}
