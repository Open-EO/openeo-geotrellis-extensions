package org.openeo.geotrellissentinelhub

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Hit, HitReader, Indexable, TimestampElasticDate}
import geotrellis.vector._

import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_OFFSET_DATE_TIME}
import scala.util.{Success, Try}

object ElasticsearchCacheRepository {
  case class CacheEntry(tileId: String, date: ZonedDateTime, bandName: String, location: Geometry = null,
                        empty: Boolean = false) {
    def filePath: Option[Path] =
      if (empty) None
      else Some(Paths.get(s"/data/projects/OpenEO/sentinel-hub-s2l2a-cache/${BASIC_ISO_DATE format date.toLocalDate}/$tileId/$bandName.tif"))
  }

  private implicit object CacheEntryHitReader extends HitReader[CacheEntry] {
    override def read(hit: Hit): Try[CacheEntry] = Try {
      val source = hit.sourceAsMap

      CacheEntry(
        tileId = source("tileId").asInstanceOf[String],
        date = ZonedDateTime parse source("date").asInstanceOf[String],
        bandName = source("bandName").asInstanceOf[String],
        empty = source("filePath") == null // TODO: get actual value
        // remaining fields are only used to query against, not to fetch
      )
    }
  }

  private implicit object CacheEntryIndexable extends Indexable[CacheEntry] {
    override def json(entry: CacheEntry): String =
      s"""{
         |  "tileId": "${entry.tileId}",
         |  "date": "${ISO_OFFSET_DATE_TIME format entry.date}",
         |  "bandName": "${entry.bandName}",
         |  "location": ${entry.location.toGeoJson()},
         |  "filePath": ${entry.filePath.map(path => s""""$path"""").orNull}
         |}""".stripMargin
  }

  case class Sentinel1CacheEntry(tileId: String, date: ZonedDateTime, bandName: String, backCoeff: String = null,
                                 orthorectify: Boolean = false, demInstance: String = null, location: Geometry = null,
                                 empty: Boolean) {
    private val formattedDate = BASIC_ISO_DATE format date.toLocalDate

    def filePath: Option[Path] = {
      if (empty) None
      else {
        val root = Paths.get("/data/projects/OpenEO/sentinel-hub-s1grd-cache")
        val orthorectifyFlag = if (orthorectify) "orthorectified" else "non-orthorectified"
        val subDirectory = s"$backCoeff/$orthorectifyFlag/$demInstance/$formattedDate/$tileId/$bandName.tif"
        Some(root.resolve(subDirectory))
      }
    }
  }

  private implicit object Sentinel1CacheEntryHitReader extends HitReader[Sentinel1CacheEntry] {
    override def read(hit: Hit): Try[Sentinel1CacheEntry] = Try {
      val source = hit.sourceAsMap

      Sentinel1CacheEntry(
        tileId = source("tileId").asInstanceOf[String],
        date = ZonedDateTime parse source("date").asInstanceOf[String],
        bandName = source("bandName").asInstanceOf[String],
        empty = source("filePath") == null // TODO: get actual value
        // remaining fields are only used to query against, not to fetch
      )
    }
  }

  private implicit object Sentinel1CacheEntryIndexable extends Indexable[Sentinel1CacheEntry] {
    override def json(entry: Sentinel1CacheEntry): String =
      s"""{
         |  "tileId": "${entry.tileId}",
         |  "date": "${ISO_OFFSET_DATE_TIME format entry.date}",
         |  "bandName": "${entry.bandName}",
         |  "backCoeff": "${entry.backCoeff}",
         |  "orthorectify": ${entry.orthorectify},
         |  "demInstance": ${entry.demInstance},
         |  "location": ${entry.location.toGeoJson()},
         |  "filePath": ${entry.filePath.map(path => s""""$path"""").orNull}
         |}""".stripMargin
  }
}

class ElasticsearchCacheRepository(uri: String) {
  // TODO: use paging instead of size(10000)
  import ElasticsearchCacheRepository._

  private def elasticClient: ElasticClient = ElasticClient(JavaClient(ElasticProperties(uri)))

  def query(cacheIndex: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
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

  def save(cacheIndex: String, entry: CacheEntry): Unit = {
    val client = elasticClient

    try {
      val docId = Seq(
        BASIC_ISO_DATE format entry.date.toLocalDate,
        entry.tileId,
        entry.bandName
      ).mkString("-")

      client.execute {
        indexInto(cacheIndex).id(docId).doc(entry)
      }.await
    } finally client.close()
  }

  // TODO: reduce code duplication with query()
  def querySentinel1(cacheIndex: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                     bandNames: Seq[String], backCoeff: String, orthorectify: Boolean,
                     demInstance: String): Iterable[Sentinel1CacheEntry] = {
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
              termsQuery("bandName", bandNames),
              termQuery("backCoeff", backCoeff),
              termQuery("orthorectify", orthorectify),
              termQuery("demInstance", demInstance)
            ))
          .size(10000)
      }.await

      resp.result
        .safeTo[Sentinel1CacheEntry]
        .collect {
          case Success(cacheEntry) => cacheEntry
          // faulty CacheEntries will be considered not cached, requested again and updated/fixed
        }
    } finally client.close()
  }

  // TODO: reduce code duplication with save()
  def saveSentinel1(cacheIndex: String, entry: Sentinel1CacheEntry): Unit = {
    val client = elasticClient

    try {
      val docId = Seq(
        BASIC_ISO_DATE format entry.date.toLocalDate,
        entry.tileId,
        entry.bandName,
        entry.backCoeff,
        if (entry.orthorectify) "orthorectified" else "non-orthorectified",
        entry.demInstance
      ).mkString("-")

      client.execute {
        indexInto(cacheIndex).id(docId).doc(entry)
      }.await
    } finally client.close()
  }
}
