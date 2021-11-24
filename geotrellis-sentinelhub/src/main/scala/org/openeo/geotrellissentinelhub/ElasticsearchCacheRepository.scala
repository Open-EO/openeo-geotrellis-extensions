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
  trait CacheEntry {
    def matchesExpected(tileId: String, date: ZonedDateTime, bandName: String): Boolean
    def geometry: Geometry
    def filePath: Option[Path]
  }

  case class Sentinel2L2aCacheEntry(tileId: String, date: ZonedDateTime, bandName: String,
                                    override val geometry: Geometry, empty: Boolean) extends CacheEntry {
    override def matchesExpected(tileId: String, date: ZonedDateTime, bandName: String): Boolean =
      this.tileId == tileId && this.date.isEqual(date) && this.bandName == bandName

    override val filePath: Option[Path] = {
      if (empty) None
      else {
        val cacheRoot = Paths.get("/data/projects/OpenEO/sentinel-hub-s2l2a-cache")
        Some(cacheRoot.resolve(s"${BASIC_ISO_DATE format date.toLocalDate}/$tileId/$bandName.tif"))
      }
    }
  }

  private implicit object Sentinel2L2aCacheEntryHitReader extends HitReader[Sentinel2L2aCacheEntry] {
    override def read(hit: Hit): Try[Sentinel2L2aCacheEntry] = Try {
      val source = hit.sourceAsMap

      Sentinel2L2aCacheEntry(
        tileId = source("tileId").asInstanceOf[String],
        date = ZonedDateTime parse source("date").asInstanceOf[String],
        bandName = source("bandName").asInstanceOf[String],
        geometry = null, // TODO: get value
        empty = source("filePath") == null // TODO: get actual value
      )
    }
  }

  private implicit object Sentinel2L2aCacheEntryIndexable extends Indexable[Sentinel2L2aCacheEntry] {
    override def json(entry: Sentinel2L2aCacheEntry): String =
      s"""{
         |  "tileId": "${entry.tileId}",
         |  "date": "${ISO_OFFSET_DATE_TIME format entry.date}",
         |  "bandName": "${entry.bandName}",
         |  "location": ${entry.geometry.toGeoJson()},
         |  "filePath": ${entry.filePath.map(path => s""""$path"""").orNull}
         |}""".stripMargin
  }

  case class Sentinel1GrdCacheEntry(tileId: String, date: ZonedDateTime, bandName: String, backCoeff: String,
                                    orthorectify: Boolean, demInstance: String, override val geometry: Geometry,
                                    empty: Boolean) extends CacheEntry {
    override def matchesExpected(tileId: String, date: ZonedDateTime, bandName: String): Boolean =
      this.tileId == tileId && this.date.isEqual(date) && this.bandName == bandName

    override val filePath: Option[Path] = {
      require(backCoeff != null, "backCoeff is null")
      require(demInstance != null, "demInstance is null")

      if (empty) None
      else {
        val cacheRoot = Paths.get("/data/projects/OpenEO/sentinel-hub-s1grd-cache")
        val orthorectifyFlag = if (orthorectify) "orthorectified" else "non-orthorectified"

        Some(cacheRoot
          .resolve(s"$backCoeff/$orthorectifyFlag/$demInstance")
          .resolve(s"${BASIC_ISO_DATE format date.toLocalDate}/$tileId/$bandName.tif"))
      }
    }
  }

  private implicit object Sentinel1CacheEntryHitReader extends HitReader[Sentinel1GrdCacheEntry] {
    override def read(hit: Hit): Try[Sentinel1GrdCacheEntry] = Try {
      val source = hit.sourceAsMap

      Sentinel1GrdCacheEntry(
        tileId = source("tileId").asInstanceOf[String],
        date = ZonedDateTime parse source("date").asInstanceOf[String],
        bandName = source("bandName").asInstanceOf[String],
        backCoeff = source("backCoeff").asInstanceOf[String],
        orthorectify = source("orthorectify").asInstanceOf[Boolean],
        demInstance = source("demInstance").asInstanceOf[String],
        geometry = null, // TODO: get value
        empty = source("filePath") == null // TODO: get actual value
        // TODO: remaining fields are only used to query against, not to fetch
      )
    }
  }

  private implicit object Sentinel1CacheEntryIndexable extends Indexable[Sentinel1GrdCacheEntry] {
    override def json(entry: Sentinel1GrdCacheEntry): String =
      s"""{
         |  "tileId": "${entry.tileId}",
         |  "date": "${ISO_OFFSET_DATE_TIME format entry.date}",
         |  "bandName": "${entry.bandName}",
         |  "backCoeff": "${entry.backCoeff}",
         |  "orthorectify": ${entry.orthorectify},
         |  "demInstance": "${entry.demInstance}",
         |  "location": ${entry.geometry.toGeoJson()},
         |  "filePath": ${entry.filePath.map(path => s""""$path"""").orNull}
         |}""".stripMargin
  }
}

class ElasticsearchCacheRepository(uri: String) {
  // TODO: use paging instead of size(10000)
  import ElasticsearchCacheRepository._

  private def elasticClient: ElasticClient = ElasticClient(JavaClient(ElasticProperties(uri)))

  def query(cacheIndex: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
            bandNames: Seq[String]): Iterable[Sentinel2L2aCacheEntry] = {
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
        .safeTo[Sentinel2L2aCacheEntry]
        .collect {
          case Success(cacheEntry) => cacheEntry
          // faulty CacheEntries will be considered not cached, requested again and updated/fixed
        }
    } finally client.close()
  }

  def save(cacheIndex: String, entry: Sentinel2L2aCacheEntry): Unit = {
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
                     demInstance: String): Iterable[Sentinel1GrdCacheEntry] = {
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
        .safeTo[Sentinel1GrdCacheEntry]
        .collect {
          case Success(cacheEntry) => cacheEntry
          // faulty CacheEntries will be considered not cached, requested again and updated/fixed
        }
    } finally client.close()
  }

  // TODO: reduce code duplication with save()
  def saveSentinel1(cacheIndex: String, entry: Sentinel1GrdCacheEntry): Unit = {
    val client = elasticClient

    try {
      val docId = Seq(
        entry.backCoeff,
        if (entry.orthorectify) "orthorectified" else "non-orthorectified",
        entry.demInstance,
        BASIC_ISO_DATE format entry.date.toLocalDate,
        entry.tileId,
        entry.bandName
      ).mkString("-")

      client.execute {
        indexInto(cacheIndex).id(docId).doc(entry)
      }.await
    } finally client.close()
  }
}
