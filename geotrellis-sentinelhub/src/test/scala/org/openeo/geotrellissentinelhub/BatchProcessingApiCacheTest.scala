package org.openeo.geotrellissentinelhub

import _root_.io.circe.generic.auto._
import _root_.io.circe.parser.decode
import cats.syntax.either._
import geotrellis.shapefile.ShapeFileReader
import geotrellis.vector.JTS.MultiPolygon
import geotrellis.vector._
import org.junit.{Ignore, Test}
import scalaj.http.Http

import java.time.{LocalDate, ZoneId, ZonedDateTime}

object BatchProcessingApiCacheTest {
  private val index = "features"

  private case class CacheKey(tileId: String, date: ZonedDateTime, bandName: String)

  private case class Hit(_id: String)
  private case class Hits(hits: Array[Hit])
  private case class SearchResponse(hits: Hits)

  private implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }

  private def sequentialDays(from: ZonedDateTime, to: ZonedDateTime): Stream[ZonedDateTime] = {
    def sequentialDays0(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDays0(from plusDays 1)

    sequentialDays0(from)
      .takeWhile(date => !(date isAfter to))
  }
}

class BatchProcessingApiCacheTest {
  import org.openeo.geotrellissentinelhub.BatchProcessingApiCacheTest._

  private def bulkUpsertFeatures(batch: Iterable[Feature[Geometry, String]]): Unit = {
    val items = batch.map { case Feature(geom, tileId) =>
      s"""
         |{"index": {"_id": "$tileId"}}
         |{"location": ${geom.toGeoJson()}}""".stripMargin.trim
    }

    val bulkRequestBody = items.mkString(start = "", sep = "\n", end = "\n")

    val responseBody = Http(s"http://localhost:9200/$index/_bulk")
      .headers("Content-Type" -> "application/json")
      .put(bulkRequestBody)
      .asString

    if (responseBody.isError) {
      throw new Exception(responseBody.body)
    }

    println(s"ingested a batch into $index")
  }

  @Ignore
  @Test
  def ingestTilingGrid(): Unit = {
    val tilingGrid = ShapeFileReader.readMultiPolygonFeatures[String](
      "/home/bossie/Documents/VITO/EP-3931: Optimize (sparse) polygon based requests to sentinelhub/tiling-grid-1/10km.shp",
      dataField = "name"
    )

    val batchSize = 1000
    val batches = tilingGrid.sliding(size = batchSize, step = batchSize).toSeq

    for (batch <- batches) {
      bulkUpsertFeatures(batch)
    }
  }

  private def intersectingGridTileIds(collectionId: String, geometry: Geometry): Seq[String] = {
    // TODO: fetch from ElasticSearch
    val searchRequestBody =
      s"""
        |{
        |  "query": {
        |    "geo_shape": {
        |      "location": {
        |        "shape": ${geometry.toGeoJson()},
        |        "relation": "intersects"
        |      }
        |    }
        |  },
        |  "size": 10000
        |}
        |""".stripMargin

    val responseBody = Http(s"http://localhost:9200/$index/_search")
      .headers("Content-Type" -> "application/json")
      .postData(searchRequestBody)
      .asString

    if (responseBody.isError) {
      throw new Exception(responseBody.body)
    }

    val searchResponse = decode[SearchResponse](responseBody.body)
      .valueOr(throw _)

    searchResponse.hits.hits.map(_._id)
  }

  private def queryCache(collectionId: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                 bandNames: Set[String]): Iterable[CacheKey] = {
    // TODO: query cache (Elasticsearch) for entries:
    //  - with matching collectionId
    //  - overlapping with geometry
    //  - between from and to
    //  - having one of bandNames

    List() // nothing cached
    List(CacheKey("31UFS_1_0", LocalDate.of(2021, 8, 10).atStartOfDay(ZoneId.of("UTC")), "B04")) // 1 tile cached but for another date
    List(CacheKey("31UFS_9_9", LocalDate.of(2021, 8, 17).atStartOfDay(ZoneId.of("UTC")), "B04")) // 1 tile cached but not in the geometry
    List(CacheKey("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(ZoneId.of("UTC")), "B04")) // 1 tile cached
    List(CacheKey("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(ZoneId.of("UTC")), "B03")) // 1 tile cached but the wrong band
  }

  @Test
  def determineExpectedTiles(): Unit = {
    // 0) client wants SHub batch process API to give him data for: collectionId, geometry, dateRange, bands
    val collectionId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(ZoneId.of("UTC"))
    val to = from
    val bandNames = Set("B04")

    // 1) instead of passing this straight on to SHub, we examine the request and determine the expected tiles
    val expectedCacheKeys = for {
      gridTileId <- intersectingGridTileIds(collectionId, geometry)
      date <- sequentialDays(from, to)
      bandName <- bandNames
    } yield CacheKey(tileId = gridTileId, date, bandName)

    // 2) which ones are already in the cache?
    val cachedCacheKeys = queryCache(collectionId, geometry, from, to, bandNames)

    // 3) send SHub a request for the missing cache keys
    val missingCacheKeys = expectedCacheKeys.toSet diff cachedCacheKeys.toSet

    if (missingCacheKeys.isEmpty) return

    val missingMultibandTiles = missingCacheKeys
      .groupBy(cacheKey => (cacheKey.tileId, cacheKey.date))
      .mapValues(cacheKeys => cacheKeys.map(_.bandName))
      .toSeq

    missingMultibandTiles foreach println

    // turn incomplete tiles into a SHub request:
    // determine all dates with missing positions (includes missing bands)
    // do a request for:
    // - [lower, upper]
    // - positions with missing bands
    // - missing bands

    val datesWithIncompleteBands = missingMultibandTiles
      .map { case ((_, date), _) => date }
      .distinct
      .sorted

    val lower = datesWithIncompleteBands.head
    val upper = datesWithIncompleteBands.last

    val incompleteTiles = missingMultibandTiles
      .map { case ((tileId, _), _) => tileId }
      .distinct

    val missingBands = missingMultibandTiles
      .map { case (_, missingBands) => missingBands }
      .reduce {_ union _}

    println(s"> submit a batch request for ${incompleteTiles.size} tiles, for [$lower, $upper] and $missingBands")
  }
}
