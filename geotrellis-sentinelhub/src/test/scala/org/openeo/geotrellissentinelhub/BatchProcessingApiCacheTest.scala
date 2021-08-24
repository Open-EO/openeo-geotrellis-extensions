package org.openeo.geotrellissentinelhub

import _root_.io.circe.generic.auto._
import _root_.io.circe.Json
import _root_.io.circe.parser.decode
import cats.syntax.either._
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, RequestSuccess, TimestampElasticDate}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.get.GetResponse
import geotrellis.vector.io.json.GeoJson

import java.nio.file.Paths
import java.time.LocalTime
/*import com.sksamuel.elastic4s.requests.searches.GeoPoint
import com.sksamuel.elastic4s.requests.searches.queries.geo.ShapeRelation.INTERSECTS
import com.sksamuel.elastic4s.requests.searches.queries.geo.{InlineShape, PointShape}*/
import geotrellis.proj4.LatLng
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.JTS.MultiPolygon
import geotrellis.vector._

import java.util.Collections
import org.junit.{Ignore, Test}

import scala.collection.JavaConverters._
import scalaj.http.Http

import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_OFFSET_DATE_TIME}
import java.time.{LocalDate, ZoneId, ZonedDateTime}

object BatchProcessingApiCacheTest {
  private val featureIndex = "features" // tiling grid "1"
  private val utc = ZoneId.of("UTC")

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  private case class CacheEntry(tileId: String, date: ZonedDateTime, bandName: String, location: Geometry = null) {
    def filePath: String = s"/tmp/cache/${BASIC_ISO_DATE format date.toLocalDate}/$tileId/$bandName.tif"
  }

  private case class Hit(_id: String, _source: Json)
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

  private def elasticClient: ElasticClient = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

  private trait Cache {
    def query(datasetId: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime, // FIXME: accept more geometries
              bandNames: Set[String]): Iterable[CacheEntry]

    def add(datasetId: String, entry: CacheEntry)
  }

  private class FixedCache(entries: Iterable[CacheEntry]) extends Cache {
    override def query(datasetId: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                       bandNames: Set[String]): Iterable[CacheEntry] = entries

    override def add(datasetId: String, entry: CacheEntry): Unit = ()
  }

  private class ElasticsearchCache extends Cache {
    private val cacheIndex = "cache" // dataset ID "S2L2A"

    override def query(datasetId: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                       bandNames: Set[String]): Iterable[CacheEntry] = {
      // query cache (Elasticsearch) for entries:
      //  - with matching datasetId
      //  - overlapping with geometry
      //  - between from and to
      //  - having one of bandNames

      val client = elasticClient

      try {
        val resp = client.execute {
          search(cacheIndex)
            .query(
              boolQuery().filter(
                /*geoShapeQuery("location", InlineShape(PointShape(GeoPoint(lat = geometry.y, long = geometry.x)))).relation(INTERSECTS),*/
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

        resp match {
          case results: RequestSuccess[com.sksamuel.elastic4s.requests.searches.SearchResponse] =>
            // TODO: define a HitReader to map this to a CacheEntry
            results.result.hits.hits.map(hit => CacheEntry(
              tileId = hit.sourceField("tileId").asInstanceOf[String],
              date = ZonedDateTime parse hit.sourceField("date").asInstanceOf[String],
              bandName = hit.sourceField("bandName").asInstanceOf[String]
            ))
            // FIXME: handle failures
        }
      } finally client.close()
    }

    override def add(datasetId: String, entry: CacheEntry): Unit = {
      val tileId = entry.tileId
      val date = ISO_OFFSET_DATE_TIME format entry.date
      val bandName = entry.bandName

      val client = elasticClient

      try {
        client.execute {
          indexInto(cacheIndex).id(s"${tileId}_${date}_${bandName}").doc(
            s"""
               |{
               |  "tileId": "${tileId}",
               |  "date": "${date}",
               |  "bandName": "${bandName}",
               |  "location": ${entry.location.toGeoJson()},
               |  "filePath": "${entry.filePath}"
               |}""".stripMargin)
        }.await
      } finally client.close()
    }
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

    val responseBody = Http(s"http://localhost:9200/$featureIndex/_bulk")
      .headers("Content-Type" -> "application/json")
      .put(bulkRequestBody)
      .asString

    if (responseBody.isError) {
      throw new Exception(responseBody.body)
    }

    println(s"ingested a batch into $featureIndex")
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

  private def intersectingGridTiles(datasetId: String, geometry: Geometry): Seq[(String, Geometry)] = {
    val searchRequestBody = // TODO: replace with Elasticsearch client
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

    val responseBody = Http(s"http://localhost:9200/$featureIndex/_search")
      .headers("Content-Type" -> "application/json")
      .postData(searchRequestBody)
      .asString

    if (responseBody.isError) {
      throw new Exception(responseBody.body)
    }

    val searchResponse = decode[SearchResponse](responseBody.body)
      .valueOr(throw _)

    searchResponse.hits.hits.map { case Hit(_id, _source) =>
      val tileId = _id

      val geometry = for {
        o <- _source.asObject
        location <- o("location")
      } yield GeoJson.parse[MultiPolygon](location.noSpaces)

      (tileId, geometry.get)
    }
  }

  private def getGeometry(tileId: String): Geometry = {
    // TODO: improve this JSON parsing (and everywhere else)
    val client = elasticClient

    try {
      val resp = client.execute {
        get(tileId).from(featureIndex)
      }.await

      val geometry = resp match {
        case result: RequestSuccess[GetResponse] =>
          val source = decode[Json](result.result.sourceAsString)
            .valueOr(throw _)

          for {
            o <- source.asObject
            location <- o("location")
          } yield GeoJson.parse[MultiPolygon](location.noSpaces)

          // FIXME: handle failures
      }

      geometry.get
    } finally client.close()
  }

  private def getNarrowerRequest(datasetId: String, geometry: Geometry, from: ZonedDateTime, to: ZonedDateTime,
                                 bandNames: Set[String], cache: Cache): Option[(Seq[(String, Geometry)], ZonedDateTime, ZonedDateTime, Set[String])] = {

    // 1) instead of passing this straight on to SHub, we examine the request and determine the expected tiles
    val expectedTiles = for {
      (gridTileId, geometry) <- intersectingGridTiles(datasetId, geometry)
      date <- sequentialDays(from, to)
      bandName <- bandNames
    } yield (gridTileId, geometry, date, bandName)

    // 2) which ones are already in the cache?
    val cacheEntries = cache.query(datasetId, geometry, from, to, bandNames)

    // 3) send SHub a request for the missing cache keys
    // TODO: optimize?
    val missingTiles = expectedTiles
      .filterNot { case (tileId, _, date, bandName) =>
        cacheEntries.exists(cachedTile => cachedTile.tileId == tileId && cachedTile.date.isEqual(date) && cachedTile.bandName == bandName)
      }
      .toSet

    if (missingTiles.isEmpty) {
      println("everything's cached, no need for additional requests")
      return None
    }

    val missingMultibandTiles = missingTiles
      .groupBy { case (tileId, _, date, _) => (tileId, date) }
      .mapValues { cacheKeys =>
        val (_, geometry, _, _) = cacheKeys.head // same tile ID so same geometry
        val bandNames = cacheKeys.map { case (_, _, _, bandName) => bandName }
        (geometry, bandNames)
      }
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
      .map { case ((tileId,  _), (geometry, _)) => (tileId, geometry) }
      .distinct

    val missingBands = missingMultibandTiles
      .map { case (_, (_, missingBands)) => missingBands }
      .reduce {_ union _}

    println(
      s"""submit a batch request for:
         | - $datasetId
         | - ${incompleteTiles.size} positions
         | - [$lower, $upper]
         | - $missingBands""".stripMargin)

    Some((incompleteTiles, lower, upper, missingBands))
  }

  @Test
  def emptyCache(): Unit = {
    // 0) client wants SHub batch process API to give him data for: datasetId, geometry, dateRange, bands
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from
    val bandNames = Set("B04")

    val cache = new FixedCache(List())

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def wrongDateCached(): Unit = {
    // 0) client wants SHub batch process API to give him data for: datasetId, geometry, dateRange, bands
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from
    val bandNames = Set("B04")

    // 1 tile cached but for another date
    val cache = new FixedCache(List(
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 10).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def wrongPositionCached(): Unit = {
    // 0) client wants SHub batch process API to give him data for: datasetId, geometry, dateRange, bands
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from
    val bandNames = Set("B04")

    // 1 tile cached but not in the geometry
    val cache = new FixedCache(List(
      CacheEntry("31UFS_9_9", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def wrongBandCached(): Unit = {
    // 0) client wants SHub batch process API to give him data for: datasetId, geometry, dateRange, bands
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from
    val bandNames = Set("B04")

    // 1 tile cached but the wrong band
    val cache = new FixedCache(List(
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B03")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def oneTileCached(): Unit = {
    // 0) client wants SHub batch process API to give him data for: datasetId, geometry, dateRange, bands
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.3670654296875, 51.37863823622004, 5.134735107421875, 51.5189980614127).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from
    val bandNames = Set("B04")

    // 1 tile cached
    val cache = new FixedCache(List(
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def test1(): Unit = {
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.5049056649529309, 51.2778631700642364, 4.9462099707168559, 51.3966497916229912).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from plusDays 1
    val bandNames = Set("B04")

    val cache = new FixedCache(List(
      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),

      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def test2(): Unit = {
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(
        Extent(4.466034003575176, 51.27379477302177, 4.652641953920658, 51.41225921706625).toPolygon(),
        Extent(4.903579509277572, 51.265666231556011, 4.962355741802364, 51.409347158480443).toPolygon()
      )
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from plusDays 1
    val bandNames = Set("B04")

    val cache = new FixedCache(List(
      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),

      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def test3(): Unit = {
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.5049056649529309, 51.2778631700642364, 4.9462099707168559, 51.3966497916229912).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from plusDays 1
    val bandNames = Set("B04")

    val cache = new FixedCache(List(
      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),

      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def test4(): Unit = {
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.5049056649529309, 51.2778631700642364, 4.9462099707168559, 51.3966497916229912).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from plusDays 1
    val bandNames = Set("B0", "B1")

    val cache = new FixedCache(List(
      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B0"),

      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B0"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B0"),
      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B0"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B0"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B0"),

      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B0"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B1"),

      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B1")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def allCached(): Unit = {
    val datasetId = "S2L2A"
    val geometry: Geometry =
      MultiPolygon(Extent(4.5049056649529309, 51.2778631700642364, 4.9462099707168559, 51.3966497916229912).toPolygon())
    val from = LocalDate.of(2021, 8, 17).atStartOfDay(utc)
    val to = from plusDays 1
    val bandNames = Set("B04")

    val cache = new FixedCache(List(
      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 17).atStartOfDay(utc), "B04"),

      CacheEntry("31UFS_0_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_0", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_0_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_1_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_2_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04"),
      CacheEntry("31UFS_3_1", LocalDate.of(2021, 8, 18).atStartOfDay(utc), "B04")
    ))

    getNarrowerRequest(datasetId, geometry, from, to, bandNames, cache)
  }

  @Test
  def poc(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val collectionId = "sentinel-2-l2a"
    val datasetId = "S2L2A"
    val from = LocalDate.of(2019, 9, 21)
    val to = LocalDate.of(2019, 9, 26)
    val bbox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val bandNames = List("B04")

    println(ISO_OFFSET_DATE_TIME format from.atStartOfDay(utc))

    def getDataSync(date: LocalDate): Unit = {
      val pyramidFactory = new PyramidFactory(
        collectionId,
        datasetId,
        new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint),
        clientId,
        clientSecret
      )

      val isoTimestamp = ISO_OFFSET_DATE_TIME format date.atStartOfDay(utc)

      val pyramid = pyramidFactory.pyramid_seq(
        bbox.extent,
        bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}",
        from_date = isoTimestamp,
        to_date = isoTimestamp,
        band_names = bandNames.asJava,
        metadata_properties = Collections.emptyMap()
      )

      val (_, baseLayer) = pyramid
        .maxBy { case (zoom, _) => zoom }

      val spatialLayer = baseLayer
        .toSpatial()
        .crop(bbox.reproject(baseLayer.metadata.crs))

      val Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs)
      tif.write(s"/tmp/poc_sync_${BASIC_ISO_DATE format date}.tif")
    }

    def fetchReferenceGeotiffs() {
      val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

      try {
        getDataSync(from)
        getDataSync(to)
      } finally sc.stop()
    }

    /*fetchReferenceGeotiffs()
    return*/

    val geometry = bbox.extent.toPolygon()
    val cache: Cache = new ElasticsearchCache

    val narrowerRequest = getNarrowerRequest(
      datasetId,
      geometry,
      from.atStartOfDay(utc),
      to.atStartOfDay(utc),
      bandNames.toSet, // FIXME: should probably be a Seq to maintain band order
      cache
    )

    narrowerRequest foreach { case (incompleteTiles, lower, upper, missingBands) =>
      val batchProcessingService = new BatchProcessingService(
        endpoint,
        bucketName = "openeo-sentinelhub",
        clientId,
        clientSecret
      )

      val multiPolygons = incompleteTiles.toArray
        .map { case (_, geometry) =>
          val shrinkDistance = geometry.getEnvelopeInternal.getWidth * 0.05
          MultiPolygon(geometry.buffer(-shrinkDistance).asInstanceOf[Polygon])
        } // TODO: make it explicit that all grid tiles are MultiPolygons?

      val multiPolygonsCrs = LatLng

      val Some(batchRequestId) = batchProcessingService.start_batch_process(
        collectionId,
        datasetId,
        multiPolygons,
        multiPolygonsCrs,
        from_date = ISO_OFFSET_DATE_TIME format lower,
        to_date = ISO_OFFSET_DATE_TIME format upper,
        band_names = missingBands.toSeq.asJava,
        SampleType.FLOAT32, // FIXME: UINT16 gives bad (not scaled?) results?
        metadata_properties = Collections.emptyMap[String, Any],
        processing_options = Collections.emptyMap[String, Any]
      )

      awaitDone(batchProcessingService, Seq(batchRequestId))

      // val batchRequestId = "3db6d7ff-094f-4150-b418-8e8375cfa4da"

      def cacheTile(tileId: String, date: ZonedDateTime, bandName: String): Unit = {
        val location = getGeometry(tileId)
        val entry = CacheEntry(tileId, date, bandName, location)
        cache.add(datasetId, entry)
        println(s"cached $entry")
      }

      new S3Service().downloadBatchProcessResults(
        batchProcessingService.bucketName,
        subfolder = batchRequestId,
        targetDir = Paths.get("/tmp/cache"),
        bandNames,
        cacheTile
      )
    }
  }

  private def awaitDone(batchProcessingService: BatchProcessingService, batchRequestIds: Iterable[String]): Map[String, String] = {
    import java.util.concurrent.TimeUnit._

    while (true) {
      SECONDS.sleep(10)
      val statuses = batchRequestIds.map(id => id -> batchProcessingService.get_batch_process_status(id)).toMap
      println(s"[${LocalTime.now()}] intermediary statuses: $statuses")

      val uniqueStatuses = statuses.values.toSet

      if (uniqueStatuses == Set("DONE") || uniqueStatuses.contains("FAILED")) {
        return statuses
      }
    }

    throw new AssertionError
  }

  @Ignore
  @Test
  def es(): Unit = {
    val cache = new ElasticsearchCache

    val cacheEntries = cache.query(
      datasetId = "S2L2A",
      geometry = Extent(-176.71783447265625, -176.59149169921875, -82.89630761177784, -82.883729227997).toPolygon(),
      from = LocalDate.of(2019, 9, 20).atStartOfDay(utc),
      to = LocalDate.of(2019, 9, 22).atStartOfDay(utc),
      bandNames = Set("B04", "B05")
    )

    cacheEntries foreach println

    cache.add(
      datasetId = "S2L2A",
      CacheEntry("31UFS_1_1", LocalDate.of(1981, 4, 24).atStartOfDay(utc), bandName = "B04", location =
        Extent(-177.0, -82.94157475849404, -176.28014823592997, -82.85256027220672).toPolygon())
    )
  }
}
