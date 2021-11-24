package org.openeo.geotrellissentinelhub

import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}
import org.openeo.geotrellissentinelhub.ElasticsearchCacheRepository.{Sentinel1GrdCacheEntry, Sentinel2L2aCacheEntry}

import java.nio.file.Paths
import java.time.{LocalDate, ZoneOffset}

class ElasticsearchCacheRepositoryTest {
  private val cacheRepository = new ElasticsearchCacheRepository("http://localhost:9200")
  private val s1GrdCacheIndex = "sentinel-hub-s1grd-cache"
  private val utc = ZoneOffset.UTC

  private val s1CacheEntry = Sentinel1GrdCacheEntry(
    tileId = "31UDS_7_2",
    date = LocalDate.of(2019, 9, 23).atStartOfDay(utc),
    bandName = "VV",
    backCoeff = "GAMMA0_TERRAIN",
    orthorectify = true,
    demInstance = "MAPZEN",
    geometry = GeoJson.parse[MultiPolygon](
      """{
        |  "type":"MultiPolygon",
        |  "coordinates":[
        |    [
        |      [
        |        [
        |          2.570794875637462,
        |          51.180634616694185
        |        ],
        |        [
        |          2.569957714730166,
        |          51.270553600044025
        |        ],
        |        [
        |          2.7133027360362014,
        |          51.27099235598838
        |        ],
        |        [
        |          2.7138608596046008,
        |          51.181071973576714
        |        ],
        |        [
        |          2.570794875637462,
        |          51.180634616694185
        |        ]
        |      ]
        |    ]
        |  ]
        |}""".stripMargin),
    empty = false
  )

  private val s2CacheEntry = Sentinel2L2aCacheEntry(
    tileId = "31UDS_7_2",
    date = LocalDate.of(2019, 9, 21).atStartOfDay(utc),
    bandName = "B04",
    geometry = GeoJson.parse[MultiPolygon](
      """{
        |  "type":"MultiPolygon",
        |  "coordinates":[
        |    [
        |      [
        |        [
        |          2.570794875637462,
        |          51.180634616694185
        |        ],
        |        [
        |          2.569957714730166,
        |          51.270553600044025
        |        ],
        |        [
        |          2.7133027360362014,
        |          51.27099235598838
        |        ],
        |        [
        |          2.7138608596046008,
        |          51.181071973576714
        |        ],
        |        [
        |          2.570794875637462,
        |          51.180634616694185
        |        ]
        |      ]
        |    ]
        |  ]
        |}""".stripMargin),
    empty = false
  )

  @Test
  def testSentinel1FilePath(): Unit = {
    val expectedFilePath = Paths.get("/data/projects/OpenEO/sentinel-hub-s1grd-cache/GAMMA0_TERRAIN/orthorectified/MAPZEN/20190923/31UDS_7_2/VV.tif")
    assertEquals(Some(expectedFilePath), s1CacheEntry.filePath)
  }

  @Ignore
  @Test
  def saveSentinel1(): Unit =
    cacheRepository.saveSentinel1(s1GrdCacheIndex, s1CacheEntry)

  @Ignore
  @Test
  def querySentinel1(): Unit = {
    val date = LocalDate.of(2019, 9, 23).atStartOfDay(utc)

    val cacheEntries = cacheRepository.querySentinel1(s1GrdCacheIndex,
      geometry = Extent(2.6264190673828125, 51.22365776470275, 2.65045166015625, 51.23741678601641).toPolygon(),
      from = date,
      to = date,
      bandNames = Seq("VV"),
      backCoeff = "GAMMA0_TERRAIN",
      orthorectify = true,
      demInstance = "MAPZEN"
    )

    val someCacheEntry = cacheEntries.head

    println(someCacheEntry)
    println(cacheEntries.head.filePath)
  }

  @Test
  def testSentinel2FilePath(): Unit = {
    val expectedFilePath = Paths.get("/data/projects/OpenEO/sentinel-hub-s2l2a-cache/20190921/31UDS_7_2/B04.tif")
    assertEquals(Some(expectedFilePath), s2CacheEntry.filePath)
  }
}
