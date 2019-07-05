package org.openeo.geotrellis

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import org.junit.{Before, Test}
import org.junit.Assert._
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Polygon
import geotrellis.vector.io._

import scala.collection.JavaConverters._

class ComputeStatsGeotrellisAdapterTest {

  @Before
  def assertKerberosAuthentication(): Unit = {
    assertNotNull(getClass.getResourceAsStream("/core-site.xml"))
  }

  @Test
  def compute_average_timeseries(): Unit = {
    val computeStatsGeotrellisAdapter = new ComputeStatsGeotrellisAdapter

    val from = ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from plusWeeks 1

    val polygon1 =
      """
        |{
        |  "type": "Polygon",
        |  "coordinates": [
        |    [
        |      [
        |        4.6770629938691854,
        |        50.82172692290532
        |      ],
        |      [
        |        4.6550903376191854,
        |        50.80697613242405
        |      ],
        |      [
        |        4.6866760309785604,
        |        50.797429020705295
        |      ],
        |      [
        |        4.7196350153535604,
        |        50.795692972629176
        |      ],
        |      [
        |        4.7402343805879354,
        |        50.81738893871384
        |      ],
        |      [
        |        4.6770629938691854,
        |        50.82172692290532
        |      ]
        |    ]
        |  ]
        |}
      """.stripMargin.parseGeoJson[Polygon]()

    val polygon2 =
      """
        |{
        |  "type": "Polygon",
        |  "coordinates": [
        |    [
        |      [
        |        3.950237888725339,
        |        51.01001898590911
        |      ],
        |      [
        |        3.950237888725339,
        |        51.03442207171108
        |      ],
        |      [
        |        4.032635349662839,
        |        51.03442207171108
        |      ],
        |      [
        |        4.032635349662839,
        |        51.01001898590911
        |      ],
        |      [
        |        3.950237888725339,
        |        51.01001898590911
        |      ]
        |    ]
        |  ]
        |}
      """.stripMargin.parseGeoJson[Polygon]()

    val polygons = Seq(polygon1.toWKT(), polygon2.toWKT())

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName)

    try {
      val stats = computeStatsGeotrellisAdapter.compute_average_timeseries(
        "S1_GRD_SIGMA0_ASCENDING",
        polygons.asJava,
        polygons_srs = "EPSG:4326",
        from_date = ISO_OFFSET_DATE_TIME format from,
        to_date = ISO_OFFSET_DATE_TIME format to,
        zoom = 14,
        band_index = 2
      ).asScala

      for ((date, means) <- stats) {
        println(s"$date: $means")
      }

      assertFalse(stats.isEmpty)

      val means = stats
        .flatMap { case (_, dailyMeans) => dailyMeans.asScala }

      assertTrue(means.exists(mean => !mean.isNaN))
    } finally {
      sc.stop()
    }
  }
}
