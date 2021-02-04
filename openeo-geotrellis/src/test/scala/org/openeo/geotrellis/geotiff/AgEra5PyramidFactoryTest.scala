package org.openeo.geotrellis.geotiff

import java.time.LocalTime.MIDNIGHT
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import cats.data.NonEmptyList
import geotrellis.layer.{TemporalKeyExtractor, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.RasterSource
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector.{Extent, MultiPolygon, Polygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.file.AgEra5PyramidFactory
import org.openeo.geotrellis.layers.BandCompositeRasterSource
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object AgEra5PyramidFactoryTest extends LocalSparkContext {
  private def deriveDate(filename: String, date: Regex): ZonedDateTime = {
    filename match {
      case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), MIDNIGHT, ZoneId.of("UTC"))
    }
  }
}

class AgEra5PyramidFactoryTest {
  import AgEra5PyramidFactoryTest._

  private implicit val sc: SparkContext = AgEra5PyramidFactoryTest.sc

  private def getSiblings(dewPointTemperatureFile: String): Seq[String] = {
    val dewPointTemperatureMarker = "dewpoint-temperature"

    val remainingMarkers = Seq(
      "precipitation-flux",
      "solar-radiation-flux",
      "temperature-max"/*,
      "temperature-mean",
      "temperature-min",
      "vapour-pressure",
      "wind-speed"*/
    )

    remainingMarkers.map(remainingMarker => dewPointTemperatureFile.replace(dewPointTemperatureMarker, remainingMarker))
  }

  @Ignore("trying things out")
  @Test
  def agEra5(): Unit = {
    // note: reprojecting to e.g. WebMercator fails its extent is beyond LatLng's worldExtent
    val someDewPointTemperatureFile = "/data/MTDA/AgERA5/2020/20200424/AgERA5_dewpoint-temperature_20200424.tif"
    val date = raw".+_(\d{4})(\d{2})(\d{2})\.tif".r

    val bandRasterSources: Seq[RasterSource] =
      (someDewPointTemperatureFile +: getSiblings(someDewPointTemperatureFile))
        .map(GeoTiffRasterSource(_))

    val rasterSource = new BandCompositeRasterSource(NonEmptyList.of(bandRasterSources.head, bandRasterSources.tail: _*), LatLng)

    val rasterSources: RDD[RasterSource] = sc.parallelize(Seq(rasterSource))
    val layout = ZoomedLayoutScheme(rasterSource.crs).levelFor(rasterSource.extent, rasterSource.cellSize).layout
    val keyExtractor = TemporalKeyExtractor.fromPath { case GeoTiffPath(value) => deriveDate(value, date) }

    val layer = RasterSourceRDD.tiledLayerRDD(rasterSources, layout, keyExtractor)

    val projectedExtent = ProjectedExtent(Extent(0.0, 50.0, 5.0, 55.0), LatLng)

    val spatialLayer = layer
      .toSpatial()
      .withContext(_.filter { case (spatialKey, _) =>
        val keyExtent = spatialKey.extent(layout) // TODO: act upon RasterRegions instead (= before reading the tile)
        keyExtent intersects projectedExtent.extent
      })
      .cache()

    spatialLayer.writeGeoTiff(s"/tmp/agEra5_3_bands.tif", projectedExtent)
  }

  @Test
  def sparsePolygons(): Unit = {
    val pyramidFactory = new AgEra5PyramidFactory(
      dataGlob = "/data/MTDA/AgERA5/2020/202004*/AgERA5_dewpoint-temperature_*.tif",
      bandFileMarkers = Seq("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux").asJava,
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})\.tif"
    )

    val from = LocalDate.of(2020, 4, 1).atStartOfDay(ZoneId.of("UTC"))
    val to = from plusWeeks 1

    val bbox1 = Extent(-9.0, 51.0, -1.0, 59.0)
    val bbox2 = Extent(1.0, 41.0, 9.0, 49.0)
    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1, bbox2)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(projectedPolygons,
      from_date = ISO_OFFSET_DATE_TIME format from, to_date = ISO_OFFSET_DATE_TIME format to)

    baseLayer.cache()

    assert(baseLayer.metadata.crs == bboxCrs)

    def physicalMean(polygon: Polygon, at: ZonedDateTime, bandIndex: Int): Double = {
      val spatialLayer = baseLayer
        .toSpatial(at)
        .cache()

      val Summary(bandMeans) = spatialLayer
        .polygonalSummaryValue(polygon, MeanVisitor)

      val scalingFactor = 0.01
      val offset = 0

      bandMeans(bandIndex).mean * scalingFactor + offset
    }

    assertEquals(275.9338171360439, physicalMean(bbox1.extent.toPolygon(), from, bandIndex = 0), 0.01)
    assertEquals(0.16656857030940905, physicalMean(bbox2.extent.toPolygon(), to, bandIndex = 1), 0.01)
  }
}
