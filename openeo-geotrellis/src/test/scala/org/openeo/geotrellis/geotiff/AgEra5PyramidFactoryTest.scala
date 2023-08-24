package org.openeo.geotrellis.geotiff

import cats.data.NonEmptyList
import geotrellis.layer.{SpaceTimeKey, TemporalKeyExtractor, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector.{Extent, MultiPolygon, Polygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.layers.BandCompositeRasterSource
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.time.LocalTime.MIDNIGHT
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
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

  def physicalMean(baseLayer: MultibandTileLayerRDD[SpaceTimeKey] ,polygon: Polygon, at: ZonedDateTime, bandIndex: Int): Double = {
    val spatialLayer = baseLayer
      .toSpatial(at)
      .cache()

    val Summary(bandMeans) = spatialLayer
      .polygonalSummaryValue(polygon, MeanVisitor)

    val scalingFactor = 0.01
    val offset = 0

    bandMeans(bandIndex).mean * scalingFactor + offset
  }

  @Test
  def sparsePolygons(): Unit = {
    val dataGlob = "/data/MEP/ECMWF/AgERA5/2020/202004*/AgERA5_dewpoint-temperature_*.tif"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})\.tif"
    val bandFileMarkers = Seq("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux").asJava
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, bandFileMarkers, "agera5")
    val pyramidFactory = new org.openeo.geotrellis.file.PyramidFactory(
      openSearchClient, "", bandFileMarkers, "",
      maxSpatialResolution = CellSize(0.1,0.1)
    )

    val from = LocalDate.of(2020, 4, 1).atStartOfDay(ZoneId.of("UTC"))
    val to = from plusWeeks 1 plusDays(1)

    val bbox1 = Extent(-9.0, 51.0, -1.0, 59.0)
    val bbox2 = Extent(1.0, 41.0, 9.0, 49.0)
    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1, bbox2)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val params = new DataCubeParameters()

    val Seq((_, baseLayer: MultibandTileLayerRDD[SpaceTimeKey])) = pyramidFactory.datacube_seq(projectedPolygons,
      from_date = ISO_OFFSET_DATE_TIME format from, to_date = ISO_OFFSET_DATE_TIME format to, new util.HashMap(), "", params)

    baseLayer.cache()

    assert(baseLayer.metadata.crs == bboxCrs)

    assertEquals(275.93, physicalMean(baseLayer,bbox1.extent.toPolygon(), from, bandIndex = 0), 0.03)
    assertEquals(0.19, physicalMean(baseLayer,bbox2.extent.toPolygon(), to, bandIndex = 1), 0.03)
  }

  @Test
  def smallPolygon(): Unit = {
    val dataGlob = "/data/MEP/ECMWF/AgERA5/2020/202004*/AgERA5_dewpoint-temperature_*.tif"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})\.tif"
    val bandFileMarkers = Seq("dewpoint-temperature", "precipitation-flux", "solar-radiation-flux").asJava
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, bandFileMarkers, "agera5")
    val pyramidFactory = new org.openeo.geotrellis.file.PyramidFactory(
      openSearchClient, "", bandFileMarkers, "",
      maxSpatialResolution = CellSize(10,10)
    )

    val from = LocalDate.of(2020, 4, 1).atStartOfDay(ZoneId.of("UTC"))
    val to = from plusWeeks 1

    val bbox1 = Extent(5.15183, 51.18192, 5.153381, 51.184696)

    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val params = new DataCubeParameters()
    params.layoutScheme = "FloatingLayoutScheme"
    params.setGlobalExtent(bbox1.xmin,bbox1.ymin,bbox1.xmax,bbox1.ymax,"EPSG:4326")


    val Seq((_, baseLayer)) = pyramidFactory.datacube_seq(ProjectedPolygons.reproject(projectedPolygons,32631),
      from_date = ISO_OFFSET_DATE_TIME format from, to_date = ISO_OFFSET_DATE_TIME format to, new util.HashMap(), "", params)

    baseLayer.cache()
    baseLayer.toSpatial(from).writeGeoTiff(s"/tmp/agEra5_3_bands.tif", ProjectedExtent(baseLayer.metadata.extent,baseLayer.metadata.crs))
    assert(baseLayer.metadata.crs.epsgCode.get == 32631)

    assertEquals(270.95, physicalMean(baseLayer, bbox1.extent.reproject(LatLng,baseLayer.metadata.crs).toPolygon(), from, bandIndex = 0), 0.03)

  }
}
