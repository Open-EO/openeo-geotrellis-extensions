package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import be.vito.eodata.extracttimeseries.geotrellis.{AccumuloLayerProvider, AccumuloSpaceTimeMask, AccumuloSpatialMask, CancellationContext, ComputeStatsGeotrellis, ComputeStatsGeotrellisHelpers, LayerConfig, LayersConfig, MultibandLayerConfig, StatisticsCallback}
import be.vito.eodata.processing.MaskedStatisticsProcessor.StatsMeanResult
import geotrellis.proj4.CRS
import geotrellis.raster.UByteUserDefinedNoDataCellType
import geotrellis.vector.io._
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.vector.{MultiPolygon, Polygon}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class ComputeStatsGeotrellisAdapter {
  type JMap[K, V] = java.util.Map[K, V]
  type JList[T] = java.util.List[T]

  def compute_average_timeseries(product_id: String, polygon_wkts: JList[String], polygons_srs: String, from_date: String, to_date: String, zoom: Int): JMap[String, JList[Double]] = {
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig)

    val polygons = polygon_wkts.asScala
      .map(_.parseWKT().asInstanceOf[Polygon])
      .map(MultiPolygon(_))
      .toArray

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val statisticsCollector = new StatisticsCollector
    val cancellationContext: CancellationContext = new CancellationContext("id", "description")
    val sc = SparkContext.getOrCreate()

    computeStatsGeotrellis.computeAverageTimeSeries(product_id, polygons, crs, startDate, endDate, zoom,
      statisticsCollector, cancellationContext, sc)

    statisticsCollector.results
  }

  private lazy val layersConfig: LayersConfig = new LayersConfig {
    override val layers: Map[String, LayerConfig] = {
      implicit val accumuloSupplier: () => AccumuloInstance = ComputeStatsGeotrellisHelpers.accumuloSupplier.get

      val probav = LayerConfig(
        layerProvider = new AccumuloLayerProvider("PROBAV_L3_S10_TOC_NDVI_333M_V2"),
        scalingFactor = 1.0 / 250,
        offset = -20.0 / 250,
        dataType = UByteUserDefinedNoDataCellType(255.asInstanceOf[Byte]),
        masks = List(
          new AccumuloSpaceTimeMask(
            layerName = "PROBAV_L3_S10_TOC_SM_333M_V2",
            validFlags = Some(List(
              Integer.parseInt("11100000", 2),
              Integer.parseInt("11110000", 2),
              Integer.parseInt("11101000", 2),
              Integer.parseInt("11111000", 2)
            ))),
          new AccumuloSpatialMask(
            layerName = "CCI_LANDCOVER",
            invalidFlags = Some(List(210)) // Water bodies
          )
        )
      )

      Map("PROBAV_L3_S10_TOC_NDVI_333M" -> probav)
    }

    override val multibandLayers: Map[String, MultibandLayerConfig] = Map()
  }

  private class StatisticsCollector extends StatisticsCallback[StatsMeanResult] {
    import java.util._

    val results: JMap[String, JList[Double]] =
      Collections.synchronizedMap(new HashMap[String, JList[Double]])

    override def onComputed(date: ZonedDateTime, results: Seq[StatsMeanResult]): Unit = {
      val timestamp = date format DateTimeFormatter.ISO_DATE_TIME
      val means = results.map(_.getAverage)

      this.results.put(timestamp, means.asJava)
    }

    override def onCompleted(): Unit = ()
  }
}
