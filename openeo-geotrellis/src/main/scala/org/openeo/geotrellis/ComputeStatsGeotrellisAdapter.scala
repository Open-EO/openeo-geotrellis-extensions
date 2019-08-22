package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import be.vito.eodata.extracttimeseries.geotrellis.{AccumuloLayerProvider, CancellationContext, ComputeStatsGeotrellis, ComputeStatsGeotrellisHelpers, LayerConfig, LayersConfig, MultibandLayerConfig, StatisticsCallback}
import be.vito.eodata.processing.MaskedStatisticsProcessor.StatsMeanResult
import geotrellis.proj4.CRS
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.{FloatConstantNoDataCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpaceTimeKey, TileLayerRDD}
import geotrellis.vector.io._
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.vector.{MultiPolygon, Polygon}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

object ComputeStatsGeotrellisAdapter {
  // OpenEO doesn't return physical values
  val noScaling = 1.0
  val noOffset = 0.0

  private object Sigma0Band extends Enumeration {
    val VH, VV, Angle = Value
  }

  private def singleBand(bandIndex: Int): MultibandTileLayerRDD[SpaceTimeKey] => TileLayerRDD[SpaceTimeKey] =
    multiBandRdd => {
      val valueRdd = multiBandRdd.mapValues(multiBandTile => multiBandTile.band(bandIndex))
      ContextRDD(valueRdd, multiBandRdd.metadata)
    }

  private def s1GrdSigma0LayerConfig(accumuloLayerId: String, band: Sigma0Band.Value)(implicit accumuloSupplier: () => AccumuloInstance): LayerConfig =
    LayerConfig(
      layerProvider = new AccumuloLayerProvider(accumuloLayerId),
      scalingFactor = noScaling,
      offset = noOffset,
      dataType = FloatConstantNoDataCellType,
      multiBandMath = singleBand(band.id)
    )
}

class ComputeStatsGeotrellisAdapter {
  import ComputeStatsGeotrellisAdapter._

  type JMap[K, V] = java.util.Map[K, V]
  type JList[T] = java.util.List[T]

  private val unusedCancellationContext = new CancellationContext(null, null)

  def compute_average_timeseries(product_id: String, polygon_wkts: JList[String], polygons_srs: String, from_date: String, to_date: String, zoom: Int, band_index: Int): JMap[String, JList[Double]] = {
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = parsePolygonWkts(polygon_wkts)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val statisticsCollector = new StatisticsCollector

    computeStatsGeotrellis.computeAverageTimeSeries(product_id, polygons, crs, startDate, endDate, zoom,
      statisticsCollector, unusedCancellationContext, sc)

    statisticsCollector.results
  }

  def compute_histogram_time_series(product_id: String, polygon_wkts: JList[String], polygons_srs: String,
                                    from_date: String, to_date: String, zoom: Int, band_index: Int):
  JMap[String, JList[JMap[Double, Long]]] = { // date -> polygon -> value/count
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = parsePolygonWkts(polygon_wkts)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val histogramsCollector = new HistogramsCollector

    computeStatsGeotrellis.computeHistogramTimeSeries(product_id, polygons, crs, startDate, endDate, zoom,
      histogramsCollector, unusedCancellationContext, sc)

    histogramsCollector.results
  }

  private def parsePolygonWkts(polygonWkts: JList[String]): Array[MultiPolygon] =
    polygonWkts.asScala
      .map(_.parseWKT().asInstanceOf[Polygon])
      .map(MultiPolygon(_))
      .toArray

  private def sc: SparkContext = SparkContext.getOrCreate()

  private def layersConfig(bandIndex: Int): LayersConfig = new LayersConfig {
    private implicit val accumuloSupplier: () => AccumuloInstance = ComputeStatsGeotrellisHelpers.accumuloSupplier.get

    override val layers: Map[String, LayerConfig] =
      Map(
        "BIOPAR_FAPAR_V1_GLOBAL" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("BIOPAR_FAPAR_V1_GLOBAL"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteUserDefinedNoDataCellType(255.asInstanceOf[Byte])
        ),
        "S2_FAPAR_V102_WEBMERCATOR2" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("S2_FAPAR_V102_WEBMERCATOR2"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteConstantNoDataCellType
        ),
        "S2_FCOVER_PYRAMID" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("S2_FCOVER_PYRAMID_EARLY"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteConstantNoDataCellType
        ),
        "PROBAV_L3_S10_TOC_NDVI_333M_V2" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("PROBAV_L3_S10_TOC_NDVI_333M_V2"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteUserDefinedNoDataCellType(255.asInstanceOf[Byte])
        ),
        "S2_NDVI_PYRAMID" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("S2_NDVI_PYRAMID_EARLY"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteConstantNoDataCellType
        ),
        "S2_LAI_PYRAMID" -> LayerConfig(
          layerProvider = new AccumuloLayerProvider("S2_LAI_PYRAMID_20190625"),
          scalingFactor = noScaling,
          offset = noOffset,
          dataType = UByteConstantNoDataCellType
        ),
        "S1_GRD_SIGMA0_ASCENDING" -> s1GrdSigma0LayerConfig("S1_GRD_SIGMA0_ASCENDING_PYRAMID", Sigma0Band(bandIndex)),
        "S1_GRD_SIGMA0_DESCENDING" -> s1GrdSigma0LayerConfig("S1_GRD_SIGMA0_DESCENDING_PYRAMID", Sigma0Band(bandIndex))
      )

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

  private class HistogramsCollector extends StatisticsCallback[Histogram[Double]] {
    import java.util._

    val results: JMap[String, JList[JMap[Double, Long]]] =
      Collections.synchronizedMap(new HashMap[String, JList[JMap[Double, Long]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Histogram[Double]]): Unit = {
      val timestamp = date format DateTimeFormatter.ISO_DATE_TIME

      val polygonalHistograms: Seq[JMap[Double, Long]] = results.map { histogram =>
        val buckets: JMap[Double, Long] = new HashMap[Double, Long]
        histogram.foreach { case (value, count) => buckets.put(value, count) }

        buckets
      }

      this.results.put(timestamp, polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }
}
