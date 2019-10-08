package org.openeo.geotrellis

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util

import be.vito.eodata.extracttimeseries.geotrellis._
import be.vito.eodata.geopysparkextensions.KerberizedAccumuloInstance
import be.vito.eodata.processing.MaskedStatisticsProcessor.StatsMeanResult
import geotrellis.proj4.CRS
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.summary.Statistics
import geotrellis.raster.{FloatConstantNoDataCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType}
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, SpaceTimeKey, TemporalKey, TileLayerRDD}
import geotrellis.vector.io._
import geotrellis.vector.{Geometry, MultiPolygon, Polygon}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object ComputeStatsGeotrellisAdapter {
  private type JMap[K, V] = java.util.Map[K, V]
  private type JList[T] = java.util.List[T]

  // OpenEO doesn't return physical values
  private val noScaling = 1.0
  private val noOffset = 0.0

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

  private def toMap(histogram: Histogram[Double]): JMap[Double, Long] = {
    val buckets: JMap[Double, Long] = new util.HashMap[Double, Long]
    histogram.foreach { case (value, count) => buckets.put(value, count) }

    buckets
  }

  private def isoFormat(timestamp: ZonedDateTime): String = timestamp format DateTimeFormatter.ISO_DATE_TIME
}

class ComputeStatsGeotrellisAdapter(zookeepers: String, accumuloInstanceName: String) {
  import ComputeStatsGeotrellisAdapter._

  private val unusedCancellationContext = new CancellationContext(null, null)

  def compute_average_timeseries(product_id: String, polygon_wkts: JList[String], polygons_srs: String, from_date: String, to_date: String, zoom: Int, band_index: Int): JMap[String, JList[Double]] = {
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = polygon_wkts.asScala.map(parsePolygonWkt)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val statisticsCollector = new StatisticsCollector

    computeStatsGeotrellis.computeAverageTimeSeries(product_id, polygons.toArray, crs, startDate, endDate, zoom,
      statisticsCollector, unusedCancellationContext, sc)

    statisticsCollector.results
  }

  def compute_average_timeseries_from_datacube(datacube:MultibandTileLayerRDD[SpaceTimeKey], polygon_wkts: JList[String], polygons_srs: String, from_date: String, to_date: String, band_index: Int): JMap[String, JList[JList[Double]]] = {
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = polygon_wkts.asScala.map(parsePolygonWkt)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val statisticsCollector = new MultibandStatisticsCollector

    computeStatsGeotrellis.computeAverageTimeSeries(datacube, polygons.toArray, crs, startDate, endDate, statisticsCollector, unusedCancellationContext, sc)

    statisticsCollector.results
  }

  def compute_histogram_time_series(product_id: String, polygon_wkt: String, polygon_srs: String,
                                    from_date: String, to_date: String, zoom: Int, band_index: Int):
  JMap[String, JMap[Double, Long]] = { // date -> value/count
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygon = parsePolygonWkt(polygon_wkt)
    val crs: CRS = CRS.fromName(polygon_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)

    val histograms = computeStatsGeotrellis.computeHistogramTimeSeries(product_id, polygon, crs, startDate, endDate, zoom, sc)

    histograms
      .map { case (temporalKey, histogram) => (isoFormat(temporalKey.time), toMap(histogram)) }
      .collectAsMap()
      .asJava
  }

  def compute_histogram_time_series_from_datacube(datacube:MultibandTileLayerRDD[SpaceTimeKey], polygon_wkt: String, polygon_srs: String,
                                    from_date: String, to_date: String, band_index: Int):
  JMap[String, JList[JMap[Double, Long]]] = { // date -> value/count
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygon = parsePolygonWkt(polygon_wkt)
    val crs: CRS = CRS.fromName(polygon_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)

    val histograms: Array[RDD[(TemporalKey, Array[Histogram[Double]])]] = computeStatsGeotrellis.computeHistogramTimeSeries(datacube, Array(polygon), crs, startDate, endDate,  sc)

    histograms(0)
      .map { case (temporalKey, histogram) => (isoFormat(temporalKey.time), histogram.map(toMap(_)).toSeq.asJava) }
      .collectAsMap()
      .asJava
  }

  def compute_histograms_time_series_from_datacube(datacube:MultibandTileLayerRDD[SpaceTimeKey], polygon_wkts: JList[String], polygons_srs: String,
                                     from_date: String, to_date: String, band_index: Int):

  JMap[String, JList[JList[JMap[Double, Long]]]] = { // date -> polygon -> value/count
    val histogramsCollector = new MultibandHistogramsCollector
    _compute_histograms_time_series_from_datacube(datacube, polygon_wkts, polygons_srs, from_date, to_date, band_index, histogramsCollector)
    histogramsCollector.results
  }

  def compute_median_time_series_from_datacube(datacube:MultibandTileLayerRDD[SpaceTimeKey], polygon_wkts: JList[String], polygons_srs: String,
                                                   from_date: String, to_date: String, band_index: Int):
  JMap[String, JList[JList[Double]]] = { // date -> polygon -> value/count

    val histogramsCollector = new MultibandMediansCollector
    _compute_histograms_time_series_from_datacube(datacube, polygon_wkts, polygons_srs, from_date, to_date, band_index, histogramsCollector)
    histogramsCollector.results

  }

  def compute_sd_time_series_from_datacube(datacube:MultibandTileLayerRDD[SpaceTimeKey], polygon_wkts: JList[String], polygons_srs: String,
                                               from_date: String, to_date: String, band_index: Int):
  JMap[String, JList[JList[Double]]] = { // date -> polygon -> value/count

    val histogramsCollector = new MultibandStdDevCollector
    _compute_histograms_time_series_from_datacube(datacube, polygon_wkts, polygons_srs, from_date, to_date, band_index, histogramsCollector)
    histogramsCollector.results

  }

  private def _compute_histograms_time_series_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygon_wkts: JList[String], polygons_srs: String, from_date: String, to_date: String, band_index: Int, histogramsCollector: StatisticsCallback[_ >: Seq[Histogram[Double]]]) = {
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = polygon_wkts.asScala.map(parsePolygonWkt)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)

    computeStatsGeotrellis.computeHistogramTimeSeries(datacube, polygons.toArray, crs, startDate, endDate, histogramsCollector, unusedCancellationContext, sc)
  }

  def compute_histograms_time_series(product_id: String, polygon_wkts: JList[String], polygons_srs: String,
                                     from_date: String, to_date: String, zoom: Int, band_index: Int):
  JMap[String, JList[JMap[Double, Long]]] = { // date -> polygon -> value/count
    val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig(band_index))

    val polygons = polygon_wkts.asScala.map(parsePolygonWkt)

    val crs: CRS = CRS.fromName(polygons_srs)
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val histogramsCollector = new HistogramsCollector

    computeStatsGeotrellis.computeHistogramTimeSeries(product_id, polygons.toArray, crs, startDate, endDate, zoom,
      histogramsCollector, unusedCancellationContext, sc)

    histogramsCollector.results
  }

  private def parsePolygonWkt(polygonWkt: String): MultiPolygon = {
    val geometry: Geometry = polygonWkt.parseWKT()
    geometry match {
      case polygon: MultiPolygon =>
        polygon
      case _ =>
        MultiPolygon(geometry.asInstanceOf[Polygon])
    }
  }

  private def sc: SparkContext = SparkContext.getOrCreate()

  private def layersConfig(bandIndex: Int): LayersConfig = new LayersConfig {
    private implicit val accumuloSupplier: () => AccumuloInstance =
      () => KerberizedAccumuloInstance(zookeepers, accumuloInstanceName)

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
      Collections.synchronizedMap(new util.HashMap[String, JList[Double]])

    override def onComputed(date: ZonedDateTime, results: Seq[StatsMeanResult]): Unit = {
      val means = results.map(_.getAverage)

      this.results.put(isoFormat(date), means.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandStatisticsCollector extends StatisticsCallback[Seq[StatsMeanResult]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[StatsMeanResult]]): Unit = {
      val means = results.map(_.map(_.getAverage))

      this.results.put(isoFormat(date), means.map(_.asJava).asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class HistogramsCollector extends StatisticsCallback[Histogram[Double]] {
    import java.util._

    val results: JMap[String, JList[JMap[Double, Long]]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[JMap[Double, Long]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Histogram[Double]]): Unit = {
      val polygonalHistograms = results map toMap

      this.results.put(isoFormat(date), polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandHistogramsCollector extends StatisticsCallback[Seq[Histogram[Double]]] {
    import java.util._

    val results: JMap[String, JList[JList[JMap[Double, Long]]]] =
      Collections.synchronizedMap(new HashMap[String, JList[JList[JMap[Double, Long]]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[Histogram[Double]]]): Unit = {
      val polygonalHistograms: Seq[JList[JMap[Double, Long]]] = results.map( _.map(toMap).asJava)
      this.results.put(isoFormat(date), polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandMediansCollector extends StatisticsCallback[Seq[Histogram[Double]]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[Histogram[Double]]]): Unit = {
      val polygonalHistograms: Seq[JList[Double]] = results.map( _.map(_.median().getOrElse(Double.NaN)).asJava)
      this.results.put(isoFormat(date), polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandStdDevCollector extends StatisticsCallback[Seq[Histogram[Double]]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[Histogram[Double]]]): Unit = {
      val polygonalHistograms: Seq[JList[Double]] = results.map( _.map(_.statistics().getOrElse(Statistics.EMPTYDouble()).stddev).asJava)
      this.results.put(isoFormat(date), polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }
}
