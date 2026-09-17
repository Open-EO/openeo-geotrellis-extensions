package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.{Tile, isData, isNoData}
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.summary.Statistics
import geotrellis.spark._
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.openeo.geotrellis.aggregate_polygon.intern._
import org.openeo.geotrellis.aggregate_polygon.{AggregatePolygonProcess, SparkAggregateScriptBuilder, intern}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import scala.jdk.CollectionConverters._


object ComputeStatsGeotrellisAdapter {
  private type JMap[K, V] = java.util.Map[K, V]
  private type JList[T] = java.util.List[T]

  private type MultibandMeans = Seq[MeanResult]

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[ComputeStatsGeotrellisAdapter])

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


  private def toMap(histogram: Histogram[Double]): JMap[Double, Long] = {
    val buckets: JMap[Double, Long] = new util.HashMap[Double, Long]
    histogram.foreach { case (value, count) => buckets.put(value, count) }

    buckets
  }

  private def isoFormat(timestamp: ZonedDateTime): String = timestamp format DateTimeFormatter.ISO_DATE_TIME
}

class ComputeStatsGeotrellisAdapter(zookeepers: String, accumuloInstanceName: String) {
  import ComputeStatsGeotrellisAdapter._

  def this() {
    this("","")
  }

  private val unusedCancellationContext = new CancellationContext(null, null)


  /**
   *
   * @deprecated Got replaced by more generic approaches, is not called from python
   */
  def compute_average_timeseries_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons, from_date: String, to_date: String, band_index: Int): JMap[String, JList[JList[Double]]] = {
    val computeStatsGeotrellis = new AggregatePolygonProcess()

    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    val statisticsCollector = new MultibandStatisticsCollector

    computeStatsGeotrellis.computeAverageTimeSeries(datacube.persist(MEMORY_AND_DISK_SER), polygons.polygons, polygons.crs, startDate, endDate, statisticsCollector, unusedCancellationContext, sc)

    statisticsCollector.results
  }


  /**
   * Writes means to an UTF-8 encoded JSON file.
   */
  def compute_generic_timeseries_from_datacube(reducer:String, datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons, output_file: String): Unit = {
    val builder = new SparkAggregateScriptBuilder
    builder.expressionEnd(reducer,new util.HashMap[String,Object]())
    this.compute_generic_timeseries_from_datacube(builder,datacube, polygons, output_file)
  }

  /**
   * Writes means to an UTF-8 encoded JSON file.
   */
  def compute_generic_timeseries_from_datacube(scriptBuilder:SparkAggregateScriptBuilder, datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons, output_file: String): Unit = {
    val computeStatsGeotrellis = new AggregatePolygonProcess()

    if(polygons.polygons.isEmpty) {
      return //not a lot we can compute here
    }
    val splitPolygons = splitOverlappingPolygons(polygons.polygons)

    if(splitPolygons._1.isEmpty) {
      return //happens when all polygons are empty
    }

    val bandCount = new OpenEOProcesses().RDDBandCount(datacube)
    computeStatsGeotrellis.aggregateSpatialGeneric(scriptBuilder, datacube.persist(MEMORY_AND_DISK_SER),splitPolygons, polygons.crs, bandCount,output_file)

  }

  def compute_generic_timeseries_from_datacube(reducer: String, datacube: MultibandTileLayerRDD[SpaceTimeKey],
                                               geometry_wkts: JList[String], geometries_srs: String,
                                               output_dir: String): Unit = {
    val builder = new SparkAggregateScriptBuilder
    builder.expressionEnd(reducer, new util.HashMap[String, Object])
    compute_generic_timeseries_from_datacube(builder, datacube, geometry_wkts, geometries_srs, output_dir)
  }

  def compute_generic_timeseries_from_datacube(scriptBuilder: SparkAggregateScriptBuilder,
                                               datacube: MultibandTileLayerRDD[SpaceTimeKey],
                                               geometry_wkts: JList[String], geometries_srs: String,
                                               output_dir: String): Unit = {
    val geometries = geometry_wkts.asScala.map(_.parseWKT()).toSeq
    val geometriesCrs = CRS.fromName(geometries_srs)

    new AggregatePolygonProcess().aggregateSpatialForGeometry(scriptBuilder, datacube, geometries, geometriesCrs,
      bandCount = new OpenEOProcesses().RDDBandCount(datacube), output_dir)
  }

  def compute_generic_timeseries_from_spatial_datacube(reducer: String,
                                                       datacube: MultibandTileLayerRDD[SpatialKey],
                                                       geometry_wkts: JList[String], geometries_srs: String,
                                                       output_dir: String): Unit = {
    val builder = new SparkAggregateScriptBuilder
    builder.expressionEnd(reducer, new util.HashMap[String, Object])
    compute_generic_timeseries_from_spatial_datacube(builder, datacube, geometry_wkts, geometries_srs, output_dir)
  }

  def compute_generic_timeseries_from_spatial_datacube(scriptBuilder: SparkAggregateScriptBuilder,
                                                       datacube: MultibandTileLayerRDD[SpatialKey],
                                                       geometry_wkts: JList[String], geometries_srs: String,
                                                       output_dir: String): Unit = {
    val geometries = geometry_wkts.asScala.map(_.parseWKT()).toSeq
    val geometriesCrs = CRS.fromName(geometries_srs)

    new AggregatePolygonProcess().aggregateSpatialForGeometryWithSpatialCube(scriptBuilder, datacube, geometries,
      geometriesCrs, bandCount = new OpenEOProcesses().RDDBandCount(datacube), output_dir)
  }

  /**
   * @deprecated histograms are not supported in openEO
   */
  def compute_histograms_time_series_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons,
                                                   from_date: String, to_date: String, band_index: Int
                                                  ): JMap[String, JList[JList[JMap[Double, Long]]]] = { // date -> polygon -> band -> value/count
    val histogramsCollector = new MultibandHistogramsCollector
    _compute_histograms_time_series_from_datacube(datacube, polygons, from_date, to_date, band_index, histogramsCollector)
    histogramsCollector.results
  }

  /**
   * @deprecated
   */
  def compute_median_time_series_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons,
                                               from_date: String, to_date: String, band_index: Int
                                              ): JMap[String, JList[JList[Double]]] = {
    val mediansCollector = new MultibandMediansCollector
    _compute_histograms_time_series_from_datacube(datacube, polygons, from_date, to_date, band_index, mediansCollector)
    mediansCollector.results
  }

  /**
   * @deprecated
   */
  def compute_sd_time_series_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons,
                                           from_date: String, to_date: String, band_index: Int
                                          ): JMap[String, JList[JList[Double]]] = { // date -> polygon -> value/count
    val stdDevCollector = new MultibandStdDevCollector
    _compute_histograms_time_series_from_datacube(datacube, polygons, from_date, to_date, band_index, stdDevCollector)
    stdDevCollector.results
  }

  private def _compute_histograms_time_series_from_datacube(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: ProjectedPolygons, from_date: String, to_date: String, band_index: Int, histogramsCollector: StatisticsCallback[_ >: Seq[Histogram[Double]]]): Unit = {
    val startDate: ZonedDateTime = ZonedDateTime.parse(from_date)
    val endDate: ZonedDateTime = ZonedDateTime.parse(to_date)
    intern.computeHistogramTimeSeries(datacube, polygons.polygons, polygons.crs, startDate, endDate, histogramsCollector, unusedCancellationContext, sc)
  }


  def compute_reduction_from_spatial_datacube(cube: MultibandTileLayerRDD[SpatialKey], reducer: String): JList[Double] = {
    def reduce(reducer: String): Vector[Double] = {
      val aggregate = aggregateBandTile(reducer)
      val combine = combineBandValues(reducer)

      val bandAggregatesPerTile = cube
        .map { case (_, multibandTile) => multibandTile.bands.map(aggregate) }

      val bandAggregates = bandAggregatesPerTile.fold(Vector[Double]()) { (bandAggregatesLeft, bandAggregatesRight) =>
        if (bandAggregatesLeft.isEmpty) bandAggregatesRight
        else if (bandAggregatesRight.isEmpty) bandAggregatesLeft
        else bandAggregatesLeft.zip(bandAggregatesRight)
          .map { case (leftAggregate, rightAggregate) =>
            combine(leftAggregate, rightAggregate)
          }
      }

      bandAggregates
    }

    val result = reducer match {
      case "mean" =>
        cube.cache()
        reduce("sum")
          .zip(reduce("count"))
          .map { case (sum, count) => sum / count }
      case _ => reduce(reducer)
    }

    result.asJava
  }

  private def aggregateBandTile(reducer: String): Tile => Double =
    reducer match {
      case "max" => tile => { val (_, max) = tile.findMinMaxDouble; max }
      case "min" => tile => { val (min, _) = tile.findMinMaxDouble; min }
      case "sum" => tile => {
        var sum = Double.NaN

        tile.foreachDouble { v =>
          if (isData(v)) {
            if (sum.isNaN) sum = v
            else sum += v
          }
        }

        sum
      }
      case "count" => tile => {
        var count = 0

        tile.foreachDouble { v => if (isData(v)) count += 1 }

        count
      }
  }

  private def combineBandValues(reducer: String): (Double, Double) => Double =
    reducer match {
      case "max" => _ max _
      case "min" => _ min _
      case "sum" => _ + _
      case "count" => _ + _
    }

  def compute_reduction_timeseries_from_spatiotemporal_datacube(cube: MultibandTileLayerRDD[SpaceTimeKey], reducer: String): JMap[String, JList[Double]] = {
    def reduce(reducer: String): RDD[(String, Vector[Double])] = {
      val aggregate = aggregateBandTile(reducer)
      val combine = combineBandValues(reducer)

      val timestampedBandAggregates = cube
        .groupBy { case (SpaceTimeKey(_, _, timestamp), _) => timestamp.toString } // TODO: properly format timestamp
        .mapValues { keyedMultibandTiles =>
          val multibandTiles = keyedMultibandTiles.map { case (_, multibandTile) => multibandTile }

          val bandAggregatesPerTile = multibandTiles
            .map { multibandTile =>
              multibandTile.bands.map(aggregate)
            }

          val bandAggregates = bandAggregatesPerTile.fold(Vector[Double]()) { (bandAggregatesLeft, bandAggregatesRight) =>
            if (bandAggregatesLeft.isEmpty) bandAggregatesRight
            else if (bandAggregatesRight.isEmpty) bandAggregatesLeft
            else bandAggregatesLeft.zip(bandAggregatesRight)
              .map { case (leftAggregate, rightAggregate) =>
                combine(leftAggregate, rightAggregate)
              }
          }

          bandAggregates
        }

      timestampedBandAggregates
    }

    val results = reducer match {
      case "mean" =>
        cube.cache()
        reduce("sum")
          .join(reduce("count"))
          .mapValues { case (sums, counts) =>
            sums.zip(counts).map { case (sum, count) => sum / count }
          }
      case _ => reduce(reducer)
    }

    results.collectAsMap()
      .view
      .mapValues(bandValues => bandValues.asJava)
      .toMap
      .asJava
  }

  def reduce_spatial(cube: MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder: SparkAggregateScriptBuilder): Unit = {
    // TODO: support spatial cube
    import org.apache.spark.sql._

    val isFloatingPoint = cube.metadata.cellType.isFloatingPoint
    val bandCount = new OpenEOProcesses().RDDBandCount(cube)

    val pixelRdd: RDD[Row] = for {
      keyedMultibandTile <- cube
      (spaceTimeKey, multibandTile) = keyedMultibandTile // odd that this doesn't work directly
      date = java.sql.Timestamp.from(spaceTimeKey.time.toInstant)
      row <- 0 until multibandTile.rows
      col <- 0 until multibandTile.cols
      bandValues = multibandTile.bands.map { tile =>
        if (isFloatingPoint) {
          val value = tile.getDouble(col, row)
          if (isNoData(value)) null else value
        } else {
          val value = tile.get(col, row)
          if (isNoData(value)) null else value
        }
      }
    } yield Row.fromSeq(date +: bandValues)

    val dataType = if (isFloatingPoint) DoubleType else IntegerType
    val bandColumns = (0 until bandCount).map(bandIndex => s"band_$bandIndex") // TODO: use actual band names

    val bandStructs = bandColumns.map(StructField(_, dataType))
    val dateStruct = StructField("date", TimestampType)

    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val df = spark.createDataFrame(pixelRdd, schema = StructType(dateStruct +: bandStructs))

    val expressionBuilder = scriptBuilder.generateFunction()
    val expressionColumns = for {
      colName <- bandColumns
      expressionColumn <- expressionBuilder(df.col(colName), colName)
    } yield expressionColumn

    val aggregated = df.groupBy("date").agg(expressionColumns.head, expressionColumns.tail: _*)
    aggregated.show() // TODO: write to CSV
  }

  private def sc: SparkContext = SparkContext.getOrCreate()


  private class StatisticsCollector extends StatisticsCallback[MeanResult] {
    import java.util._

    val results: JMap[String, JList[Double]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[Double]])

    override def onComputed(date: ZonedDateTime, results: Seq[MeanResult]): Unit = {
      val means = results.map(_.mean)

      this.results.put(isoFormat(date), means.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandStatisticsCollector extends StatisticsCallback[Seq[MeanResult]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[MeanResult]]): Unit = {
      val means = results.map(_.map(_.mean))

      this.results.put(isoFormat(date), means.map(_.asJava).asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandStatisticsWriter(outputFile: File) extends StatisticsCallback[MultibandMeans] with AutoCloseable {
    import com.fasterxml.jackson.core.JsonEncoding.UTF8
    import com.fasterxml.jackson.databind.ObjectMapper

    private val jsonGenerator = (new ObjectMapper).getFactory.createGenerator(outputFile, UTF8)

    jsonGenerator.synchronized {
      jsonGenerator.writeStartObject()
      jsonGenerator.flush()
    }

    override def onComputed(date: ZonedDateTime, polygonalMultibandMeans: Seq[MultibandMeans]): Unit =
      jsonGenerator.synchronized {
        jsonGenerator.writeArrayFieldStart(isoFormat(date))

        for (polygon <- polygonalMultibandMeans) {
          jsonGenerator.writeStartArray()

          for (bandMean <- polygon)
            if (bandMean.mean.isNaN) jsonGenerator.writeNull()
            else jsonGenerator.writeNumber(bandMean.mean)

          jsonGenerator.writeEndArray()
        }

        jsonGenerator.writeEndArray()
        jsonGenerator.flush()
      }

    override def onCompleted(): Unit =
      jsonGenerator.synchronized {
        jsonGenerator.writeEndObject()
        jsonGenerator.flush()
      }

    override def close(): Unit = jsonGenerator.close()
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
      Collections.synchronizedMap(new util.HashMap[String, JList[JList[JMap[Double, Long]]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[Histogram[Double]]]): Unit = {
      val polygonalHistograms: Seq[JList[JMap[Double, Long]]] = results.map( _.map(toMap).asJava)
      this.results.put(isoFormat(date), polygonalHistograms.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandMediansCollector extends StatisticsCallback[intern.MultibandHistogram[Double]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, multibandHistograms: Seq[MultibandHistogram[Double]]): Unit = {
      val polygonalMultibandMedians: Seq[JList[Double]] = for {
        multibandHistogram <- multibandHistograms
        multibandMedian = multibandHistogram.map(_.median().getOrElse(Double.NaN))
      } yield multibandMedian.asJava

      this.results.put(isoFormat(date), polygonalMultibandMedians.asJava)
    }

    override def onCompleted(): Unit = ()
  }

  private class MultibandStdDevCollector extends StatisticsCallback[Seq[Histogram[Double]]] {
    import java.util._

    val results: JMap[String, JList[JList[Double]]] =
      Collections.synchronizedMap(new util.HashMap[String, JList[JList[Double]]])

    override def onComputed(date: ZonedDateTime, results: Seq[Seq[Histogram[Double]]]): Unit = {
      val polygonalStdDevs: Seq[JList[Double]] = results.map( _.map(_.statistics().getOrElse(Statistics.EMPTYDouble()).stddev).asJava)
      this.results.put(isoFormat(date), polygonalStdDevs.asJava)
    }

    override def onCompleted(): Unit = ()
  }
}
