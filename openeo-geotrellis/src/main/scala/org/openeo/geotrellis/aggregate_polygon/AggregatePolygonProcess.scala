package org.openeo.geotrellis.aggregate_polygon

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey}
import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark._
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.openeo.geotrellis.SpatialToSpacetimeJoinRdd
import org.openeo.geotrellis.aggregate_polygon.intern.PixelRateValidator.exceedsTreshold
import org.openeo.geotrellis.aggregate_polygon.intern._
import org.openeo.geotrellis.layers.LayerProvider
import org.slf4j.LoggerFactory
import spire.syntax.cfor.cfor

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.DAYS
import scala.collection.mutable.ListBuffer

object AggregatePolygonProcess {

  private val logger = LoggerFactory.getLogger(classOf[AggregatePolygonProcess])

  private type PolygonsWithIndexMapping = (Seq[MultiPolygon], Seq[Set[Int]])
}

class AggregatePolygonProcess() {
  import AggregatePolygonProcess._

  def computeAverageTimeSeries(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: Array[MultiPolygon], crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[MeanResult]], cancellationContext: CancellationContext, sc: SparkContext): Unit = {


    val extent = sc.parallelize(polygons).map(_.extent).reduce(_.combine(_))
    val boundingBox = ProjectedExtent(extent, crs)
    val exceeds = exceedsTreshold(boundingBox, datacube.metadata, sc)

    val splitPolygons =
      try {
        Some(splitOverlappingPolygons(polygons))
      } catch {
        case _: Throwable => None
      }

    if (exceeds && splitPolygons.isDefined) {
      if(!datacube.partitioner.isEmpty && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
        println("Large number of pixels requested, we can use an optimized implementation.")
        //Use optimized implementation for space partitioner
        sc.setJobDescription("Avg timeseries: " + polygons.length + " from: " + startDate + " to: " + endDate + " using SpacePartitioner")
        computeMultibandCollectionTimeSeries(datacube, splitPolygons.get, crs, startDate, endDate, statisticsCallback, sc, cancellationContext)
      } else{
        println("Large numbers of pixels found, we will run one job per date, that computes aggregate values for all polygons.")
        sc.setJobDescription("Avg timeseries: " + polygons.length + " from: " + startDate + " to: " + endDate + " using one Spark job per date!")
        org.openeo.geotrellis.aggregate_polygon.intern.computeMultibandCollectionTimeSeries(datacube, splitPolygons.get, crs, startDate, endDate, statisticsCallback, cancellationContext, sc)
      }
    } else {
      println("The number of pixels in your request is below the threshold, we'll compute the timeseries for each polygon separately.")
      val sparkPool = sc.getLocalProperty("spark.scheduler.pool")
      val results: Array[Map[TemporalKey, Array[MeanResult]]] = org.openeo.geotrellis.aggregate_polygon.intern.computeAverageTimeSeries(datacube, polygons, crs, startDate, endDate,  sc).par.flatMap { rdd =>
        if (!cancellationContext.canceled) {
          sc.setJobGroup(cancellationContext.id, cancellationContext.description, interruptOnCancel = true)
          sc.setLocalProperty("spark.scheduler.pool", sparkPool)

          try {
            Some(rdd.collect().toMap)
          } finally {
            sc.clearJobGroup()
            sc.setLocalProperty("spark.scheduler.pool", null)
          }
        } else None
      }.toArray

      val dates = for (d <- 0 to DAYS.between(startDate, endDate).toInt) yield startDate.plusDays(d)

      dates.foreach(d => statisticsCallback.onComputed(d, {
        val maybeResultses: Array[Option[Array[MeanResult]]] = results.map(_.get(d))
        val converted: Seq[Seq[_ <: MeanResult]] = maybeResultses.map {
          case Some(r) =>r.map{meanResult => new MeanResult(meanResult.sum, meanResult.valid, meanResult.total,Option.empty,Option.empty)}.toSeq
          case None => Seq.empty
        }
        converted
      }))

      statisticsCallback.onCompleted()
    }
  }


  def aggregateSpatialForGeometry(scriptBuilder:SparkAggregateScriptBuilder, datacube : MultibandTileLayerRDD[SpaceTimeKey], points: Seq[Geometry], crs: CRS, bandCount:Int, outputPath:String): Unit = {
    val sc = datacube.sparkContext

    // each polygon becomes a feature with a value that's equal to its position in the array
    val indexedFeatures = points
      .zipWithIndex
      .map { case (geom, index) => Feature(geom, index.toInt) }

    val geometryRDD = sc.parallelize(indexedFeatures).clipToGrid(datacube.metadata).groupByKey()
    val combinedRDD = new SpatialToSpacetimeJoinRdd(datacube, geometryRDD)

    val pixelRDD: RDD[Row] = combinedRDD.flatMap {
      case (key: SpaceTimeKey, (tile: MultibandTile, geoms: Iterable[Feature[Geometry,Int]])) => {
        val result: ListBuffer[Row] = ListBuffer()
        val bands = tile.bandCount

        val options = raster.RasterizerOptions.DEFAULT


        val date = java.sql.Date.valueOf(key.time.toLocalDate)
        val re = RasterExtent(tile, datacube.metadata.keyToExtent(key))
        val bandsValues: Array[Object] = Array.ofDim[Object](bands)
        for (g: Feature[Geometry, Int] <- geoms) {
          if (tile.cellType.isFloatingPoint) {
            g.geom.foreach(re, options)({ (col: Int, row: Int) =>
              cfor(0)(_ < bands, _ + 1) { band =>
                val v = tile.band(band).getDouble(col, row)
                if(v.isNaN) {
                  bandsValues(band) = null
                }else{
                  bandsValues(band) = v.asInstanceOf[Object]
                }
              }
              result.append(Row.fromSeq(Seq(date, g.data) ++ bandsValues.toSeq))
            })
          } else {
            g.geom.foreach(re, options)({ (col: Int, row: Int) =>
              cfor(0)(_ < bands, _ + 1) { band =>
                val v = tile.band(band).get(col, row)
                if(v == NODATA) {
                  bandsValues(band) = null
                }else{
                  bandsValues(band) = v.asInstanceOf[Object]
                }
              }
              result.append(Row.fromSeq(Seq(date, g.data) ++ bandsValues.toSeq))
            })
          }
        }
        result
      }
    }
    val cellType = datacube.metadata.cellType
    aggregateByDateAndPolygon(pixelRDD, scriptBuilder, bandCount, cellType, outputPath)
  }

  def aggregateSpatialForGeometryWithSpatialCube(scriptBuilder:SparkAggregateScriptBuilder, datacube : MultibandTileLayerRDD[SpatialKey], points: Seq[Geometry], crs: CRS, bandCount:Int, outputPath:String): Unit = {
    val sc = datacube.sparkContext

    // each polygon becomes a feature with a value that's equal to its position in the array
    val indexedFeatures = points
      .zipWithIndex
      .map { case (geom, index) => Feature(geom, index.toInt) }

    val geometryRDD = sc.parallelize(indexedFeatures).clipToGrid(datacube.metadata).groupByKey()
    val combinedRDD = datacube.join(geometryRDD)

    val pixelRDD: RDD[Row] = combinedRDD.flatMap {
      case (key: SpatialKey, (tile: MultibandTile, geoms: Iterable[Feature[Geometry,Int]])) => {
        val result: ListBuffer[Row] = ListBuffer()
        val bands = tile.bandCount

        val options = raster.RasterizerOptions.DEFAULT

        val re = RasterExtent(tile, datacube.metadata.keyToExtent(key))
        val bandsValues: Array[Object] = Array.ofDim[Object](bands)
        for (g: Feature[Geometry, Int] <- geoms) {
          if (tile.cellType.isFloatingPoint) {
            g.geom.foreach(re, options)({ (col: Int, row: Int) =>
              cfor(0)(_ < bands, _ + 1) { band =>
                val v = tile.band(band).getDouble(col, row)
                if(v.isNaN) {
                  bandsValues(band) = null
                }else{
                  bandsValues(band) = v.asInstanceOf[Object]
                }
              }
              result.append(Row.fromSeq(Seq( g.data) ++ bandsValues.toSeq))
            })
          } else {
            g.geom.foreach(re, options)({ (col: Int, row: Int) =>
              cfor(0)(_ < bands, _ + 1) { band =>
                val v = tile.band(band).get(col, row)
                if(v == NODATA) {
                  bandsValues(band) = null
                }else{
                  bandsValues(band) = v.asInstanceOf[Object]
                }
              }
              result.append(Row.fromSeq(Seq( g.data) ++ bandsValues.toSeq))
            })
          }
        }
        result
      }
    }
    val cellType = datacube.metadata.cellType
    aggregateByPolygon(pixelRDD, scriptBuilder, bandCount, cellType, outputPath)
  }

  private def aggregateByPolygon(pixelRDD: RDD[Row], scriptBuilder: SparkAggregateScriptBuilder, bandCount: Int, cellType: CellType, outputPath: String) = {
    val session = SparkSession.builder().config(pixelRDD.sparkContext.getConf).getOrCreate()
    val dataType =
      if (cellType.isFloatingPoint) {
        DoubleType
      } else {
        IntegerType
      }
    val bandColumns = (0 until bandCount).map(b => f"band_$b")
    val bandStructs = bandColumns.map(StructField(_, dataType, true))

    val schema = StructType(Seq(StructField("feature_index", IntegerType, true)) ++ bandStructs)
    val df = session.createDataFrame(pixelRDD, schema)
    val dataframe = df.withColumnRenamed(df.columns(1), "feature_index")

    val builder = scriptBuilder.generateFunction()
    val expressionCols: Seq[Column] = bandColumns.flatMap(col => builder(df.col(col), col))
    dataframe.groupBy("feature_index").agg(expressionCols.head, expressionCols.tail: _*).coalesce(1).write.option("header", "true").option("emptyValue", "").mode(SaveMode.Overwrite).csv("file://" + outputPath)
  }

  def aggregateSpatialGeneric(scriptBuilder:SparkAggregateScriptBuilder, datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, bandCount:Int, outputPath:String): Unit = {
    import org.apache.spark.storage.StorageLevel._

    val (polygons, indexMapping) = polygonsWithIndexMapping

    var index = -1
    val invertedMapping = indexMapping.flatMap(set => {
      index = index + 1
      set.map((_,index))
    }).groupBy(_._1).map{t => (t._1,t._2.map(_._2))}.toList.sortBy(_._1 ).map(_._2).toArray

    // each polygon becomes a feature with a value that's equal to its position in the array
    val indexedFeatures = polygons
      .zipWithIndex
      .map { case (multiPolygon, index) => MultiPolygonFeature(multiPolygon, index.toDouble) }

    val sc = datacube.sparkContext

    val byIndexMask = LayerProvider.createMaskLayer(indexedFeatures, crs, datacube.metadata, sc)

    try {
      val polygonMappingBC = sc.broadcast(invertedMapping)
      val spatiallyPartitionedIndexMaskLayer: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = ContextRDD(byIndexMask.persist(MEMORY_ONLY_2), byIndexMask.metadata)
      val combinedRDD = new SpatialToSpacetimeJoinRdd(datacube, spatiallyPartitionedIndexMaskLayer)
      val pixelRDD: RDD[Row] = combinedRDD.flatMap{
        case (key: SpaceTimeKey,( tile: MultibandTile,zones: Tile)) => {
          val rows  = tile.rows
          val cols  = tile.cols
          val bands = tile.bandCount

          val mapping = polygonMappingBC.value
          val date = java.sql.Date.valueOf(key.time.toLocalDate)
          val result: ListBuffer[Row] = ListBuffer()
          val bandsValues: Array[Object] = Array.ofDim[Object](bands)

          if(tile.cellType.isFloatingPoint){
            cfor(0)(_ < rows, _ + 1) { row =>
              cfor(0)(_ < cols, _ + 1) { col =>
                val z = zones.get(col, row)
                if(z>=0) {
                  cfor(0)(_ < bands, _ + 1) { band =>
                    val v = tile.band(band).getDouble(col, row)
                    if(v.isNaN) {
                      bandsValues(band) = null
                    }else{
                      bandsValues(band) = v.asInstanceOf[Object]
                    }
                  }
                  val indices = mapping(z)
                  indices.foreach(polygonIndex => result.append(Row.fromSeq(Seq(date, polygonIndex) ++ bandsValues.toSeq)))
                }
              }
            }
          }else{
            cfor(0)(_ < rows, _ + 1) { row =>
              cfor(0)(_ < cols, _ + 1) { col =>
                val z = zones.get(col, row)
                if(z>=0) {
                  cfor(0)(_ < bands, _ + 1) { band =>
                    val v = tile.band(band).get(col, row)
                    if(v == NODATA) {
                      bandsValues(band) = null
                    }else{
                      bandsValues(band) = v.asInstanceOf[Object]
                    }
                  }
                  val indices = mapping(z)
                  indices.foreach(polygonIndex => result.append(Row.fromSeq(Seq(date, polygonIndex) ++ bandsValues.toSeq)))
                }
              }
            }
          }


          result
        }
      }
      val cellType = datacube.metadata.cellType
      aggregateByDateAndPolygon(pixelRDD, scriptBuilder, bandCount, cellType, outputPath)

    }finally{
      byIndexMask.unpersist()
    }
  }

  private def aggregateByDateAndPolygon(pixelRDD: RDD[Row], scriptBuilder: SparkAggregateScriptBuilder, bandCount: Int, cellType: CellType, outputPath: String) = {
    val session = SparkSession.builder().config(pixelRDD.sparkContext.getConf).getOrCreate()
    val dataType =
      if (cellType.isFloatingPoint) {
        DoubleType
      } else {
        IntegerType
      }
    val bandColumns = (0 until bandCount).map(b => f"band_$b")

    val bandStructs = bandColumns.map(StructField(_, dataType, true))

    val schema = StructType(Seq(StructField("date", DateType, true),
      StructField("feature_index", IntegerType, true),
    ) ++ bandStructs)
    val df = session.createDataFrame(pixelRDD, schema)
    val dataframe = df.withColumnRenamed(df.columns(0), "date").withColumnRenamed(df.columns(1), "feature_index")
    //val expressions = bandColumns.flatMap(col => Seq((col,"sum"),(col,"max"))).toMap
    //https://spark.apache.org/docs/3.2.0/sql-ref-null-semantics.html#built-in-aggregate
    //Seq(count(col.isNull),count(not(col.isNull)),expr(s"percentile_approx(band_1,0.95)")
    val builder = scriptBuilder.generateFunction()
    val expressionCols: Seq[Column] = bandColumns.flatMap(col => builder(df.col(col), col))
    dataframe.groupBy("date", "feature_index").agg(expressionCols.head, expressionCols.tail: _*).coalesce(1).write.option("header", "true").option("emptyValue", "").mode(SaveMode.Overwrite).csv("file://" + outputPath)
  }

  /*
  def computeMultibandCollectionTimeSeries(datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[StatsMeanResult]], cancellationContext: CancellationContext, sc: SparkContext): Unit =
    computeMultibandCollectionTimeSeries(datacube, polygonsWithIndexMapping, crs, startDate, endDate, statisticsCallback, sc, denseMeansMultiBand, cancellationContext)
*/
  private def computeMultibandCollectionTimeSeries(datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[MeanResult]], sc: SparkContext,
                                                      cancellationContext: CancellationContext): Unit = {
    import org.apache.spark.storage.StorageLevel._

    val (polygons, indexMapping) = polygonsWithIndexMapping

    // each polygon becomes a feature with a value that's equal to its position in the array
    val indexedFeatures = polygons
      .zipWithIndex
      .map { case (multiPolygon, index) => MultiPolygonFeature(multiPolygon, index.toDouble) }

    val byIndexMask = LayerProvider.createMaskLayer(indexedFeatures, crs, datacube.metadata, sc)

    try {
      val spatiallyPartitionedIndexMaskLayer: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = ContextRDD(byIndexMask.persist(MEMORY_ONLY_2), byIndexMask.metadata)
      val combinedRDD = new SpatialToSpacetimeJoinRdd(datacube, spatiallyPartitionedIndexMaskLayer)
      val zonalStats: RDD[(ZonedDateTime, Iterable[((ZonedDateTime, Int), Seq[RunningTotal])])] = combinedRDD.flatMap { case (date, (t1:MultibandTile, t2:Tile)) => ZonalRunningTotal(t1, t2).map(index_total => ((date.time,index_total._1),index_total._2)).filterKeys(_._2>=0) }
        .reduceByKey((a,b) =>a.zip(b).map({ case (total_a,total_b) => total_a + total_b})).groupBy(_._1._1)

      val statsByDate: collection.Map[ZonedDateTime, Map[Int, Seq[MeanResult]]] = zonalStats.mapValues(_.map{
        case ((date,id:Int),stats: Seq[RunningTotal]) => (id,stats.map{ meanResult: RunningTotal => new MeanResult(meanResult.validSum, meanResult.validCount, meanResult.totalCount,Option.empty,Option.empty)})
      }.toMap
      ).collectAsMap()


      for (stats <- statsByDate){
        val date = stats._1
        val valuesForDate: Map[Int, Seq[MeanResult]] = stats._2

        //the map might not contain results for all features, it is sparse, so we have to make it dense
        //we also have to merge results of features that were splitted because of overlap
        val denseResults = indexMapping.map { splitIndices =>
          val empty = MeanResult()

          splitIndices.flatMap(valuesForDate.get).fold(Seq.empty) { (denseMean1, denseMean2) =>
            val meansPairedByBand = denseMean1.zipAll(denseMean2, empty, empty)
            meansPairedByBand.map { case (leftMean, rightMean) => leftMean + rightMean }
          }
        }

        statisticsCallback.onComputed(date,denseResults)
      }
      statisticsCallback.onCompleted()
    }finally{
      byIndexMask.unpersist()
    }
  }

}
