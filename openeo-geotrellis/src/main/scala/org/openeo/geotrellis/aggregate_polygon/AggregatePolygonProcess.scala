package org.openeo.geotrellis.aggregate_polygon

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.DAYS

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.SpatialToSpacetimeJoinRdd
import org.openeo.geotrellis.aggregate_polygon.intern.PixelRateValidator.exceedsTreshold
import org.openeo.geotrellis.aggregate_polygon.intern._
import org.openeo.geotrellis.layers.LayerProvider

object AggregatePolygonProcess {
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
      val zonalStats = combinedRDD.flatMap { case (date, (t1:MultibandTile, t2:Tile)) => ZonalRunningTotal(t1, t2).map(index_total => ((date.time,index_total._1),index_total._2)).filterKeys(_._2>=0) }
        .reduceByKey((a,b) =>a.zip(b).map({ case (total_a,total_b) => total_a + total_b})).collectAsMap()

      val statsByDate: Map[ZonedDateTime, collection.Map[(ZonedDateTime, Int), Seq[RunningTotal]]] = zonalStats.groupBy(_._1._1)

      for (stats <- statsByDate){
        val date = stats._1
        val valuesForDate: collection.Map[Int, Seq[MeanResult]] = stats._2.map(t=>(t._1._2,t._2.map{ meanResult: RunningTotal => new MeanResult(meanResult.validSum, meanResult.validCount, meanResult.totalCount,Option.empty,Option.empty)
        }))

        //the map might not contain results for all features, it is sparse, so we have to make it dense
        //we also have to merge results of features that were splitted because of overlap
        val denseResults = indexMapping.map { splitIndices =>
          val empty = new MeanResult(Double.NaN, 0L, 0L,Option.empty,Option.empty)

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
