package org.openeo.geotrellis.aggregate_polygon

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.DAYS

import be.vito.eodata.extracttimeseries.geotrellis.ComputeStatsGeotrellis.{PolygonsWithIndexMapping, splitOverlappingPolygons}
import be.vito.eodata.extracttimeseries.geotrellis.PixelRateValidator.exceedsTreshold
import be.vito.eodata.extracttimeseries.geotrellis.{CancellationContext, ComputeStatsGeotrellis, LayerConfig, LayerProvider, LayersConfig, MeanResult, RunningTotal, StatisticsCallback, ZonalRunningTotal}
import be.vito.eodata.processing.MaskedStatisticsProcessor.StatsMeanResult
import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileBounds, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.{RangePartitioner, SparkContext}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.openeo.geotrellis.SpatialToSpacetimeJoinRdd

import scala.Double.NaN

object AggregatePolygonProcess {
  private type PolygonsWithIndexMapping = (Seq[MultiPolygon], Seq[Set[Int]])
}

class AggregatePolygonProcess(layersConfig: LayersConfig) {
  import AggregatePolygonProcess._

  val computeStatsGeotrellis = new ComputeStatsGeotrellis(layersConfig)

  def computeAverageTimeSeries(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: Array[MultiPolygon], crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[StatsMeanResult]], cancellationContext: CancellationContext, sc: SparkContext): Unit = {


    val boundingBox = ProjectedExtent(polygons.toSeq.extent, crs)
    val exceeds = exceedsTreshold(boundingBox, datacube.metadata, sc)

    val splitPolygons =
      try {
        Some(splitOverlappingPolygons(polygons))
      } catch {
        case _: Throwable => None
      }

    if (exceeds && splitPolygons.isDefined) {
      computeMultibandCollectionTimeSeries(datacube, splitPolygons.get, crs, startDate, endDate, statisticsCallback, sc, cancellationContext)
    } else {
      val sparkPool = sc.getLocalProperty("spark.scheduler.pool")
      val results: Array[Map[TemporalKey, Array[MeanResult]]] = computeStatsGeotrellis.computeAverageTimeSeries(datacube, polygons, crs, startDate, endDate,  sc).par.flatMap { rdd =>
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
        val converted: Seq[Seq[_ <: StatsMeanResult]] = maybeResultses.map {
          case Some(r) =>r.map{meanResult => new StatsMeanResult(meanResult.meanPhysical, meanResult.total, meanResult.valid)}.toSeq
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
  private def computeMultibandCollectionTimeSeries(datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[StatsMeanResult]], sc: SparkContext,
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
      val zonalStats = combinedRDD.flatMap { case (date, (t1, t2)) => ZonalRunningTotal(t1, t2).map(index_total => ((date.time,index_total._1),index_total._2)).filterKeys(_._2>=0) }
        .reduceByKey((a,b) =>a.zip(b).map({ case (total_a,total_b) => total_a + total_b})).collectAsMap()

      val dates: Iterable[ZonedDateTime] = zonalStats.map(_._1._1)
      for (date <- dates){
        val valuesForDate: collection.Map[Int, Seq[StatsMeanResult]] = zonalStats.filter(_._1._1==date).map(t=>(t._1._2,t._2.map{ meanResult => new StatsMeanResult(meanResult.mean.getOrElse(Double.NaN), meanResult.totalCount, meanResult.validCount)
        }))
        statisticsCallback.onComputed(date,valuesForDate.toSeq.sortBy(_._1).map(_._2))
      }
      statisticsCallback.onCompleted()
    }finally{
      byIndexMask.unpersist()
    }
  }

}
