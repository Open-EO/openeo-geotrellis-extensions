package org.openeo.geotrellis.aggregate_polygon

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.DAYS
import java.util.concurrent.TimeUnit.MINUTES

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileBounds, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.histogram.{FastMapHistogram, Histogram, MutableHistogram, StreamingHistogram}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.{MultibandTileLayerRDD, _}
import geotrellis.vector._
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkContext}
import org.openeo.geotrellis.aggregate_polygon.intern.PixelRateValidator.exceedsTreshold
import org.openeo.geotrellis.aggregate_polygon.intern.polygonal._
import org.openeo.geotrellis.layers.LayerProvider
import org.slf4j.LoggerFactory

import scala.Double.NaN
import scala.collection.mutable
import scala.concurrent.duration.Duration

package object intern {

  private type PolygonsWithIndexMapping = (Seq[MultiPolygon], Seq[Set[Int]])
  type MultibandHistogram[T <: AnyVal] = Seq[Histogram[T]]

  private val logger = LoggerFactory.getLogger("be.vito.eodata.extracttimeseries.geotrellis.ComputeStatsGeotrellis")

  def computeAverageTimeSeries(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: Array[MultiPolygon], crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[MeanResult]], cancellationContext: CancellationContext, sc: SparkContext): Unit = {
    val boundingBox = ProjectedExtent(polygons.toSeq.extent, crs)
    val exceeds = exceedsTreshold(boundingBox, datacube.metadata, sc)

    val splitPolygons =
      try {
        Some(splitOverlappingPolygons(polygons))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          None
      }

    if (exceeds && splitPolygons.isDefined) {
      computeMultibandCollectionTimeSeries(datacube, splitPolygons.get, crs, startDate, endDate, statisticsCallback, cancellationContext, sc)
    } else {
      if (exceeds) {
        logOverlapWarning(polygons.length)
      }
      val sparkPool = sc.getLocalProperty("spark.scheduler.pool")
      val results: Array[Map[TemporalKey, Array[MeanResult]]] = computeAverageTimeSeries(datacube, polygons, crs, startDate, endDate,  sc).par.flatMap { rdd =>
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

  def computeAverageTimeSeries(maskedRdd: MultibandTileLayerRDD[SpaceTimeKey], polygons: Array[MultiPolygon], crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, sc: SparkContext) : Array[RDD[(TemporalKey, Array[MeanResult])]] = {
    if (polygons.length > 1 && maskedRdd.getStorageLevel == StorageLevel.NONE) {
      maskedRdd.cache()
    }
    val reprojected = polygons.map(_.reproject(crs, maskedRdd.metadata.crs))
    reprojected.map { polygon =>
      maskedRdd.polygonalSummaryByKey(
        polygon,
        Array.empty[MeanResult],
        MultibandMeanSummary,
        (k: SpaceTimeKey) => k.temporalKey
      )
    }
  }


  def computeMultibandCollectionTimeSeries(datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[MeanResult]], cancellationContext: CancellationContext, sc: SparkContext): Unit =
    computeMultibandCollectionTimeSeries(datacube, polygonsWithIndexMapping, crs, startDate, endDate, statisticsCallback, sc, denseMeansMultiBand, cancellationContext)

  def computeMultibandCollectionHistogramTimeSeries(datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, multibandHistogramsCallback: StatisticsCallback[_ >: MultibandHistogram[Double]], cancellationContext: CancellationContext, sc: SparkContext): Unit = {
    val multibandHistogramsWithoutEmptyDatesCallback = new StatisticsCallback[Seq[Histogram[Double]]] {
      override def onComputed(date: ZonedDateTime, multibandHistograms: Seq[MultibandHistogram[Double]]): Unit =
        if (multibandHistograms.flatten.nonEmpty) multibandHistogramsCallback.onComputed(date, multibandHistograms)

      override def onCompleted(): Unit = multibandHistogramsCallback.onCompleted()
    }

    computeMultibandCollectionTimeSeries(datacube, polygonsWithIndexMapping, crs, startDate, endDate, multibandHistogramsWithoutEmptyDatesCallback, sc, denseHistogramsMultiband, cancellationContext)
  }

  private def computeMultibandCollectionTimeSeries[V](datacube : MultibandTileLayerRDD[SpaceTimeKey], polygonsWithIndexMapping: PolygonsWithIndexMapping, crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[V], sc: SparkContext,
                                                      denseResults: (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]],
                                                        RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition], Seq[Set[Int]], ZonedDateTime,  Int) => Seq[V] , cancellationContext: CancellationContext): Unit = {
    import org.apache.spark.storage.StorageLevel._

    val (polygons, indexMapping) = polygonsWithIndexMapping

    // each polygon becomes a feature with a value that's equal to its position in the array
    val indexedFeatures = polygons
      .zipWithIndex
      .map { case (multiPolygon, index) => MultiPolygonFeature(multiPolygon, index.toDouble) }

    val byIndexMask = LayerProvider.createMaskLayer(indexedFeatures, crs, datacube.metadata, sc)

    try {
      val spatiallyPartitionedIndexMaskLayer = ContextRDD(byIndexMask.persist(MEMORY_ONLY_2), byIndexMask.metadata)

      val duration = DAYS.between(startDate.toLocalDate, endDate.toLocalDate)

      //Create list for the duration
      val sequentialDays = (0 to duration.toInt).par
        .map(dayOffset => startDate.plusDays(dayOffset))

      val featuresEnvelope: Extent = indexedFeatures.map(_.geom).extent.reproject(crs, datacube.metadata.crs)

      val spatialBounds: TileBounds = datacube.metadata.mapTransform.apply(featuresEnvelope)
      val minKey = SpatialKey(spatialBounds.colMin,spatialBounds.rowMin)
      val maxKey = SpatialKey(spatialBounds.colMax,spatialBounds.rowMax)


      val sparkPool = sc.getLocalProperty("spark.scheduler.pool")
      for {
        day <- sequentialDays
      } {
        if (!cancellationContext.canceled) {
          val lower = SpaceTimeKey(minKey, TemporalKey(day))
          val upper = SpaceTimeKey(maxKey, TemporalKey(day))
          val dataLayer = datacube.partitioner match {
            case Some(rp: SpacePartitioner[SpaceTimeKey]) =>
              val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
                case (l, u) => Math.min(l, u) to Math.max(l, u)
              }
              PartitionPruningRDD.create(datacube,partitionIndicies.contains).filterByRange(lower,upper)
            case Some(rp: RangePartitioner[SpaceTimeKey, MultibandTile]) =>
              datacube.filterByRange(lower,upper)
            case _ =>
              datacube.persist(MEMORY_AND_DISK_SER).filterByRange(lower,upper)
          }

          sc.setJobGroup(cancellationContext.id, cancellationContext.description, true)
          sc.setLocalProperty("spark.scheduler.pool", sparkPool)

          try {
            val results = denseResults(ContextRDD(dataLayer, datacube.metadata), spatiallyPartitionedIndexMaskLayer,
              indexMapping, day, indexedFeatures.size)

            statisticsCallback.onComputed(day, results)
          } finally {
            sc.clearJobGroup()
            sc.setLocalProperty("spark.scheduler.pool", null)
          }
        }
      }

      statisticsCallback.onCompleted()
    } finally {
      byIndexMask.unpersist(false)
    }
  }


  private def denseHistogramsMultiband(dataLayer: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]],
                                       zoneLayer: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition], indexMapping: Seq[Set[Int]],
                                       date: ZonedDateTime, targetSize: Int): Seq[MultibandHistogram[Double]] = {
    val empty: Histogram[Double] = StreamingHistogram(size = 0)

    // merge histograms of n split polygons, band-wise
    def merge(bhs: Iterable[MultibandHistogram[Double]]): MultibandHistogram[Double] = {
      def insert(from: Histogram[Double], into: MutableHistogram[Double]): Unit = from.foreach(into.countItem)

      def mergeBandWise(bh1: MultibandHistogram[Double], bh2: MultibandHistogram[Double]): MultibandHistogram[Double] = {
        val histogramPairs = bh1.zipAll(bh2, empty, empty)

        histogramPairs map { case (h1, h2) =>
          val (small, large) = if (h1.bucketCount() < h2.bucketCount()) (h1, h2) else (h2, h1)

          large match {
            case mutableLarge: MutableHistogram[Double] =>
              insert(small, into = mutableLarge)
              mutableLarge
            case _ => large merge small
          }
        }
      }

      bhs.foldLeft(Seq.empty[Histogram[Double]])(mergeBandWise)
    }

    val sparseHistograms: Map[Int, MultibandHistogram[Double]] = zonalHistograms(dataLayer, zoneLayer, date)

    indexMapping map { splitIndices =>
      def empty(splitIndex: Int): MultibandHistogram[Double] = Seq.empty

      val splitHistograms = splitIndices map { splitIndex => sparseHistograms.applyOrElse(splitIndex, empty) }
      merge(splitHistograms)
    }
  }



  private def zonalHistograms(dataLayer: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], zoneLayer: RDD[(SpatialKey, Tile)]  with Metadata[LayoutDefinition], date: ZonedDateTime): Map[Int, Seq[Histogram[Double]]] = {

    if(dataLayer.metadata.cellType.isFloatingPoint) {
      val histogramsForDate = MultibandZonal.histogramDouble(dataLayer.toSpatial(date),zoneLayer, zoneLayer.partitioner)
      val doubleHistograms = histogramsForDate
        .filter { case (zone, _) => zone != Int.MinValue } // noDataValue for IntConstantNoDataCellType of mask layer
        .mapValues(_.map((hist: Histogram[Double]) => {
          val mutableCopy = hist.mutable()

          mutableCopy.values()
            .filter(digitalValue => isNoData(digitalValue))
            .foreach(mutableCopy.uncountItem)

          mutableCopy
        }))

      doubleHistograms
    }else{
      val histogramsForDate = MultibandZonal.histogram(dataLayer.toSpatial(date),zoneLayer, zoneLayer.partitioner)
      val doubleHistograms = histogramsForDate
        .filter { case (zone, _) => zone != Int.MinValue } // noDataValue for IntConstantNoDataCellType of mask layer
        .mapValues(_.map((hist: Histogram[Int]) => toDouble(withoutNoData(hist)) { digital => digital.doubleValue() }))

      doubleHistograms
    }

  }


  private def denseMeansMultiBand(dataLayer: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]],
                                  zoneLayer: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition], indexMapping: Seq[Set[Int]],
                                  date: ZonedDateTime, targetSize: Int): Seq[Seq[MeanResult]] = {

    val sparseMeans: Map[Int, Seq[MeanResult]] = meansByRunningTotal(dataLayer, zoneLayer, date)
    //TODO also add histogram based computation as an optimization
    //if (dataLayer.metadata.cellType.isFloatingPoint) meansByRunningTotal(dataLayer, zoneLayer, date)
    //else meansByZonalHistogram(dataLayer, zoneLayer, date)

    indexMapping.map { splitIndices =>
      val empty = MeanResult()

      splitIndices.flatMap(sparseMeans.get).fold(Seq.empty) { (denseMean1, denseMean2) =>
        val meansPairedByBand = denseMean1.zipAll(denseMean2, empty, empty)
        meansPairedByBand.map { case (leftMean, rightMean) => leftMean + rightMean }
      }
    }
  }


  /**
   * Multiband version of the above
   * @param dataLayer
   * @param zoneLayer
   * @param date
   * @return
   */
  private def meansByRunningTotal(dataLayer: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], zoneLayer: RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition], date: ZonedDateTime): Map[Int, Seq[MeanResult]] = {
    val runningTotalsForDate: Map[Int, Seq[RunningTotal]] = ZonalRunningTotal.runningTotalsMultiband(dataLayer.toSpatial(date), zoneLayer)

    val means = runningTotalsForDate
      .filter { case (zone, _) => zone != Int.MinValue } // noDataValue for IntConstantNoDataCellType of mask layer
      .map { case (index, runningTotalForBands) =>
        (index, runningTotalForBands.map{runningTotal => MeanResult(runningTotal.validSum,runningTotal.validCount, runningTotal.totalCount) })
      }
    means
  }

  def computeHistogramTimeSeries(datacube: MultibandTileLayerRDD[SpaceTimeKey], polygons: Array[MultiPolygon], crs: CRS, startDate: ZonedDateTime, endDate: ZonedDateTime, statisticsCallback: StatisticsCallback[_ >: Seq[Histogram[Double]]], cancellationContext: CancellationContext, sc: SparkContext): Unit = {
    val boundingBox = ProjectedExtent(polygons.toSeq.extent, crs)
    val exceeds = exceedsTreshold(boundingBox, datacube.metadata, sc)

    val splitPolygons =
      try {
        Some(splitOverlappingPolygons(polygons))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          None
      }

    if (exceeds && splitPolygons.isDefined) {
      computeMultibandCollectionHistogramTimeSeries(datacube, splitPolygons.get, crs, startDate, endDate, statisticsCallback, cancellationContext, sc)
    } else {
      if (exceeds) {
        logOverlapWarning(polygons.length)
      }
      val sparkPool = sc.getLocalProperty("spark.scheduler.pool")
      val results: Array[Map[TemporalKey, Array[Histogram[Double]]]] = computeHistogramTimeSeries(datacube, polygons, crs, sc).par.flatMap { rdd =>
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

      val dates = results.flatMap(_.keys).distinct.sorted

      def filterNoDataFromHistogram(hist: Histogram[Double]) = {
        val newHist = StreamingHistogram(hist.bucketCount())
        hist.foreach((value, count) => if (!isNoData(value)) newHist.countItem(value, count))
        newHist
      }

      dates.foreach(d => statisticsCallback.onComputed(d, results.flatMap(_.get(d)).map(_.map(filterNoDataFromHistogram).toSeq)))
    }
  }


  private def computeHistogramTimeSeries(maskedRdd: MultibandTileLayerRDD[SpaceTimeKey], multiPolygons: Array[MultiPolygon], crs: CRS, sc: SparkContext): Array[RDD[(TemporalKey, Array[Histogram[Double]])]] = {

    if (multiPolygons.length > 1) {
      maskedRdd.cache
    }

    val reprojected = multiPolygons.map(_.reproject(crs, maskedRdd.metadata.crs))
    reprojected.map { polygon =>

      if (maskedRdd.metadata.cellType.isFloatingPoint) {
        val histograms: RDD[(TemporalKey, Array[Histogram[Double]])] = maskedRdd.polygonalSummaryByKey(
          polygon,
          Array.empty[Histogram[Double]],
          MultibandNoDataIncludingDoubleHistogramSummary,
          (k: SpaceTimeKey) => k.temporalKey
        )
        histograms
      } else {
        val histograms: RDD[(TemporalKey, Array[Histogram[Int]])] = maskedRdd.polygonalSummaryByKey(
          polygon,
          Array.empty[Histogram[Int]],
          MultibandNoDataIncludingIntHistogramSummary,
          (k: SpaceTimeKey) => k.temporalKey
        )
        histograms.mapValues(_.map(toDouble(_) {
          digital =>
            if (isData(digital)) digital.doubleValue()
            else NaN
        }))
      }
    }
  }


  private def withoutNoData(histogram: Histogram[Int]): Histogram[Int] = {
    // workaround for https://github.com/locationtech/geotrellis/issues/3149 but
    // FastMapHistogram(size = hist.bucketCount()) crashes (see https://github.com/locationtech/geotrellis/issues/3182)
    val dest = FastMapHistogram()
    histogram.foreach((digitalValue, count) => if (isData(digitalValue)) dest.countItem(digitalValue, count))
    dest
  }

  private def toDouble[T <: AnyVal](histogram: Histogram[T])(f: T => Double): Histogram[Double] = {
    val doubleHistogram = StreamingHistogram(histogram.bucketCount())

    for (value <- histogram.values()) {
      doubleHistogram.countItem(f(value), histogram.itemCount(value))
    }

    doubleHistogram
  }

  def overlap(polygons: Array[MultiPolygon]): Boolean = {
    val threshold = 0 // degrees^2

    for {
      i <- 0 until (polygons.length - 1)
      j <- (i + 1) until polygons.length
    } {
      val (p, q) = (polygons(i), polygons(j))
      if (p.intersects(q) && p.intersection(q).getArea > threshold) {
        return true
      }
    }

    false
  }

  def convertToMultiPolygon(geom: Geometry): Option[MultiPolygon] = {
    if (!geom.isEmpty)
      geom match {
        case polygon: MultiPolygon => Some(polygon)
        case polygon: Polygon => Some(MultiPolygon.apply(polygon))
        case _ => None
      }
    else {
      None
    }
  }

  def splitOverlappingPolygons(polygons: Seq[MultiPolygon]): PolygonsWithIndexMapping = {
    case class MultiPolygonEq(p: MultiPolygon) {
      override def equals(obj: Any): Boolean = {
        if (!obj.isInstanceOf[MultiPolygonEq]) return false
        p.equals(obj.asInstanceOf[MultiPolygonEq].p)
      }
    }

    if (overlap(polygons.toArray)) {
      val mappedPolygons = polygons.map(MultiPolygonEq)

      val mapIndexToPolygons = new mutable.HashMap[Int, Set[MultiPolygonEq]]

      mappedPolygons.zipWithIndex.foreach(p => mapIndexToPolygons.put(p._2, Set(p._1)))

      for (i <- 0 until (mappedPolygons.length - 1)) {
        val iPols = mapIndexToPolygons(i)
        val iPolMap = new mutable.HashMap[MultiPolygonEq, mutable.Set[MultiPolygonEq]]
        iPols.foreach(p => iPolMap.put(p, mutable.Set(p)))
        for (j <- i + 1 until polygons.length) {
          val jPols = mapIndexToPolygons(j)
          val jPolMap = new mutable.HashMap[MultiPolygonEq, mutable.Set[MultiPolygonEq]]
          jPols.foreach(p => jPolMap.put(p, mutable.Set(p)))
          for {
            iPol <- iPols
            jPol <- jPols
          } {
            val iPolRemove = mutable.Set[MultiPolygonEq]()
            val iPolAdd = mutable.Set[MultiPolygonEq]()
            for (iPolPart <- iPolMap(iPol)) {
              val jPolRemove = mutable.Set[MultiPolygonEq]()
              val jPolAdd = mutable.Set[MultiPolygonEq]()
              for (jPolPart <- jPolMap(jPol)) {
                if (iPolPart.p.intersects(jPolPart.p)) {
                  iPolPart.p.intersectionSafe(jPolPart.p).asMultiPolygon.foreach(int => {
                    convertToMultiPolygon(iPolPart.p.difference(int)).foreach(dif => {
                      iPolRemove += iPolPart
                      iPolAdd += MultiPolygonEq(int) += MultiPolygonEq(dif)
                    })
                    convertToMultiPolygon(jPolPart.p.difference(int)).foreach(dif => {
                      jPolRemove += jPolPart
                      jPolAdd += MultiPolygonEq(int) += MultiPolygonEq(dif)
                    })
                  })
                }
              }
              jPolMap(jPol) --= jPolRemove
              jPolMap(jPol) ++= jPolAdd
            }
            iPolMap(iPol) --= iPolRemove
            iPolMap(iPol) ++= iPolAdd
          }
          mapIndexToPolygons.put(j, jPolMap.values.reduce(_ ++= _).toSet)
        }
        mapIndexToPolygons.put(i, iPolMap.values.reduce(_ ++= _).toSet)
      }

      val polygonsResult = mapIndexToPolygons.values.flatten.toSeq.distinct
      val indexResult = for (i <- polygons.indices) yield mapIndexToPolygons(i).map(polygonsResult.indexOf)

      (polygonsResult.map(_.p), indexResult)
    } else {
      (polygons, polygons.zipWithIndex.map { case(_, i) => Set(i) })
    }
  }

  private def logOverlapWarning(nbPolygons: Int): Unit = {
    val estimateDuration = Duration(nbPolygons * 2 - nbPolygons * 2 % 15 + 15, MINUTES)

    val estimateString = {
      val hours = estimateDuration.toHours
      val minutes = estimateDuration.toMinutes % 60
      s"${if (hours > 0) s"$hours hour${if (hours > 1) "s" else ""}" +
        s"${if (minutes > 0) s" and " else ""}" else ""}" +
        s"${if (minutes > 0) s"$minutes minutes" else ""}"
    }

    logger.warn("Overlap detected\n\n" +
      "We detected that your input polygons overlap.\n\n" +
      "We will continue computing your timeseries, but when working with a lot of polygons, " +
      s"it is more efficient to make sure that they do not overlap.\n\n" +
      s"Your input contains $nbPolygons polygons.\n" +
      s"Note that your computations can take up to $estimateString")
  }

  object MultibandMeanSummary extends MultibandTilePolygonalSummaryHandler[Array[MeanResult]] {
    val MEAN0 = MeanResult()

    override def handlePartialMultibandTile(raster: Raster[MultibandTile], polygon: Polygon): Array[MeanResult] = {
      val Raster(multibandTile, _) = raster
      val rasterExtent = raster.rasterExtent

      multibandTile.bands.toArray.map { tile =>
        var sum = 0.0
        var valid = 0L
        var total = 0L
        if (multibandTile.cellType.isFloatingPoint) {
          polygon.foreach(rasterExtent)({ (col: Int, row: Int) =>
            val z = tile.getDouble(col, row)
            total += 1
            if (isData(z)) {
              sum = sum + z; valid = valid + 1
            }
          })
        } else {
          polygon.foreach(rasterExtent)({ (col: Int, row: Int) =>
            val z = tile.get(col, row)
            total += 1
            if (isData(z)) {
              sum = sum + z; valid = valid + 1
            }
          })
        }

        MeanResult(sum, valid, total, None, None)
      }
    }

    override def handleFullMultibandTile(multibandTile: MultibandTile): Array[MeanResult] =
      multibandTile.bands.toArray.map { tile =>
        if (tile.cellType.isFloatingPoint) {
          MeanResult.fromFullTileDouble(tile)
        } else {
          MeanResult.fromFullTile(tile)
        }
      }

    override def combineResults(rs: Seq[Array[MeanResult]]): Array[MeanResult] = {

      val bandCount = rs.map(_.length).max
      rs.foldLeft(Array.fill(bandCount)(MEAN0))((a1, a2) => a1.zip(a2).map{ case (mr1:MeanResult,mr2:MeanResult)=> mr1+mr2 })
    }

    override def combineOp(v1: Array[MeanResult], v2: Array[MeanResult]): Array[MeanResult] = {
      if(v1.isEmpty) {
        combineResults(Seq(Array.fill(v2.length)(MEAN0), v2))
      }else if(v2.isEmpty) {
        combineResults(Seq(v1,Array.fill(v1.length)(MEAN0)))
      }else{
        combineResults(Seq(v1, v2))
      }
    }
  }


}
