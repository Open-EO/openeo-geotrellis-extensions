package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.layer.{LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import org.apache.spark.rdd.RDD
import spire.syntax.cfor.cfor

import scala.collection.mutable

/*
  Ad-hoc implementation because Geotrellis lacks support for histograms with double values (it truncates the values).
  A StreamingHistogram only delivers approximate results anyway; also, a histogram is not suited for data like CHIRPS
  that consists of 32 bit floats since it gets very big.
*/

object ZonalRunningTotal {
  def apply(tile: Tile, zones: Tile): Map[Int, RunningTotal] = {
    val runningTotals = mutable.Map[Int, RunningTotal]()

    val rows  = tile.rows
    val cols  = tile.cols

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        val v = tile.getDouble(col, row)
        val z = zones.get(col, row)
        if(!runningTotals.contains(z)) { runningTotals(z) = new RunningTotal() }
        runningTotals(z) += v
      }
    }

    runningTotals.toMap
  }

  def apply(tile: MultibandTile, zones: Tile): Map[Int, Seq[RunningTotal]] = {
    val runningTotals = mutable.Map[Int, Seq[RunningTotal]]()

    val rows  = tile.rows
    val cols  = tile.cols
    val bands = tile.bandCount

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        val z = zones.get(col, row)
        if(!runningTotals.contains(z)) { runningTotals(z) = Array.fill(tile.bandCount){new RunningTotal()} }
        cfor(0)(_ < bands, _ + 1) { band =>
          val v = tile.band(band).getDouble(col, row)
          runningTotals(z)(band) += v
        }
      }
    }

    runningTotals.toMap
  }

  def merge(a: Map[Int, RunningTotal], b: Map[Int, RunningTotal]): Map[Int, RunningTotal] = {
    val res = a.withDefaultValue(new RunningTotal)
    b.foldLeft(res) { case (acc, (zone, runningTotal)) => acc + (zone -> (acc(zone) + runningTotal)) }
  }

  def mergeMultiband(a: Map[Int, Seq[RunningTotal]], b: Map[Int, Seq[RunningTotal]]): Map[Int, Seq[RunningTotal]] = {
    val head = a.headOption.orElse(b.headOption)
    if(head.isEmpty) {
      return a
    }
    val res: Map[Int, Seq[RunningTotal]] = a.withDefaultValue(Array.fill(head.get._2.size){new RunningTotal})
    b.foldLeft(res) { case (acc: Map[Int, Seq[RunningTotal]], (zone: Int, runningTotalList: Seq[RunningTotal])) => acc + (zone -> acc(zone).zip(runningTotalList).map({ case (total_a,total_b) => total_a + total_b})) }
  }

  def runningTotals(maskLayer: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]], zoneLayer: RDD[(SpatialKey, Tile)]  with Metadata[LayoutDefinition] ): Map[Int, RunningTotal] = {
    maskLayer.join(zoneLayer,zoneLayer.partitioner.get)
      .map { case (_, (t1, t2)) => ZonalRunningTotal(t1, t2) }
      .fold(Map[Int, RunningTotal]())(ZonalRunningTotal.merge)
  }

  def runningTotalsMultiband(maskLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], zoneLayer: RDD[(SpatialKey, Tile)]  with Metadata[LayoutDefinition] ): Map[Int, Seq[RunningTotal]] = {
    maskLayer.join(zoneLayer,zoneLayer.partitioner.get)
      .map { case (_, (t1, t2)) => ZonalRunningTotal(t1, t2) }
      .fold(Map[Int, Seq[RunningTotal]]())(ZonalRunningTotal.mergeMultiband)
  }
}

case class RunningTotal private(var validSum: Double, var validCount: Long, var totalCount: Long) {
  def this() = this(0, 0, 0)

  def mean: Option[Double] =
    if (validCount > 0) Some(validSum / validCount)
    else None

  def +(that: RunningTotal): RunningTotal =
    copy(this.validSum + that.validSum, this.validCount + that.validCount, this.totalCount + that.totalCount)

  def +=(value: Double): Unit = {
    if (!value.isNaN) {
      validSum += value
      validCount += 1
    }

    totalCount += 1
  }
}
