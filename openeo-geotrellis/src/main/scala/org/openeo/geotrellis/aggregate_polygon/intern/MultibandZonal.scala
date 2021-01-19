/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.raster._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.mapalgebra.zonal._
import org.apache.spark.Partitioner
import org.apache.spark.rdd._
import spire.syntax.cfor.cfor

import scala.collection.mutable
import scala.reflect.ClassTag

object MultibandZonal {
  private def mergeMaps[T <: AnyVal](a: Map[Int, MultibandHistogram[T]], b: Map[Int, MultibandHistogram[T]]): Map[Int, MultibandHistogram[T]] = {
    var res = a
    for ((k, v) <- b)
      res = res + (k ->
        (
          if (res.contains(k)) res(k).zip(v).map(t => t._1.merge(t._2))
          else v
        )
      )

    res
  }

  def histogram[K: ClassTag](rdd: RDD[(K, MultibandTile)], zonesTileRdd: RDD[(K, Tile)], partitioner: Option[Partitioner] = None): Map[Int, MultibandHistogram[Int]] =
    partitioner
      .fold(rdd.join(zonesTileRdd))(rdd.join(zonesTileRdd, _))
      .map((t: (K, (MultibandTile, Tile))) => t._2._1.bands.map(b =>IntZonalHistogram(b, t._2._2).toSeq ).reduce( _ ++ _).groupBy(_._1).mapValues(_.map(_._2).toSeq) )
      .fold(Map[Int, MultibandHistogram[Int]]())(mergeMaps)

  def histogramDouble[K: ClassTag](rdd: RDD[(K, MultibandTile)], zonestileRdd: RDD[(K, Tile)], partitioner: Option[Partitioner] = None): Map[Int, MultibandHistogram[Double]] =
    partitioner
      .fold(rdd.join(zonestileRdd))(rdd.join(zonestileRdd, _))
      .map { case (_, (multibandDataTile, zoneTile)) =>
        val bandZonalHistograms = multibandDataTile.bands
          .map(bandTile => NoDataHandlingDoubleZonalHistogram(bandTile, zoneTile, 512))

        val zonalMultibandHistograms: Map[Int, MultibandHistogram[Double]] = bandZonalHistograms
          .map(_.toSeq)
          .reduce(_ ++ _)
          .groupBy { case (zone, _) => zone }
          .mapValues(zonalHistograms => zonalHistograms.map { case (_, histogram) => histogram })

        zonalMultibandHistograms
      }
      .fold(Map[Int, MultibandHistogram[Double]]())(mergeMaps)
}


object NoDataHandlingDoubleZonalHistogram extends ZonalHistogram[Double] {

  def apply(tile: Tile, zones: Tile): Map[Int, Histogram[Double]] =
    apply(tile, zones, 80)

  def apply(tile: Tile, zones: Tile, n: Int): Map[Int, Histogram[Double]] = {
    val histMap = mutable.Map[Int, MutableHistogram[Double]]()

    val rows  = tile.rows
    val cols  = tile.cols

    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>

        val v = tile.getDouble(col, row)
        val z = zones.get(col, row)
        if(isData(v)) {
          if(!histMap.contains(z)) { histMap(z) = StreamingHistogram(n) }
          histMap(z).countItem(v)
        }
      }
    }

    histMap.toMap
  }
}
