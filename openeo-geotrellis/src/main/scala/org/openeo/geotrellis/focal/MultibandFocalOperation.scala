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

package org.openeo.geotrellis.focal

import geotrellis.layer.SpatialComponent
import geotrellis.raster._
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.mapalgebra.focal._
import geotrellis.spark._
import geotrellis.util.MethodExtensions
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object MultibandFocalOperation {
  private def mapOverBufferedTiles[K: SpatialComponent: ClassTag](bufferedTiles: RDD[(K, BufferedTile[MultibandTile])], neighborhood: Neighborhood)
      (calc: (Tile, Option[GridBounds[Int]]) => Tile): RDD[(K, MultibandTile)] =
    bufferedTiles
      .mapValues { case BufferedTile(tile, gridBounds) => tile.mapBands( (index,tile) => calc(tile, Some(gridBounds))) }

  def apply[K: SpatialComponent: ClassTag](
    rdd: RDD[(K, MultibandTile)],
    neighborhood: Neighborhood,
    partitioner: Option[Partitioner])
    (calc: (Tile, Option[GridBounds[Int]]) => Tile)(implicit d: DummyImplicit): RDD[(K, MultibandTile)] =
      mapOverBufferedTiles(rdd.bufferTiles(neighborhood.extent, partitioner), neighborhood)(calc)

  def apply[K: SpatialComponent: ClassTag](
    rdd: RDD[(K, MultibandTile)],
    neighborhood: Neighborhood,
    layerBounds: GridBounds[Int],
    partitioner: Option[Partitioner])
    (calc: (Tile, Option[GridBounds[Int]]) => Tile): RDD[(K, MultibandTile)] =
      mapOverBufferedTiles(rdd.bufferTiles(neighborhood.extent, layerBounds, partitioner), neighborhood)(calc)

  def apply[K: SpatialComponent: ClassTag](rasterRDD: MultibandTileLayerRDD[K], neighborhood: Neighborhood, partitioner: Option[Partitioner])
      (calc: (Tile, Option[GridBounds[Int]]) => Tile): MultibandTileLayerRDD[K] =
    rasterRDD.withContext { rdd =>
      apply(rdd, neighborhood, rasterRDD.metadata.tileBounds, partitioner)(calc)
    }
}

abstract class MultibandFocalOperation[K: SpatialComponent: ClassTag] extends MethodExtensions[MultibandTileLayerRDD[K]] {

  def focal(n: Neighborhood, partitioner: Option[Partitioner])
      (calc: (Tile, Option[GridBounds[Int]]) => Tile): MultibandTileLayerRDD[K] =
    MultibandFocalOperation(self, n, partitioner)(calc)

  def focalWithCellSize(n: Neighborhood, partitioner: Option[Partitioner])
      (calc: (Tile, Option[GridBounds[Int]], CellSize) => Tile): MultibandTileLayerRDD[K] = {
    val cellSize = self.metadata.layout.cellSize
    MultibandFocalOperation(self, n, partitioner){ (tile, bounds) => calc(tile, bounds, cellSize) }
  }
}
