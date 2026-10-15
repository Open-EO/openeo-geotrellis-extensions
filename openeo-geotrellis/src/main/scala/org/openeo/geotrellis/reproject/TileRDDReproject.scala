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

package org.openeo.geotrellis.reproject

import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.buffer.{BufferSizes, BufferedTile}
import geotrellis.raster.crop._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.reproject.{ReprojectRasterExtent}
import org.openeo.geotrellis.reproject.RasterRegionReproject
import geotrellis.raster.stitch._
import geotrellis.spark._
import geotrellis.spark.buffer.BufferTilesRDD
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.reproject.Reproject
import geotrellis.vector._
import geotrellis.util._
import org.log4s._
import org.apache.spark.rdd._
import org.apache.spark._
import org.openeo.geotrelliscommon.{ByTileSpacetimePartitioner, ConfigurableSpaceTimePartitioner, ConfigurableSpatialPartitionerReduceZ, SparseSpaceOnlyPartitioner, SparseSpaceTimePartitioner, SparseSpatialPartitioner}

import scala.reflect.ClassTag

object TileRDDReproject {
  import geotrellis.spark.reproject.Reproject.Options
  @transient private[this] lazy val logger = getLogger

  /** Reproject a set of buffered
    * @tparam           K           Key type; requires spatial component.
    * @tparam           V           Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            bufferedTiles      An RDD of buffered tiles, created using the BufferTiles operation.
    * @param            metadata           The raster metadata for this keyed tile set.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       Either the layout scheme or layout definition to use when re-keying the reprojected layers.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    */
  def apply[
    K: SpatialComponent: Boundable: ClassTag,
    V <: CellGrid[Int]: ClassTag: RasterRegionReproject: * => TileMergeMethods[V]: * => TilePrototypeMethods[V]
  ](
    bufferedTiles: RDD[(K, BufferedTile[V])],
    metadata: TileLayerMetadata[K],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    options: Options,
    partitioner: Option[Partitioner]
  ): (Int, RDD[(K, V)] with Metadata[TileLayerMetadata[K]]) = {
    val crs: CRS = metadata.crs
    val layout = metadata.layout
    val tileLayout: TileLayout = layout.tileLayout
    implicit val sc = bufferedTiles.context

    val sourceDataGridExtent = metadata.layout.createAlignedGridExtent(metadata.extent)
    val (targetCellSize:CellSize,targetDataExtent:Extent)=
    targetLayout match {
      case Right(l) => {
        (l.cellSize, l.createAlignedGridExtent(ProjectedExtent(metadata.extent,metadata.crs).reproject(destCrs)).extent)
      }
      case Left(l) => {
        val passthroughGridExtent = ReprojectRasterExtent(sourceDataGridExtent, metadata.crs, destCrs)
        (passthroughGridExtent.cellSize,passthroughGridExtent.extent)
      }
    }


    // inspect the change in spatial extent to get the pixel counts
    val reprojectSummary = matchReprojectRasterExtent(
      metadata.crs, destCrs,
      metadata.layout,
      metadata.bounds.toOption.map { case KeyBounds(s, e) =>
        KeyBounds(s.getComponent[SpatialKey], e.getComponent[SpatialKey])
      })
    logger.debug(s"$reprojectSummary")

    // First figure out where we're going through option yoga
    // You'll want to read [[ReprojectRasterExtent]] to grok this
    val LayoutLevel(targetZoom, targetLayerLayout) = targetLayout match {
      case Right(layoutDefinition) =>
        // A LayoutDefinition specifies a GridExtent.  The presence of this
        // option indicates that the user knows exactly the grid to resample to.
        LayoutLevel(0, layoutDefinition)

      case Left(layoutScheme: FloatingLayoutScheme) =>
        // FloatingLayoutScheme implies trying to match the resulting layout to
        // the extent of the reprojected input region.  This may require snapping
        // to a different GridExtent depending on the settings in
        // rasterReprojectOptions.
        if ((options: Reproject.Options).matchLayerExtent) {
          val tre = ReprojectRasterExtent(layout, crs, destCrs, (options: Reproject.Options).rasterReprojectOptions)

          layoutScheme.levelFor(tre.extent, tre.cellSize)
        } else {
          (options: Reproject.Options).rasterReprojectOptions.parentGridExtent match {
            case Some(ge) =>
              layoutScheme.levelFor(targetDataExtent, ge.cellSize)

            case None =>
              (options: Reproject.Options).rasterReprojectOptions.targetCellSize match {
                case Some(ct) =>
                  layoutScheme.levelFor(targetDataExtent, ct)

                case None =>
                  layoutScheme.levelFor(targetDataExtent, targetCellSize)
              }
          }
        }

      case Left(layoutScheme) =>
        // Zoomed or user-defined layout.  Cannot snap to new grid.  Only need
        // to find appropriate zoom level.
        if (options.matchLayerExtent) {
          val tre = ReprojectRasterExtent(
            sourceDataGridExtent, crs, destCrs,
            (options: Reproject.Options).rasterReprojectOptions.copy(
              parentGridExtent=None, targetCellSize=None, targetRasterExtent=None))

          layoutScheme.levelFor(tre.extent, tre.cellSize)
        } else {
          val tre = ReprojectRasterExtent(
            sourceDataGridExtent, crs, destCrs,
            (options: Reproject.Options).rasterReprojectOptions)

          if ((options: Reproject.Options).rasterReprojectOptions.targetCellSize.isDefined
              || (options: Reproject.Options).rasterReprojectOptions.parentGridExtent.isDefined) {
            // options targetCellSize or parentGridExtent will have effected cellSize
            layoutScheme.levelFor(tre.extent, tre.cellSize)
          } else {
            val cellSize: CellSize = reprojectSummary.cellSize
            layoutScheme.levelFor(tre.extent, cellSize)
          }
        }
    }

    val rasterReprojectOptions = (options: Reproject.Options).rasterReprojectOptions.copy(
      parentGridExtent = Some(targetLayerLayout),
      targetCellSize = None,
      targetRasterExtent = None
    )

    val newMetadata = {
      metadata.copy(
        crs = destCrs,
        layout = targetLayerLayout,
        extent = targetDataExtent,
        bounds = metadata.bounds.setSpatialBounds(
          KeyBounds(targetLayerLayout.mapTransform.extentToBounds(targetDataExtent)))
      )
    }

    val newLayout = newMetadata.layout
    val maptrans = newLayout.mapTransform

    val targetPartitioner: Option[Partitioner] = {
      val sourceNumPartitions = bufferedTiles.getNumPartitions
      val tileCountRatio = targetToSourceTileCountRatio(reprojectSummary, tileLayout, newLayout)
      val remapSpatialKey: SpatialKey => Iterable[SpatialKey] = key =>
        maptrans.keysForGeometry(ProjectedExtent(key.extent(layout), crs).reprojectAsPolygon(destCrs, 0.05))

      adaptPartitionerToDownsampling(
        partitioner.orElse(bufferedTiles.partitioner), sourceNumPartitions, tileCountRatio, newMetadata.bounds,
        remapSpatialKey)
    }

    val rrp = implicitly[org.openeo.geotrellis.reproject.RasterRegionReproject[V]]

    val stagedTiles: RDD[(K, (Raster[V], RasterExtent, Polygon))] =
      bufferedTiles
        .mapPartitions { partition =>
          partition.flatMap { case (key, BufferedTile(tile, gridBounds)) => {
            val innerExtent = key.getComponent[SpatialKey].extent(layout)
            val innerRasterExtent = RasterExtent(innerExtent, gridBounds.width, gridBounds.height)
            val outerGridBounds =
              GridBounds(
                -gridBounds.colMin,
                -gridBounds.rowMin,
                tile.cols - gridBounds.colMin - 1,
                tile.rows - gridBounds.rowMin - 1
              )
            val outerExtent = innerRasterExtent.extentFor(outerGridBounds, clamp = false)
            val destRegion = ProjectedExtent(innerExtent, crs).reprojectAsPolygon(destCrs, 0.05)

            maptrans.keysForGeometry(destRegion).map { newKey =>
              val destRE = RasterExtent(maptrans(newKey), newLayout.tileLayout.tileCols, newLayout.tileLayout.tileRows)
              (key.setComponent[SpatialKey](newKey), (Raster(tile, outerExtent), destRE, destRegion))
            }
          }}
        }

    def createCombiner(tup: (Raster[V], RasterExtent, Polygon)) = {
        val (raster, destRE, destRegion) = tup
        rrp.regionReproject(raster, crs, destCrs, destRE, destRegion, rasterReprojectOptions.method, rasterReprojectOptions.errorThreshold).tile
      }

    def mergeValues(reprojectedTile: V, toReproject: (Raster[V], RasterExtent, Polygon)) = {
      val (raster, destRE, destRegion) = toReproject
      val destRaster = Raster(reprojectedTile, destRE.extent)
      // RDD.combineByKey contract allows us to safely re-use and mutate the accumulator, reprojectedTile
      rrp.regionReprojectMutable(raster, crs, destCrs, destRaster, destRegion, rasterReprojectOptions.method, rasterReprojectOptions.errorThreshold).tile
    }

    def mergeCombiners(reproj1: V, reproj2: V) = reproj1.merge(reproj2)

    val tiled: RDD[(K, V)] =
      targetPartitioner match {
        case Some(part) => stagedTiles.combineByKey(createCombiner, mergeValues, mergeCombiners, partitioner = part)
        case None => stagedTiles.combineByKey(createCombiner, mergeValues, mergeCombiners)
      }

    (targetZoom, ContextRDD(tiled, newMetadata))
  }

  /** Reproject a keyed tile RDD.
    *
    * @tparam           K           Key type; requires spatial component.
    * @tparam           V           Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            rdd                The keyed tile RDD.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       The layout scheme to use when re-keying the reprojected layers.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    */
  def apply[
    K: SpatialComponent: Boundable: ClassTag,
    V <: CellGrid[Int]: ClassTag: RasterRegionReproject: Stitcher: * => CropMethods[V]: * => TileMergeMethods[V]: * => TilePrototypeMethods[V]
  ](
    rdd: RDD[(K, V)] with Metadata[TileLayerMetadata[K]],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    options: Options,
    partitioner: Option[Partitioner]
  ): (Int, RDD[(K, V)] with Metadata[TileLayerMetadata[K]]) = {
    if(rdd.metadata.crs == destCrs) {
      val layout = rdd.metadata.layout

      val (zoom, bail) =
        targetLayout match {
          case Left(layoutScheme) =>
            val LayoutLevel(zoom, newLayout) = layoutScheme.levelFor(layout.extent, layout.cellSize)
            (zoom, newLayout == layout)
          case Right(layoutDefinition) =>
            (0, layoutDefinition == layout)
        }

      if(bail) {
        // This is a no-op, just return the source
        (zoom, rdd)
      } else {
        // We are tiling against a new layout but we
        // don't need to worry about buffers since the source and target are
        // in the same CRS.
        apply(rdd, destCrs, targetLayout, bufferSize = 0, options = options, partitioner = partitioner)
      }
    } else {
      val crs = rdd.metadata.crs
      val layout = rdd.metadata.layout
      val tileLayout = rdd.metadata.layout.tileLayout
      // Avoid capturing instantiated Transform in closure because its not Serializable
      lazy val transform = Transform(crs, destCrs)

      val bufferedTiles =
        BufferTilesRDD(
          layer = rdd,
          includeKey = rdd.metadata.bounds.includes(_: K),
          getBufferSizes = { key: K =>
            val extent = key.getComponent[SpatialKey].extent(layout)
            val srcRE = RasterExtent(extent, tileLayout.tileCols, tileLayout.tileRows)
            val dstRE = ReprojectRasterExtent(srcRE, transform)

            // Reproject the extent back into the original CRS,
            // to determine how many border pixels we need.
            // Pad by one extra pixel.
            val e = dstRE.extent.reproject(destCrs, crs)
            val gb = srcRE.gridBoundsFor(e, clamp = false)
            BufferSizes(
              left   = 1 + (if(gb.colMin < 0) -gb.colMin else 0),
              right  = 1 + (if(gb.colMax >= srcRE.cols) gb.colMax - (srcRE.cols - 1) else 0),
              top    = 1 + (if(gb.rowMin < 0) -gb.rowMin else 0),
              bottom = 1 + (if(gb.rowMax >= srcRE.rows) gb.rowMax - (srcRE.rows - 1) else 0)
            )
          })

      apply(bufferedTiles, rdd.metadata, destCrs, targetLayout, options, partitioner = partitioner)
    }
  }

  /** Reproject this keyed tile RDD, using a constant border size for the operation.
    * @tparam           K                  Key type; requires spatial component.
    * @tparam           V                  Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            rdd                The keyed tile RDD.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       The layout scheme to use when re-keying the reprojected layers.
    * @param            bufferSize         Number of pixels to buffer the tile with. The tile will only be buffered by this amount on
    *                                      any side if there is an adjacent, abutting tile to contribute the border pixels.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    *
    * @note             This is faster than computing the correct border size per key, so if you know that a specific border size will be sufficient
    *                   to be accurate, e.g. if the CRS's are not very different and so the rasters will not skew heavily, then this method can be used
    *                   for performance benefit.
    */
  def apply[
    K: SpatialComponent: Boundable: ClassTag,
    V <: CellGrid[Int]: ClassTag: RasterRegionReproject: Stitcher: * => CropMethods[V]: * => TileMergeMethods[V]: * => TilePrototypeMethods[V]
  ](
    rdd: RDD[(K, V)] with Metadata[TileLayerMetadata[K]],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    bufferSize: Int,
    options: Options,
    partitioner: Option[Partitioner]
  ): (Int, RDD[(K, V)] with Metadata[TileLayerMetadata[K]]) =
    if(bufferSize == 0) {
      val fakeBuffers: RDD[(K, BufferedTile[V])] = rdd.withContext(_.mapValues { tile: V => BufferedTile(tile, GridBounds(0, 0, tile.cols - 1, tile.rows - 1)) })
      apply(fakeBuffers, rdd.metadata, destCrs, targetLayout, options, partitioner)
    } else
      apply(rdd.bufferTiles(bufferSize), rdd.metadata, destCrs, targetLayout, options, partitioner)

  /** Ratio of the number of target tiles to the number of source tiles covering the same area. */
  private def targetToSourceTileCountRatio(summary: ReprojectSummary, sourceTileLayout: TileLayout,
                                           targetLayout: LayoutDefinition): Double = {
    val sourceTileCount = summary.sourcePixels / (sourceTileLayout.tileCols.toDouble * sourceTileLayout.tileRows)
    val targetTileCount = summary.rescaledPixelCount(targetLayout.cellSize) /
      (targetLayout.tileLayout.tileCols.toDouble * targetLayout.tileLayout.tileRows)
    targetTileCount / sourceTileCount
  }

  /**
   * When downsampling significantly (e.g. 20m to 500m), the number of output tiles is much smaller than the number of
   * input tiles. Keeping the input partition count then results in a lot of (nearly) empty partitions for this and all
   * subsequent stages, see https://github.com/Open-EO/openeo-geotrellis-extensions/issues/508.
   *
   * In that case, a partitioner with fewer partitions that fits the target layout is returned instead. A
   * SpacePartitioner remains a SpacePartitioner with a compatible index because downstream code relies on it.
   */
  private[reproject] def adaptPartitionerToDownsampling[K: Boundable: ClassTag](
    partitioner: Option[Partitioner],
    sourceNumPartitions: Int,
    tileCountRatio: Double,
    targetBounds: Bounds[K],
    remapSpatialKey: SpatialKey => Iterable[SpatialKey]
  ): Option[Partitioner] = {
    val significantDownsampling = !tileCountRatio.isNaN && !tileCountRatio.isInfinite && tileCountRatio < 0.5
    if (!significantDownsampling) return partitioner

    val currentNumPartitions = partitioner.map(_.numPartitions).getOrElse(sourceNumPartitions)
    val proposedNumPartitions = math.max(1, math.ceil(sourceNumPartitions * tileCountRatio).toInt)
    if (currentNumPartitions <= 2 * proposedNumPartitions) return partitioner

    val adapted: Partitioner = partitioner match {
      case Some(spacePartitioner: SpacePartitioner[K] @unchecked) =>
        val targetIndex = retargetPartitionerIndex(spacePartitioner.index, remapSpatialKey)
        SpacePartitioner(targetBounds)(implicitly, implicitly, targetIndex)
      case _ => new HashPartitioner(proposedNumPartitions)
    }

    if (adapted.numPartitions * 2 <= currentNumPartitions) {
      logger.info(s"Downsampling during reproject (target/source tile count ratio: $tileCountRatio): reducing " +
        s"partitions from $currentNumPartitions to ${adapted.numPartitions} (proposed: $proposedNumPartitions)")
      Some(adapted)
    } else partitioner
  }

  /** Sparse indices refer to keys in the source layout; map them to the target layout. */
  private def retargetPartitionerIndex[K](index: PartitionerIndex[K],
                                          remapSpatialKey: SpatialKey => Iterable[SpatialKey]): PartitionerIndex[K] = {
    def remapSpaceTimeKeys(keys: Array[SpaceTimeKey]): Array[SpaceTimeKey] = {
      val remappedSpatialKeys = keys.map(_.spatialKey).distinct.map(k => k -> remapSpatialKey(k)).toMap
      keys.flatMap(key => remappedSpatialKeys(key.spatialKey).map(SpaceTimeKey(_, key.time))).distinct
    }

    def remapSpatialKeys(keys: Array[SpatialKey]): Array[SpatialKey] = keys.flatMap(remapSpatialKey).distinct

    val targetIndex: PartitionerIndex[_] = index match {
      case sparse: SparseSpaceTimePartitioner => sparse.theKeys match {
        case Some(keys) =>
          val targetKeys = remapSpaceTimeKeys(keys)
          val indices = targetKeys.map(SparseSpaceTimePartitioner.toIndex(_, sparse.indexReduction)).distinct.sorted
          new SparseSpaceTimePartitioner(indices, sparse.indexReduction, Some(targetKeys))
        case None => new ConfigurableSpaceTimePartitioner(sparse.indexReduction)
      }
      case sparse: SparseSpaceOnlyPartitioner => sparse.theKeys match {
        case Some(keys) =>
          val targetKeys = remapSpaceTimeKeys(keys)
          val indices = targetKeys.map(SparseSpaceOnlyPartitioner.toIndex(_, sparse.indexReduction)).distinct.sorted
          new SparseSpaceOnlyPartitioner(indices, sparse.indexReduction, Some(targetKeys))
        case None => new ConfigurableSpaceTimePartitioner(sparse.indexReduction)
      }
      case sparse: SparseSpatialPartitioner => sparse.theKeys match {
        case Some(keys) =>
          val targetKeys = remapSpatialKeys(keys)
          val indices = targetKeys.map(SparseSpaceOnlyPartitioner.toIndex(_, sparse.indexReduction)).distinct.sorted
          new SparseSpatialPartitioner(indices, sparse.indexReduction, Some(targetKeys))
        case None => new ConfigurableSpatialPartitionerReduceZ(sparse.indexReduction)
      }
      case byTile: ByTileSpacetimePartitioner => new ByTileSpacetimePartitioner(byTile.theKeys.map(remapSpatialKeys))
      case boundsBased => boundsBased // derives its partitions from the (target) bounds
    }

    targetIndex.asInstanceOf[PartitionerIndex[K]]
  }


  /** Match pixel resolution between two layouts in different projections such that
    * cell size in target projection and layout matches the most resolute tile.
    *
    * Note: that the ammount of pixel resolution distortion during the reprojection
    * depends on the location of the extent being reprojeected.
    */
  private
  def matchReprojectRasterExtent(
    src: CRS, dst: CRS,
    layout: LayoutDefinition,
    keyBounds: Option[KeyBounds[SpatialKey]]
  )(implicit sc: SparkContext): ReprojectSummary = {
    // Bounds of tiles we need to examine
    val bounds: GridBounds[Int] = keyBounds match {
      case Some(kb) =>
        kb.toGridBounds()
      case None =>
        GridBounds(0, 0, layout.layoutCols, layout.layoutRows)
    }

    val mapTransform = layout.mapTransform
    val getRasterExtent: (Int, Int) => RasterExtent = { (col, row) =>
      RasterExtent(mapTransform(col, row), layout.tileCols, layout.tileRows)
    }

    val chunks = bounds.split(512, 512).toVector
    sc.parallelize(chunks, chunks.length)
      .map { boundsChunk =>
        import scala.concurrent._
        import scala.concurrent.duration._
        import ExecutionContext.Implicits.global

        val splitWork: Iterator[Future[ReprojectSummary]] =
          boundsChunk.split(128,128) // large enough to be worth a Future
            .map { subChunk =>
              Future {
                // Proj4Transform is not thread safe
                val transform = Proj4Transform(src, dst)
                subChunk.coordsIter
                  .map { case (col, row) =>
                    val source = getRasterExtent(col, row)
                    val target = ReprojectRasterExtent(source, transform)
                    ReprojectSummary(
                      sourcePixels = source.size,
                      pixels = target.size,
                      extent = target.extent,
                      cellSize = target.cellSize
                    )
                  }
                  .reduce(_ combine _)
              }
            }

        Await
          .result(Future.sequence(splitWork), Duration.Inf)
          .reduce(_ combine _)
      }
      .reduce(_ combine _)
  }

  private case class ReprojectSummary(
    sourcePixels: Double,
    pixels: Double,
    extent: Extent,
    cellSize: CellSize
  ) {
    /** Combine summary and project pixel counts to highest resolution */
    def combine(other: ReprojectSummary): ReprojectSummary = {
      if (cellSize.resolution <= other.cellSize.resolution)
        ReprojectSummary(
          sourcePixels + other.sourcePixels,
          pixels + other.rescaledPixelCount(cellSize),
          extent combine other.extent,
          cellSize)
      else
        ReprojectSummary(
          sourcePixels + other.sourcePixels,
          rescaledPixelCount(other.cellSize) + other.pixels,
          extent combine other.extent,
          other.cellSize)
    }

    /** How many pixels were added in reproject */
    def pixelRatio: Double = pixels / sourcePixels

    def rescaledPixelRatio(target: CellSize): Double =
      rescaledPixelCount(target) / sourcePixels

    /** How many pixels would it take to cover the same area in different cellSize? */
    def rescaledPixelCount(target: CellSize): Double = {
      val CellSize(w0, h0) = cellSize
      val CellSize(w1, h1) = target
      pixels * (w0 * h0) / (w1 * h1)
    }
  }
}
