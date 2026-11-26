package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.raster
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.{BitCellType, CellType, GridBounds, MultibandTile, RasterExtent, Tile, UByteUserDefinedNoDataCellType}
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.GeneralUtils.gridBoundsFor
import org.openeo.geotrelliscommon.DatacubeSupport
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Preprocessing shared by the stitched/single-file writers (GeoTiff, NetCDF, Zarr),
 * so that the written data grid matches the (cropped) extent used to label it.
 */
object OutputPreprocessing {

  private val logger = LoggerFactory.getLogger(getClass)

  /** BitCellType is not reliably supported in output formats; convert to UByte to avoid ripple effects. */
  def safeBitCellType(cellType: CellType): CellType = cellType match {
    case BitCellType =>
      val safeCellType = UByteUserDefinedNoDataCellType(255.toByte)
      logger.debug(s"converting $cellType to $safeCellType")
      safeCellType
    case _ => cellType
  }

  /** Pixel-centre coordinates (x ascending, y descending) of a grid starting at the upper-left corner of `extent`. */
  def pixelCenterCoordinates(extent: Extent, cellWidth: Double, cellHeight: Double, cols: Int, rows: Int): (IndexedSeq[Double], IndexedSeq[Double]) = {
    val xValues = for (x <- 0 until cols) yield extent.xmin + x * cellWidth + cellWidth / 2.0
    val yValues = for (y <- 0 until rows) yield extent.ymax - y * cellHeight - cellHeight / 2.0
    (xValues, yValues)
  }

  /**
   * @param alignLayoutToCrop also re-tile when the layout origin differs from the cropped extent's upper-left corner,
   *                          so that tile (0,0) starts exactly at the cropped extent. Needed by writers that place tiles by key index (Zarr).
   */
  def preProcess[K: SpatialComponent : Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], cropBounds: Option[Extent], retainNoDataTiles:Boolean, alignLayoutToCrop: Boolean = false): (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = {
    val re = rdd.metadata.toRasterExtent()
    /**
     * CLAMPING EP-4150
     * Gridbounds are clamped to the actually available rasterextent, this means that we don't add empty data if somehow a cropping bounds is provided that is larger than the actual datacube.
     * CroppedExtent needs to match exactly with whatever gridbounds that we got, so there we do not clamp. So even though we clamp when computing gridbounds, it can still have an extent that is larger than rasterextent!!!
     */
    var gridBounds = gridBoundsFor(re, cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = false)
    val processes = new OpenEOProcesses()
    val filtered =
      if (retainNoDataTiles) rdd
      else processes.filterEmptyTile(rdd)
    val layoutExtent = rdd.metadata.layout.extent
    val layoutMisaligned = alignLayoutToCrop && {
      val eps = re.cellSize.width * 1e-6
      math.abs(layoutExtent.xmin - croppedExtent.xmin) > eps || math.abs(layoutExtent.ymax - croppedExtent.ymax) > eps
    }
    val preprocessedRdd = {
      if (gridBounds.colMin != 0 || gridBounds.rowMin != 0 || layoutMisaligned) {
        logger.info(s"save_result: unaligned grid bounds requires realignment: ${gridBounds} set strict_cropping format option to 'false' to avoid this.")
        val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), rdd.metadata.tileCols, rdd.metadata.tileRows)
        val targetBounds = rdd.metadata.bounds.setSpatialBounds(KeyBounds(geotiffLayout.mapTransform.extentToBounds(croppedExtent)))
        // The partitioner's index reduction relies on a correct band count, which comes from the band labels.
        val partitioner = if (DatacubeSupport.maybeBandLabels(rdd).isDefined)
          processes.computePartitionerForBounds(rdd, targetBounds, rdd.metadata.tileCols, rdd.metadata.tileRows)
        else {
          logger.warn("save_result: band labels are unknown, not setting a partitioner")
          None
        }
        // Same CRS, so no buffering is needed: bufferSize = 0 matches the default reproject behaviour.
        val retiledRDD = filtered.reproject(rdd.metadata.crs, geotiffLayout, 0, partitioner)._2.crop(croppedExtent, Options(force = false))

        gridBounds = gridBoundsFor(retiledRDD.metadata.toRasterExtent(), cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
        retiledRDD
      } else {
        // Buffering or not keeps the bottom line NaN.
        // However, buffering could make SentinelHub tiles to become almost empty.
        filtered.crop(croppedExtent, Options(force = false))
      }
    }
    val tileLayout = rdd.metadata.tileLayout
    val fullRDD = preprocessedRdd.withContext {
      _.mapValues[MultibandTile]((mbt: MultibandTile) => mbt.mapBands((i: Int, t: Tile) => raster.CroppedTile(t, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1))))
    }
    (gridBounds, croppedExtent, fullRDD)
  }
}
