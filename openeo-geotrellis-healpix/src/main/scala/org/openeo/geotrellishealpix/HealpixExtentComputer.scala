package org.openeo.geotrellishealpix

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.vector.Extent
import healpix.{HealpixBase, Scheme}
import org.apache.spark.sql.{DataFrame, functions}
import org.openeo.geotrellis.SafeTransform

/** * Computes the spatial extent of a HEALPix datacube in a target CRS, * based on the actual cell IDs present in the DataFrame. */
object HealpixExtentComputer {

  /**   * Compute extent from unique cell IDs in a scalar layout datacube.   * Distinct cell IDs are extracted and their center coordinates are used   * to compute the bounding extent in the target CRS.   */
  def extentFromScalarCellIds(df: DataFrame,
                              nside: Int,
                              targetCRS: CRS): Extent = {
    // Collect distinct cell IDs from the cell_id column
    val cellIds = df.select(functions.col(HealpixSchema.CellId))
      .distinct()
      .rdd
      .map(row => row.getAs[Long](0))
      .collect()
      .toSeq

    computeExtentFromCellIds(cellIds, nside, targetCRS)
  }

  /**   * Compute extent from cell ranges in a packed layout datacube.   * Each packed row represents a contiguous range: [cell_id_start, cell_id_start + chunk_size).   * All cells in these ranges are used to compute the extent.   */
  def extentFromPackedCellIds(df: DataFrame,
                              nside: Int,
                              targetCRS: CRS): Extent = {
    // Collect (start, size) pairs
    val ranges = df.select(
      functions.col(HealpixSchema.CellIdStart),
      functions.col(HealpixSchema.ChunkSize)
    ).rdd.map { row =>
      val start = row.getAs[Long](0)
      val size = row.getAs[Int](1)
      (start, size)
    }.collect().toSeq

    // Expand all ranges into individual cell IDs
    val cellIds = ranges.flatMap { case (start, size) =>
      (0 until size).map(i => start + i)
    }

    computeExtentFromCellIds(cellIds, nside, targetCRS)
  }

  /**   * Compute bounding extent from a sequence of HEALPix cell IDs.   * Converts cell centers to target CRS and tracks min/max coordinates.   *   * @param cellIds sequence of HEALPix cell IDs   * @param nside HEALPix NSIDE   * @param targetCRS target coordinate reference system   * @return bounding extent in the target CRS   */
  private def computeExtentFromCellIds(cellIds: Seq[Long],
                                       nside: Int,
                                       targetCRS: CRS): Extent = {
    if (cellIds.isEmpty) {
      return Extent(0, 0, 0, 0)  // Empty extent
    }

    val base = new HealpixBase(nside, Scheme.NESTED)

    val transform = SafeTransform(LatLng, targetCRS)

    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    for (cellId <- cellIds) {
      val pointing = base.pix2ang(cellId)
      val latRad = math.Pi / 2.0 - pointing.theta
      val lonRad = pointing.phi

      val latDeg = math.toDegrees(latRad)
      val lonDeg = math.toDegrees(lonRad)

      val (x, y) = transform(lonDeg, latDeg)

      minX = math.min(minX, x)
      minY = math.min(minY, y)
      maxX = math.max(maxX, x)
      maxY = math.max(maxY, y)
    }

    Extent(minX, minY, maxX, maxY)
  }
}