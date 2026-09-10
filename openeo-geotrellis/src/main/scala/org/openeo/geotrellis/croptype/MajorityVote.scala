package org.openeo.geotrellis.croptype

import geotrellis.raster.{ShortArrayTile, ShortConstantNoDataCellType, Tile, isNoData}
import org.openeo.geotrellis.OpenEOProcesses

/**
  * Majority-vote postprocessing for classification label tiles.
  *
  * For every pixel that is not part of `excludedValues`, replaces its label with the
  * label value that is most frequent in its `kernelSize` x `kernelSize` neighbourhood.
  * This mirrors the python reference implementation `_majority_vote_labels` in
  * worldcereal-classification/src/worldcereal/openeo/inference.py: for every unique,
  * non-excluded label value a binary mask is convolved with an all-ones kernel, and the
  * label with the highest count "wins" the pixel.
  *
  * The actual convolution reuses [[OpenEOProcesses#convolveTile]], the same
  * Kernel/Convolve/FFTConvolve based kernel operation used by
  * [[OpenEOProcesses#apply_kernel]].
  */
object MajorityVote {

  val MIN_KERNEL_SIZE = 1
  val MAX_KERNEL_SIZE = 25

  /**
    * Apply majority-vote smoothing to a single label tile.
    *
    * @param labelTile      the input label tile
    * @param kernelSize     size (in pixels) of the square, all-ones voting kernel. Must be
    *                       between [[MIN_KERNEL_SIZE]] and [[MAX_KERNEL_SIZE]]. A value of 1
    *                       is a no-op.
    * @param excludedValues label values that are never overwritten and never contribute a
    *                       "vote" (e.g. no-crop / nodata sentinel values).
    * @return a new tile (same celltype/dimensions as `labelTile`) with majority-vote applied.
    */
  def apply(labelTile: Tile, kernelSize: Int, excludedValues: Set[Int]): Tile = {
    if (kernelSize < MIN_KERNEL_SIZE)
      throw new IllegalArgumentException(s"kernelSize must be >= $MIN_KERNEL_SIZE for majority vote")
    if (kernelSize > MAX_KERNEL_SIZE)
      throw new IllegalArgumentException(s"kernelSize cannot exceed $MAX_KERNEL_SIZE for majority vote")
    if (kernelSize == 1)
      return labelTile

    val cols = labelTile.cols
    val rows = labelTile.rows

    // Determine the unique, non-excluded label values present in the tile (sorted, to
    // match python's `sorted(np.unique(labels[valid_mask]))` and its argmax tie-breaking
    // behaviour, which always favors the value with the lowest index on ties).
    val uniqueLabels = scala.collection.mutable.TreeSet.empty[Int]
    labelTile.foreach(v => if (!isNoData(v) && !excludedValues.contains(v)) uniqueLabels += v)

    if (uniqueLabels.isEmpty) return labelTile

    val labelValues = uniqueLabels.toArray

    // Use a 16-bit celltype for kernel/mask/count tiles: max possible vote count is
    // kernelSize*kernelSize (<= 625 for the allowed range), which comfortably fits a Short,
    // at half the memory footprint of Int.
    val kernelTile: Tile = ShortArrayTile(Array.fill[Short](kernelSize * kernelSize)(1), kernelSize, kernelSize)

    val countTiles: Array[Tile] = labelValues.map { labelValue =>
      val maskTile = labelTile.map(v => if (v == labelValue) 1 else 0).convert(ShortConstantNoDataCellType)
      OpenEOProcesses.convolveTile(maskTile, kernelTile)
    }

    val result = labelTile.mutable
    var row = 0
    while (row < rows) {
      var col = 0
      while (col < cols) {
        val original = labelTile.get(col, row)
        if (!isNoData(original) && !excludedValues.contains(original)) {
          var bestIdx = 0
          var bestCount = Int.MinValue
          var i = 0
          while (i < countTiles.length) {
            val count = countTiles(i).get(col, row)
            if (count > bestCount) {
              bestCount = count
              bestIdx = i
            }
            i += 1
          }
          result.set(col, row, labelValues(bestIdx))
        }
        col += 1
      }
      row += 1
    }
    result
  }
}
