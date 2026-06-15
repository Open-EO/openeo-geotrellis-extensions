package org.openeo.sar.backend

import geotrellis.raster.{FloatArrayTile, MultibandTile, Tile}
import org.openeo.sar.{SarProcessingConfig, TileComputeContext}

/** A backend computes one terrain-corrected, calibrated output tile.
 *
 *  Output band layout (all Float32, NaN outside swath unless noted):
 *
 *    0 .. nPols-1   backscatter per polarisation (linear power):
 *                     • sigma0  if config.normalization == Sigma0
 *                     • gamma0_RTC  if config.normalization == Gamma0RTC
 *                       = sigma0 × sin(θ_ellipsoidal) / sin(θ_local)
 *    nPols          ellipsoidal incidence angle in degrees (look angle vs. ellipsoid)
 *    nPols+1        local (terrain-relative) incidence angle in degrees
 *    nPols+2        validity mask  (1.0 = valid, 0.0 = outside swath)
 *    nPols+3        shadow/layover mask  (only present when config.shadowLayoverMask):
 *                     0.0 = valid illuminated pixel
 *                     1.0 = layover  (θ_local < 0, target in front of wavefront)
 *                     2.0 = shadow   (terrain facing away from radar)
 */
trait TerrainCorrectionBackend {
  def name: String
  def compute(ctx: TileComputeContext): MultibandTile
}

object TerrainCorrectionBackend {

  /** Allocate empty output tiles according to the processing config.
   *  Returns (backscatterBands, ellipsoidalInc, localInc, mask, shadowLayover). */
  def allocate(cols: Int, rows: Int, nPols: Int, config: SarProcessingConfig)
    : (Array[FloatArrayTile], FloatArrayTile, FloatArrayTile, FloatArrayTile, Option[FloatArrayTile]) = {
    val backscatter  = Array.fill(nPols)(FloatArrayTile.empty(cols, rows))
    val ellipsInc    = FloatArrayTile.empty(cols, rows)
    val localInc     = FloatArrayTile.empty(cols, rows)
    val mask         = FloatArrayTile.empty(cols, rows)
    val shadowLayover = if (config.shadowLayoverMask) Some(FloatArrayTile.empty(cols, rows)) else None
    (backscatter, ellipsInc, localInc, mask, shadowLayover)
  }

  def assemble(backscatter: Array[FloatArrayTile],
               ellipsInc: FloatArrayTile,
               localInc: FloatArrayTile,
               mask: FloatArrayTile,
               shadowLayover: Option[FloatArrayTile]): MultibandTile = {
    val bands: Seq[Tile] =
      backscatter.toSeq ++ Seq(ellipsInc, localInc, mask) ++ shadowLayover.toSeq
    MultibandTile(bands)
  }
}
