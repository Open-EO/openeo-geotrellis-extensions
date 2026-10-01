package org.openeo.sar.backend

import geotrellis.raster.{FloatArrayTile, MultibandTile, Tile}
import org.openeo.sar.{SarProcessingConfig, TileComputeContext}

/** A backend computes one terrain-corrected, calibrated output tile.
 *
 *  Output band layout (all Float32, NaN outside swath unless noted). The
 *  angle bands and shadow/layover band are only present when requested via
 *  [[SarProcessingConfig]]; when omitted, backends should skip the
 *  associated computation entirely (see [[SarProcessingConfig]] docs):
 *
 *    0 .. nPols-1        backscatter per polarisation (linear power):
 *                          • sigma0  if config.normalization == Sigma0
 *                          • gamma0_RTC  if config.normalization == Gamma0RTC
 *                            = sigma0 × sin(θ_ellipsoidal) / sin(θ_local)
 *    [nPols]             ellipsoidal incidence angle in degrees (only present when
 *                        config.ellipsoidIncidenceAngle)
 *    [nPols+..]          local (terrain-relative) incidence angle in degrees (only present
 *                        when config.localIncidenceAngle)
 *    nPols+..            validity mask  (1.0 = valid, 0.0 = outside swath)
 *    [nPols+..]          shadow/layover mask  (only present when config.shadowLayoverMask):
 *                          0.0 = valid illuminated pixel
 *                          1.0 = layover  (θ_local < 0, target in front of wavefront)
 *                          2.0 = shadow   (terrain facing away from radar)
 */
trait TerrainCorrectionBackend {
  def name: String
  def compute(ctx: TileComputeContext): MultibandTile
}

object TerrainCorrectionBackend {

  /** Allocate empty output tiles according to the processing config.
   *  Angle tiles are only allocated when the corresponding config flag is set,
   *  so backends can skip their computation entirely when not requested.
   *  Returns (backscatterBands, ellipsoidalInc, localInc, mask, shadowLayover). */
  def allocate(cols: Int, rows: Int, nPols: Int, config: SarProcessingConfig)
    : (Array[FloatArrayTile], Option[FloatArrayTile], Option[FloatArrayTile], FloatArrayTile, Option[FloatArrayTile]) = {
    val backscatter  = Array.fill(nPols)(FloatArrayTile.empty(cols, rows))
    val ellipsInc    = if (config.ellipsoidIncidenceAngle) Some(FloatArrayTile.empty(cols, rows)) else None
    val localInc     = if (config.localIncidenceAngle) Some(FloatArrayTile.empty(cols, rows)) else None
    val mask         = FloatArrayTile.empty(cols, rows)
    val shadowLayover = if (config.shadowLayoverMask) Some(FloatArrayTile.empty(cols, rows)) else None
    (backscatter, ellipsInc, localInc, mask, shadowLayover)
  }

  def assemble(backscatter: Array[FloatArrayTile],
               ellipsInc: Option[FloatArrayTile],
               localInc: Option[FloatArrayTile],
               mask: FloatArrayTile,
               shadowLayover: Option[FloatArrayTile]): MultibandTile = {
    val bands: Seq[Tile] =
      backscatter.toSeq ++ ellipsInc.toSeq ++ localInc.toSeq ++ Seq(mask) ++ shadowLayover.toSeq
    MultibandTile(bands)
  }
}
