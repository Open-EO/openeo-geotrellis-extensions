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
 *                            = sigma0 × tan(θ_local) / sin(θ_ellipsoidal)
 *                            (see [[TerrainCorrectionBackend.gamma0RtcFactor]])
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

  /** Factor converting ellipsoid-referenced sigma0 into terrain-flattened gamma0:
   *    β0 = σ0_ell / sin θ_el
   *    γ0 = β0 · A_β / A_γ ≈ β0 · tan θ_loc   (A_γ = facet area projected ⊥ look direction)
   *  so γ0 = σ0_ell · tan θ_loc / sin θ_el. Over flat terrain this reduces to σ0 / cos θ.
   *
   *  The angle-based method of: A. Vollrath, A. Mullissa, J. Reiche, "Angular-Based Radiometric Slope Correction for
   *  Sentinel-1 on Google Earth Engine", Remote Sensing 12(11):1867, 2020, doi:10.3390/rs12111867; after
   *  L.M.H. Ulander, IEEE TGRS 34(5):1115-1122, 1996, doi:10.1109/36.536527 and
   *  D.H. Hoekman, J. Reiche, RSE 156:1-10, 2015, doi:10.1016/j.rse.2014.08.037.
   *  For steep terrain see [[org.openeo.sar.RtcAlgorithm.BilinearDistribution]].
   *  Angles in radians; returns NaN for degenerate geometry. */
  def gamma0RtcFactor(thetaEl: Double, thetaLoc: Double): Double = {
    val sinEl = math.sin(thetaEl)
    val cosLoc = math.cos(thetaLoc)
    if (sinEl > 0.01 && cosLoc > 0.01) math.sin(thetaLoc) / (cosLoc * sinEl)
    else Double.NaN
  }

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
               shadowLayover: Option[FloatArrayTile],
               rtcMethod: Option[FloatArrayTile] = None): MultibandTile = {
    val bands: Seq[Tile] =
      backscatter.toSeq ++ ellipsInc.toSeq ++ localInc.toSeq ++ Seq(mask) ++ shadowLayover.toSeq ++ rtcMethod.toSeq
    MultibandTile(bands)
  }
}
