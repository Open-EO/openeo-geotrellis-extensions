package org.openeo.sar.backend

import geotrellis.raster.{FloatArrayTile, MultibandTile, Tile}
import org.openeo.sar.TileComputeContext

/** A backend computes one terrain-corrected, calibrated output tile.
 *
 *  Output layout (all Float32, NaN/0 outside swath):
 *    band 0..N-1:  sigma0 per requested polarisation, in linear power
 *    band N:       local incidence angle in degrees
 *    band N+1:     validity mask (1.0 valid / 0.0 invalid) */
trait TerrainCorrectionBackend {
  def name: String
  def compute(ctx: TileComputeContext): MultibandTile
}

object TerrainCorrectionBackend {
  /** Helper for backends: empty Float32 output tiles in the right layout. */
  def allocate(cols: Int, rows: Int, nPols: Int): (Array[FloatArrayTile], FloatArrayTile, FloatArrayTile) = {
    val sigmas = Array.fill(nPols)(FloatArrayTile.empty(cols, rows))
    val inc    = FloatArrayTile.empty(cols, rows)
    val mask   = FloatArrayTile.empty(cols, rows)
    (sigmas, inc, mask)
  }

  def assemble(sigmas: Array[FloatArrayTile], inc: FloatArrayTile, mask: FloatArrayTile): MultibandTile = {
    val bands: Array[Tile] = sigmas.asInstanceOf[Array[Tile]] :+ inc.asInstanceOf[Tile] :+ mask.asInstanceOf[Tile]
    MultibandTile(bands.toIndexedSeq)
  }
}
