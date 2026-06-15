package org.openeo.sar.backend.nativ

import geotrellis.raster.{GridBounds, MultibandTile, Tile}
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.metadata.Polarisation
import org.openeo.sar.{TerrainCorrectionProcessor, TileComputeContext}

/** Pure-Scala terrain correction. Operates on already-assembled inputs in
 *  `TileComputeContext`. */
final class NativeBackend extends TerrainCorrectionBackend {
  override val name = "native"

  override def compute(ctx: TileComputeContext): MultibandTile = {
    val req = ctx.request
    val meta = ctx.metadata
    val pols = req.polarisations.toArray

    val (sigmas, inc, mask) = TerrainCorrectionBackend.allocate(req.cols, req.rows, pols.length)

    // 1. DEM window for the output tile, ellipsoidal heights (metres).
    val dem: Array[Array[Double]] = TerrainCorrectionProcessor.readDemEllipsoidal(ctx)

    // 2. Pre-compute per-pixel (lon, lat) on the output grid.
    val lonLat: Array[Array[(Double, Double)]] = Array.tabulate(req.rows, req.cols) {
      (r, c) => TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
    }

    // 3. Seed for the zero-Doppler iteration: scene-centre azimuth time.
    val tSeed = 0.5 * meta.timing.numberOfLines * meta.timing.lineTimeInterval

    // 4. First pass: forward-geocode every output pixel to SAR (line, groundRangePx)
    //    and remember the bounding box so we issue ONE windowed read per polarisation.
    case class SarCoord(line: Double, gr: Double, rSlant: Double, pSat: Vec3, pGnd: Vec3)
    val sarCoords: Array[Array[SarCoord]] = Array.ofDim(req.rows, req.cols)
    var minLine = Int.MaxValue; var maxLine = Int.MinValue
    var minPx   = Int.MaxValue; var maxPx   = Int.MinValue
    var anyValid = false

    var r = 0
    while (r < req.rows) {
      var c = 0
      while (c < req.cols) {
        val h = dem(r)(c)
        if (!java.lang.Double.isNaN(h)) {
          val (lonRad, latRad) = lonLat(r)(c)
          val pGnd = Ecef.fromGeodetic(lonRad, latRad, h)
          val tAz  = RangeDoppler.zeroDopplerTime(pGnd, meta.orbit, tSeed)
          val pSat = meta.orbit.positionAt(tAz)
          val rSlant = (pSat - pGnd).norm
          val azLine = tAz / meta.timing.lineTimeInterval
          val srgr   = meta.polarisations(pols(0)).srgr.at(tAz)
          val grMetres = srgr.groundRangeFromSlant(rSlant, gSeed = math.max(0.0, rSlant - srgr.sr0))
          val grPx     = grMetres / meta.timing.rangePixelSpacing
          sarCoords(r)(c) = SarCoord(azLine, grPx, rSlant, pSat, pGnd)

          if (azLine >= 0 && azLine <  meta.timing.numberOfLines &&
              grPx   >= 0 && grPx   <  meta.timing.numberOfPixels) {
            anyValid = true
            val il = azLine.toInt; val ip = grPx.toInt
            if (il < minLine) minLine = il; if (il > maxLine) maxLine = il
            if (ip < minPx)   minPx   = ip; if (ip > maxPx)   maxPx   = ip
          }
        }
        c += 1
      }
      r += 1
    }

    if (!anyValid) return TerrainCorrectionBackend.assemble(sigmas, inc, mask)

    // 5. Pad window by 1 pixel for bilinear sampling, clip to scene.
    val winMinLine = math.max(0, minLine - 1)
    val winMinPx   = math.max(0, minPx   - 1)
    val winMaxLine = math.min(meta.timing.numberOfLines  - 1, maxLine + 1)
    val winMaxPx   = math.min(meta.timing.numberOfPixels - 1, maxPx   + 1)
    val winCols    = winMaxPx   - winMinPx   + 1
    val winRows    = winMaxLine - winMinLine + 1

    // 6. One windowed read per polarisation in SAR coords (Long-keyed GridBounds).
    val sarWindows: Map[Polarisation, Tile] = pols.map { pol =>
      val rs = ctx.sarSources(pol)
      val gb = GridBounds[Long](winMinPx.toLong, winMinLine.toLong, winMaxPx.toLong, winMaxLine.toLong)
      val tile = rs.read(gb).getOrElse(
        throw new IllegalStateException(s"SAR window read failed for ${pol.code}")
      ).tile.band(0)
      pol -> tile
    }.toMap

    // 7. Second pass: sample, calibrate, fill incidence + mask.
    r = 0
    while (r < req.rows) {
      var c = 0
      while (c < req.cols) {
        val sc = sarCoords(r)(c)
        if (sc != null &&
            sc.line >= 0 && sc.line < meta.timing.numberOfLines &&
            sc.gr   >= 0 && sc.gr   < meta.timing.numberOfPixels) {

          val winLine = sc.line - winMinLine
          val winPx   = sc.gr   - winMinPx

          var p = 0
          while (p < pols.length) {
            val pol = pols(p)
            val polMeta = meta.polarisations(pol)
            val dn = bilinear(sarWindows(pol), winPx, winLine)
            if (!java.lang.Double.isNaN(dn)) {
              val sigmaLut = polMeta.sigmaLut(sc.line, sc.gr)
              val noiseLut = polMeta.noiseLut(sc.line, sc.gr)
              val num = Math.fma( dn, dn, - noiseLut)
              val sigma0 = if (sigmaLut > 0) num / (sigmaLut * sigmaLut) else Float.NaN
              sigmas(p).setDouble(c, r, sigma0)
            }
            p += 1
          }

          val normal = Ecef.ellipsoidalNormal(sc.pGnd)
          val incRad = RangeDoppler.localIncidence(sc.pGnd, sc.pSat, normal)
          inc.setDouble(c, r, math.toDegrees(incRad))
          mask.setDouble(c, r, 1.0)
        }
        c += 1
      }
      r += 1
    }

    TerrainCorrectionBackend.assemble(sigmas, inc, mask)
  }

  /** Bilinear DN sample at fractional (col, row) on a `Tile`. Returns NaN
   *  if the four neighbours fall outside the tile. */
  private def bilinear(tile: Tile, col: Double, row: Double): Double = {
    val c0 = math.floor(col).toInt; val c1 = c0 + 1
    val r0 = math.floor(row).toInt; val r1 = r0 + 1
    if (c0 < 0 || r0 < 0 || c1 >= tile.cols || r1 >= tile.rows) return Double.NaN
    val tx = col - c0; val ty = row - r0
    val v00 = tile.getDouble(c0, r0); val v10 = tile.getDouble(c1, r0)
    val v01 = tile.getDouble(c0, r1); val v11 = tile.getDouble(c1, r1)
    val a = v00 + (v10 - v00) * tx
    val b = v01 + (v11 - v01) * tx
    a + (b - a) * ty
  }
}
