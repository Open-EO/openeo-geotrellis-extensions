package org.openeo.sar.backend.nativ

import geotrellis.raster.{GridBounds, MultibandTile, Tile}
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.{BackscatterNormalization, TerrainCorrectionProcessor, TileComputeContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._

object NativeBackend {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[NativeBackend])
}
/** Pure-Scala terrain correction backend.
 *
 *  Computes sigma0 or gamma0_RTC backscatter with range-Doppler orthorectification.
 *  Output band layout is determined by [[org.openeo.sar.SarProcessingConfig]] carried
 *  on the [[TileComputeContext]]; see [[TerrainCorrectionBackend]] for the full
 *  band index documentation. */
final class NativeBackend extends TerrainCorrectionBackend {

  import NativeBackend._

  override val name = "native"

  override def compute(ctx: TileComputeContext): MultibandTile = {
    logger.debug(s"sar_backscatter ${ctx.request.extent} ${ctx.request.cellSize}")
    val req    = ctx.request
    val meta   = ctx.metadata
    val config = req.config
    val pols   = req.polarisations.toArray

    val (backscatter, ellipsInc, localInc, mask, shadowLayover) =
      TerrainCorrectionBackend.allocate(req.cols, req.rows, pols.length, config)

    // 1. DEM window for the output tile, ellipsoidal heights (metres).
    val dem: Array[Array[Double]] = TerrainCorrectionProcessor.readDemEllipsoidal(ctx)

    val doGamma0 = config.normalization == BackscatterNormalization.Gamma0RTC
    val doShadow = config.shadowLayoverMask

    // Whether we need the (expensive) terrain surface normal / local incidence
    // angle at all: required for gamma0 RTC flattening, for the shadow/layover
    // classification (which also gates backscatter validity), or when the
    // caller explicitly asked for the local incidence angle band. When none of
    // these apply (e.g. plain sigma0, or sigma0 + ellipsoidal angle only), skip
    // the terrain normal computation and the shadow/layover geometry test
    // entirely — every in-swath pixel is then considered valid.
    val needTerrainCheck = doGamma0 || doShadow || config.localIncidenceAngle
    // Whether the satellite/ground ECEF positions need to be retained after
    // pass 1 at all: only the terrain-normal path and the ellipsoid incidence
    // angle band consume them. Plain sigma0 only ever needs (line, gr).
    val needGeometry = needTerrainCheck || config.ellipsoidIncidenceAngle

    val cols = req.cols
    val rows = req.rows
    def idx(r: Int, c: Int): Int = r * cols + c

    // 2. Pre-compute per-pixel (lon, lat) in radians on the output grid, but
    //    only keep the full grid around when the terrain-normal computation
    //    needs to sample neighbouring pixels; otherwise it's a per-pixel local
    //    in pass 1.
    val lonRadGrid, latRadGrid: Array[Double] =
      if (needTerrainCheck) new Array[Double](rows * cols) else null

    // 3. Seed for the zero-Doppler iteration: scene-centre azimuth time.
    val tSeed = 0.5 * meta.timing.numberOfLines * meta.timing.lineTimeInterval

    // 4. First pass: forward-geocode every output pixel to SAR (line, groundRangePx)
    //    and remember the bounding box so we issue ONE windowed read per polarisation.
    //    Flat primitive arrays (rather than a per-pixel case class) avoid one
    //    object allocation and a level of pointer-chasing per pixel; the
    //    satellite/ground ECEF positions are only materialised when needed.
    val sarLine  = new Array[Double](rows * cols)
    val sarGr    = new Array[Double](rows * cols)
    val pGndX, pGndY, pGndZ: Array[Double] = if (needGeometry) new Array[Double](rows * cols) else null
    val pSatX, pSatY, pSatZ: Array[Double] = if (needGeometry) new Array[Double](rows * cols) else null
    java.util.Arrays.fill(sarLine, Double.NaN)

    case class RowScan(minLine: Int, maxLine: Int, minPx: Int, maxPx: Int, anyValid: Boolean)

    val rowScans = Array.range(0, req.rows).par.map { r =>
      var minLine = Int.MaxValue; var maxLine = Int.MinValue
      var minPx = Int.MaxValue; var maxPx = Int.MinValue
      var anyValid = false

      // Warm-start Newton's method from the previous valid pixel's converged
      // azimuth time: adjacent columns land within a fraction of a line of
      // each other, so after the first pixel in a row this typically
      // collapses the zero-Doppler solve from several iterations to one or
      // two, without changing the converged result.
      var tSeedRow = tSeed

      var c = 0
      while (c < cols) {
        val h = dem(r)(c)
        // lon/lat depends only on the output grid position, not on DEM
        // validity: the terrain-normal finite-difference stencil samples
        // neighbours regardless of whether *this* pixel's own DEM height is
        // valid, so it must be available for every pixel, not just valid ones.
        if (needTerrainCheck) {
          val (lonRad, latRad) = TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
          val i0 = idx(r, c)
          lonRadGrid(i0) = lonRad
          latRadGrid(i0) = latRad
        }
        if (!java.lang.Double.isNaN(h)) {
          val (lonRad, latRad) =
            if (needTerrainCheck) (lonRadGrid(idx(r, c)), latRadGrid(idx(r, c)))
            else TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
          val pGnd = Ecef.fromGeodetic(lonRad, latRad, h)
          val tAz  = RangeDoppler.zeroDopplerTime(pGnd, meta.orbit, tSeedRow)
          tSeedRow = tAz
          val pSat = meta.orbit.positionAt(tAz)
          val rSlant = (pSat - pGnd).norm
          val azLine = tAz / meta.timing.lineTimeInterval
          val srgr   = meta.polarisations(pols(0)).srgr.at(tAz)
          val grMetres = srgr.groundRangeFromSlant(rSlant, gSeed = math.max(0.0, rSlant - srgr.sr0))
          val grPx     = grMetres / meta.timing.rangePixelSpacing

          val i = idx(r, c)
          sarLine(i) = azLine
          sarGr(i)   = grPx
          if (needGeometry) {
            pGndX(i) = pGnd.x; pGndY(i) = pGnd.y; pGndZ(i) = pGnd.z
            pSatX(i) = pSat.x; pSatY(i) = pSat.y; pSatZ(i) = pSat.z
          }

          if (azLine >= 0 && azLine < meta.timing.numberOfLines &&
              grPx >= 0 && grPx < meta.timing.numberOfPixels) {
            anyValid = true
            val il = azLine.toInt; val ip = grPx.toInt
            if (il < minLine) minLine = il; if (il > maxLine) maxLine = il
            if (ip < minPx) minPx = ip; if (ip > maxPx) maxPx = ip
          }
        }
        c += 1
      }

      RowScan(minLine, maxLine, minPx, maxPx, anyValid)
    }.seq

    val aggregate = rowScans.foldLeft(RowScan(Int.MaxValue, Int.MinValue, Int.MaxValue, Int.MinValue, false)) {
      case (acc, rowScan) =>
        if (!rowScan.anyValid) acc
        else RowScan(
          minLine = math.min(acc.minLine, rowScan.minLine),
          maxLine = math.max(acc.maxLine, rowScan.maxLine),
          minPx = math.min(acc.minPx, rowScan.minPx),
          maxPx = math.max(acc.maxPx, rowScan.maxPx),
          anyValid = true
        )
    }

    if (!aggregate.anyValid)
      return TerrainCorrectionBackend.assemble(backscatter, ellipsInc, localInc, mask, shadowLayover)

    val minLine = aggregate.minLine; val maxLine = aggregate.maxLine
    val minPx = aggregate.minPx; val maxPx = aggregate.maxPx

    // 5. Pad window by 1 pixel for bilinear sampling, clip to scene.
    val winMinLine = math.max(0, minLine - 1)
    val winMinPx   = math.max(0, minPx   - 1)
    val winMaxLine = math.min(meta.timing.numberOfLines  - 1, maxLine + 1)
    val winMaxPx   = math.min(meta.timing.numberOfPixels - 1, maxPx   + 1)

    // 6. One windowed read per polarisation in SAR coords. Resolved to arrays
    //    indexed by polarisation position so pass 2 avoids a map lookup per
    //    pixel per polarisation.
    val gb = GridBounds[Long](winMinPx.toLong, winMinLine.toLong, winMaxPx.toLong, winMaxLine.toLong)
    val sarWindows: Array[Tile] = pols.map { pol =>
      ctx.sarSources(pol).read(gb).getOrElse(
        throw new IllegalStateException(s"SAR window read failed for ${pol.code}")
      ).tile.band(0)
    }
    val polMetas = pols.map(meta.polarisations)

    val numberOfLines  = meta.timing.numberOfLines
    val numberOfPixels = meta.timing.numberOfPixels

    // 7. Second pass: sample, calibrate, fill angles + mask bands.
    Array.range(0, rows).par.foreach { r =>
      var c = 0
      while (c < cols) {
        val i = idx(r, c)
        val line = sarLine(i)
        val gr   = sarGr(i)
        if (!java.lang.Double.isNaN(line) &&
            line >= 0 && line < numberOfLines &&
            gr   >= 0 && gr   < numberOfPixels) {

          val winLine = line - winMinLine
          val winPx   = gr   - winMinPx

          var isLayover = false
          var isShadow  = false
          var rtcFactor = 1.0

          if (needTerrainCheck) {
            val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
            val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
            val ellipsoidNorm = Ecef.ellipsoidalNormal(pGnd)
            val thetaEl = RangeDoppler.localIncidence(pGnd, pSat, ellipsoidNorm)
            val terrainNorm = terrainSurfaceNormal(dem, lonRadGrid, latRadGrid, r, c, rows, cols)
            val thetaLoc = RangeDoppler.localIncidence(pGnd, pSat, terrainNorm)

            if (config.localIncidenceAngle) localInc.get.setDouble(c, r, math.toDegrees(thetaLoc))
            if (config.ellipsoidIncidenceAngle) ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))

            isLayover = thetaLoc < 0.0 || thetaEl > math.Pi / 2.0
            isShadow = thetaLoc > math.Pi / 2.0

            if (doShadow) {
              shadowLayover.get.setDouble(c, r,
                if (isLayover) 1.0f
                else if (isShadow) 2.0f
                else 0.0f)
            }

            if (doGamma0) {
              val sinLocal = math.sin(thetaLoc)
              rtcFactor =
                if (sinLocal > 0.01) math.sin(thetaEl) / sinLocal
                else Double.NaN
            }
          } else if (config.ellipsoidIncidenceAngle) {
            val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
            val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
            val ellipsoidNorm = Ecef.ellipsoidalNormal(pGnd)
            val thetaEl = RangeDoppler.localIncidence(pGnd, pSat, ellipsoidNorm)
            ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))
          }

          if (!isLayover && !isShadow) {
            var p = 0
            while (p < pols.length) {
              val polMeta = polMetas(p)
              val dn = bilinear(sarWindows(p), winPx, winLine)
              if (!java.lang.Double.isNaN(dn)) {
                val sigmaLut = polMeta.sigmaLut(line, gr)
                val noiseLut = polMeta.noiseLut(line, gr)
                val num = Math.fma(dn, dn, -noiseLut)
                val sigma0 = if (sigmaLut > 0) num / (sigmaLut * sigmaLut) else Float.NaN
                backscatter(p).setDouble(c, r, sigma0 * rtcFactor)
              }
              p += 1
            }
            mask.setDouble(c, r, 1.0)
          }
        }
        c += 1
      }
    }

    TerrainCorrectionBackend.assemble(backscatter, ellipsInc, localInc, mask, shadowLayover)
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Outward terrain surface normal at grid position (col, row) in ECEF,
   *  derived from centred finite differences of the DEM heights.
   *  At boundary pixels falls back to the ellipsoidal normal. */
  private def terrainSurfaceNormal(dem: Array[Array[Double]],
                                   lonRadGrid: Array[Double], latRadGrid: Array[Double],
                                   row: Int, col: Int,
                                   rows: Int, cols: Int): Vec3 = {
    def at(r: Int, c: Int): (Double, Double) = {
      val i = r * cols + c
      (lonRadGrid(i), latRadGrid(i))
    }

    if (row == 0 || row == rows - 1 || col == 0 || col == cols - 1) {
      val (lon, lat) = at(row, col)
      return Ecef.ellipsoidalNormal(Ecef.fromGeodetic(lon, lat, dem(row)(col)))
    }

    val (lonE, latE) = at(row, col + 1); val pE = Ecef.fromGeodetic(lonE, latE, dem(row)(col + 1))
    val (lonW, latW) = at(row, col - 1); val pW = Ecef.fromGeodetic(lonW, latW, dem(row)(col - 1))
    val (lonN, latN) = at(row - 1, col); val pN = Ecef.fromGeodetic(lonN, latN, dem(row - 1)(col))
    val (lonS, latS) = at(row + 1, col); val pS = Ecef.fromGeodetic(lonS, latS, dem(row + 1)(col))

    // Two tangent vectors spanning the local surface patch.
    val east  = pE - pW   // column direction (×2 spacing, direction only)
    val north = pN - pS   // row direction (×2 spacing, pointing "up" in image)

    // Cross product: north × east gives an outward-pointing normal for a
    // right-handed East-North-Up coordinate frame on the WGS84 ellipsoid.
    val n = north.cross(east)
    if (n.norm < 1e-6) Ecef.ellipsoidalNormal(pE) else n.normalize
  }

  /** Bilinear DN sample at fractional (col, row) on a [[Tile]].
   *  Returns NaN if any neighbour falls outside the tile. */
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
