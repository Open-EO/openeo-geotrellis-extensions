package org.openeo.sar.backend.nativ

import geotrellis.raster.{GridBounds, MultibandTile, Tile}
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.metadata.Polarisation
import org.openeo.sar.{BackscatterNormalization, TerrainCorrectionProcessor, TileComputeContext}
import org.slf4j.{Logger, LoggerFactory}

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

    // 2. Pre-compute per-pixel (lon, lat) in radians on the output grid.
    val lonLat: Array[Array[(Double, Double)]] = Array.tabulate(req.rows, req.cols) {
      (r, c) => TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
    }

    // 3. Seed for the zero-Doppler iteration: scene-centre azimuth time.
    val tSeed = 0.5 * meta.timing.numberOfLines * meta.timing.lineTimeInterval

    // 4. First pass: forward-geocode every output pixel to SAR (line, groundRangePx)
    //    and remember the bounding box so we issue ONE windowed read per polarisation.
    case class SarCoord(line: Double, gr: Double, pSat: Vec3, pGnd: Vec3)
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
          sarCoords(r)(c) = SarCoord(azLine, grPx, pSat, pGnd)

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

    if (!anyValid)
      return TerrainCorrectionBackend.assemble(backscatter, ellipsInc, localInc, mask, shadowLayover)

    // 5. Pad window by 1 pixel for bilinear sampling, clip to scene.
    val winMinLine = math.max(0, minLine - 1)
    val winMinPx   = math.max(0, minPx   - 1)
    val winMaxLine = math.min(meta.timing.numberOfLines  - 1, maxLine + 1)
    val winMaxPx   = math.min(meta.timing.numberOfPixels - 1, maxPx   + 1)

    // 6. One windowed read per polarisation in SAR coords.
    val sarWindows: Map[Polarisation, Tile] = pols.map { pol =>
      val gb = GridBounds[Long](winMinPx.toLong, winMinLine.toLong, winMaxPx.toLong, winMaxLine.toLong)
      val tile = ctx.sarSources(pol).read(gb).getOrElse(
        throw new IllegalStateException(s"SAR window read failed for ${pol.code}")
      ).tile.band(0)
      pol -> tile
    }.toMap

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

    // 7. Second pass: sample, calibrate, fill angles + mask bands.
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

          var isLayover = false
          var isShadow  = false
          var rtcFactor = 1.0

          if (needTerrainCheck) {
            // ---- Angles -------------------------------------------------------
            // Ellipsoidal incidence angle: look vs. smooth WGS84 ellipsoid normal.
            // Needed here (regardless of the ellipsoidIncidenceAngle flag) because
            // the shadow/layover test and the gamma0 RTC factor both depend on it.
            val ellipsoidNorm = Ecef.ellipsoidalNormal(sc.pGnd)
            val thetaEl       = RangeDoppler.localIncidence(sc.pGnd, sc.pSat, ellipsoidNorm)

            // Terrain surface normal via finite differences on the DEM/lonLat arrays.
            val terrainNorm = terrainSurfaceNormal(dem, lonLat, r, c, req.rows, req.cols)
            val thetaLoc    = RangeDoppler.localIncidence(sc.pGnd, sc.pSat, terrainNorm)

            if (config.localIncidenceAngle) localInc.get.setDouble(c, r, math.toDegrees(thetaLoc))
            if (config.ellipsoidIncidenceAngle) ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))

            // ---- Shadow / layover classification ------------------------------
            // Layover: happens when the ground point is geometrically "ahead" of
            // the satellite wavefront, indicated by negative slant-range cosine
            // w.r.t. the flight direction. Practical proxy: θ_local < 0 (masked
            // out by the geometry) — we detect it via thetaEl > thetaLoc (ground
            // is steeper than look angle).
            isLayover = thetaLoc < 0.0 || thetaEl > math.Pi / 2.0
            isShadow  = thetaLoc > math.Pi / 2.0

            if (doShadow) {
              shadowLayover.get.setDouble(c, r,
                if (isLayover) 1.0f
                else if (isShadow) 2.0f
                else 0.0f)
            }

            // RTC factor (angle-ratio method).  Clamp sin(θ_local) to avoid
            // divide-by-zero near 0° or 180° grazing.
            if (doGamma0) {
              val sinLocal = math.sin(thetaLoc)
              rtcFactor =
                if (sinLocal > 0.01) math.sin(thetaEl) / sinLocal
                else Double.NaN  // grazing — mark invalid
            }
          } else if (config.ellipsoidIncidenceAngle) {
            // Ellipsoidal incidence angle requested on its own: cheap (no DEM
            // finite-difference terrain normal needed), so compute it directly
            // without the full terrain check above.
            val ellipsoidNorm = Ecef.ellipsoidalNormal(sc.pGnd)
            val thetaEl       = RangeDoppler.localIncidence(sc.pGnd, sc.pSat, ellipsoidNorm)
            ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))
          }

          // ---- Backscatter --------------------------------------------------
          if (!isLayover && !isShadow) {
            var p = 0
            while (p < pols.length) {
              val polMeta = meta.polarisations(pols(p))
              val dn = bilinear(sarWindows(pols(p)), winPx, winLine)
              if (!java.lang.Double.isNaN(dn)) {
                val sigmaLut = polMeta.sigmaLut(sc.line, sc.gr)
                val noiseLut = polMeta.noiseLut(sc.line, sc.gr)
                val num      = Math.fma(dn, dn, -noiseLut)
                val sigma0   = if (sigmaLut > 0) num / (sigmaLut * sigmaLut) else Float.NaN
                backscatter(p).setDouble(c, r, sigma0 * rtcFactor)
              }
              p += 1
            }
            mask.setDouble(c, r, 1.0)
          }
          // shadow/layover pixels: backscatter stays NaN, mask stays 0
        }
        c += 1
      }
      r += 1
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
                                   lonLat: Array[Array[(Double, Double)]],
                                   row: Int, col: Int,
                                   rows: Int, cols: Int): Vec3 = {
    if (row == 0 || row == rows - 1 || col == 0 || col == cols - 1) {
      val (lon, lat) = lonLat(row)(col)
      return Ecef.ellipsoidalNormal(Ecef.fromGeodetic(lon, lat, dem(row)(col)))
    }

    val (lonE, latE) = lonLat(row)(col + 1); val pE = Ecef.fromGeodetic(lonE, latE, dem(row)(col + 1))
    val (lonW, latW) = lonLat(row)(col - 1); val pW = Ecef.fromGeodetic(lonW, latW, dem(row)(col - 1))
    val (lonN, latN) = lonLat(row - 1)(col); val pN = Ecef.fromGeodetic(lonN, latN, dem(row - 1)(col))
    val (lonS, latS) = lonLat(row + 1)(col); val pS = Ecef.fromGeodetic(lonS, latS, dem(row + 1)(col))

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
