package org.openeo.sar.backend.nativ

import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, FloatArrayTile, GridBounds, MultibandTile, Tile}
import geotrellis.vector.Extent
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.backend.nativ.rtc.AreaRtc
import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.{BackscatterNormalization, RtcAlgorithm, TerrainCorrectionProcessor, TileComputeContext}
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
    logger.debug(s"sar_backscatter: item ${ctx.request.sceneId}, ${ctx.request.extent} ${ctx.request.cellSize}")
    val req    = ctx.request
    val meta   = ctx.metadata
    val config = req.config
    val pols   = req.polarisations.toArray

    val (backscatter, ellipsInc, localInc, mask, shadowLayover) =
      TerrainCorrectionBackend.allocate(req.cols, req.rows, pols.length, config)

    // 1. DEM window for the output tile, ellipsoidal heights (metres).
    //    Read with a margin of 1 pixel, so the terrain normal of the pixels at the tile edge can be calculated too.
    val demB: Array[Array[Float]] = {
      val cell = req.cellSize
      val bufferedExtent = Extent(req.extent.xmin - cell.width, req.extent.ymin - cell.height,
        req.extent.xmax + cell.width, req.extent.ymax + cell.height)
      TerrainCorrectionProcessor.readDemEllipsoidal(ctx.copy(request = req.copy(extent = bufferedExtent)))
    }
    val dem: Array[Array[Float]] = Array.tabulate(req.rows)(r => java.util.Arrays.copyOfRange(demB(r + 1), 1, req.cols + 1))
    val methodBand: Option[FloatArrayTile] =
      if (config.rtcMethodBand) Some(FloatArrayTile.empty(req.cols, req.rows)) else None

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
      return TerrainCorrectionBackend.assemble(backscatter, ellipsInc, localInc, mask, shadowLayover, methodBand)

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
        throw new IllegalStateException(s"sar_backscatter: SAR window read failed for ${pol.code} of item ${ctx.request.sceneId}")
      ).tile.band(0)
    }
    val polMetas = pols.map(meta.polarisations)

    val numberOfLines  = meta.timing.numberOfLines
    val numberOfPixels = meta.timing.numberOfPixels

    // 6b. Choose the RTC algorithm per pixel. Angle-based unless area-based RTC is requested; for Auto, area-based
    //     only near pixels with a steep local incidence angle, and only computed at all for tiles that have some.
    val algorithm = if (doGamma0) config.rtcAlgorithm else RtcAlgorithm.Angle
    val isInScene: Int => Boolean = i => {
      val line = sarLine(i); val gr = sarGr(i)
      !java.lang.Double.isNaN(line) && line >= 0 && line < numberOfLines && gr >= 0 && gr < numberOfPixels
    }

    val layoverPre = new Array[Boolean](if (algorithm == RtcAlgorithm.Auto) rows * cols else 0)
    val thetaLocPre: Array[Double] =
      if (algorithm == RtcAlgorithm.Auto) {
        val t = new Array[Double](rows * cols)
        java.util.Arrays.fill(t, Double.NaN)
        Array.range(0, rows).par.foreach { r =>
          var c = 0
          while (c < cols) {
            val i = idx(r, c)
            if (isInScene(i)) {
              val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
              val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
              val normal = terrainSurfaceNormal(demB, lonRadGrid, latRadGrid, r, c, req)
              t(i) = RangeDoppler.localIncidence(pGnd, pSat, normal)
              // layover is also "steep", even if the 3D angle looks moderate
              if (RangeDoppler.localIncidenceInRangePlane(pGnd, pSat, normal, Ecef.ellipsoidalNormal(pGnd)) < 0.0)
                layoverPre(i) = true
            }
            c += 1
          }
        }
        t
      } else null

    val useArea: Array[Boolean] = algorithm match {
      case RtcAlgorithm.BilinearDistribution =>
        Array.tabulate(rows * cols)(isInScene)
      case RtcAlgorithm.Auto =>
        val minRad = math.toRadians(config.rtcAutoMinLocalIncidenceDeg)
        val maxRad = math.toRadians(config.rtcAutoMaxLocalIncidenceDeg)
        val steep = Array.tabulate(rows * cols)(i => !java.lang.Double.isNaN(thetaLocPre(i)) &&
          (thetaLocPre(i) < minRad || thetaLocPre(i) > maxRad || layoverPre(i)))
        val inScene = Array.tabulate(rows * cols)(isInScene)
        val nScene = inScene.count(identity)
        val nSteep = steep.count(identity)
        // Tile-level decision first: a tile with a noticeable share of steep pixels is area-corrected as a whole
        // (no seams between methods, and the angle method is also biased on the moderate slopes of such tiles).
        if (nScene > 0 && nSteep.toDouble / nScene >= config.rtcAutoTileFraction) inScene
        else dilate(steep, rows, cols, config.rtcAutoDilationPixels)
      case _ => null
    }

    val anf: Option[AreaRtc.Anf] =
      if (useArea != null && useArea.exists(identity)) {
        val tStart = System.nanoTime()
        val result = computeAnf(ctx, sarLine, sarGr, pGndX, pGndY, pGndZ, pSatX, pSatY, pSatZ, isInScene, tSeed)
        logger.debug(s"sar_backscatter: item ${req.sceneId}, ${config.rtcAlgorithm.name} RTC took ${(System.nanoTime() - tStart) / 1000000} ms")
        Some(result)
      } else None
    val rtcMinValue = config.rtcMinValueDb.map(db => math.pow(10.0, db / 10.0)).getOrElse(0.0)

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
          var areaFactor = Double.NaN
          val areaPx = anf.isDefined && useArea(i)
          var sinEl = 1.0

          if (needTerrainCheck) {
            val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
            val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
            val ellipsoidNorm = Ecef.ellipsoidalNormal(pGnd)
            val thetaEl = RangeDoppler.localIncidence(pGnd, pSat, ellipsoidNorm)
            val terrainNorm = terrainSurfaceNormal(demB, lonRadGrid, latRadGrid, r, c, req)
            val thetaLoc = if (thetaLocPre != null) thetaLocPre(i) else RangeDoppler.localIncidence(pGnd, pSat, terrainNorm)
            sinEl = math.sin(thetaEl)

            if (config.localIncidenceAngle) localInc.get.setDouble(c, r, math.toDegrees(thetaLoc))
            if (config.ellipsoidIncidenceAngle) ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))

            // Layover: the terrain faces the sensor more steeply than the look direction (range plane).
            // Shadow: the terrain faces away from the sensor.
            val thetaLocRange = RangeDoppler.localIncidenceInRangePlane(pGnd, pSat, terrainNorm, ellipsoidNorm)
            isLayover = thetaLocRange < 0.0 || thetaEl > math.Pi / 2.0
            isShadow = !isLayover && thetaLoc >= math.Pi / 2.0

            if (doShadow) {
              shadowLayover.get.setDouble(c, r,
                if (isLayover) 1.0f
                else if (isShadow) 2.0f
                else 0.0f)
            }

            if (areaPx) {
              val grid = anf.get.grid
              val rSlant = (pSat - pGnd).norm
              areaFactor = 1.0 / anf.get.sample(line - grid.azLine0, (rSlant - grid.rMin) / grid.dr, rtcMinValue)
            } else if (doGamma0) rtcFactor = TerrainCorrectionBackend.gamma0RtcFactor(thetaEl, thetaLoc)
          } else if (config.ellipsoidIncidenceAngle) {
            val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
            val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
            val ellipsoidNorm = Ecef.ellipsoidalNormal(pGnd)
            val thetaEl = RangeDoppler.localIncidence(pGnd, pSat, ellipsoidNorm)
            ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))
          }

          // The area-based algorithms handle layover and shadow themselves (ANF sums the facets; NaN in shadow).
          // Backscatter is only masked when the terrain classification was asked for, or for the angle-based gamma0.
          val maskInvalid = (doGamma0 || doShadow) && !areaPx
          if (!(maskInvalid && (isLayover || isShadow))) {
            var p = 0
            while (p < pols.length) {
              val polMeta = polMetas(p)
              val dn = bilinear(sarWindows(p), winPx, winLine)
              if (!java.lang.Double.isNaN(dn)) {
                val sigmaLut = polMeta.sigmaLut(line, gr)
                val noiseLut = polMeta.noiseLut(line, gr)
                val num = Math.fma(dn, dn, -noiseLut)
                val sigma0 = if (sigmaLut > 0) num / (sigmaLut * sigmaLut) else Float.NaN
                if (areaPx) {
                  // gamma0 = beta0 / ANF; without a beta0 LUT, beta0 = sigma0_ellipsoid / sin(theta_el)
                  val beta0 = polMeta.betaLut match {
                    case Some(lut) => val b = lut(line, gr); if (b > 0) num / (b * b) else Float.NaN
                    case None      => sigma0 / sinEl
                  }
                  backscatter(p).setDouble(c, r, beta0 * areaFactor)
                } else backscatter(p).setDouble(c, r, sigma0 * rtcFactor)
              }
              p += 1
            }
            mask.setDouble(c, r, 1.0)
          }
          methodBand.foreach(_.setDouble(c, r, if (areaPx) 1.0 else 0.0))
        }
        c += 1
      }
    }

    TerrainCorrectionBackend.assemble(backscatter, ellipsInc, localInc, mask, shadowLayover, methodBand)
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Dilates a boolean raster with a (2d+1)² square. */
  private def dilate(mask: Array[Boolean], rows: Int, cols: Int, d: Int): Array[Boolean] = {
    if (d <= 0) return mask
    val horizontal = new Array[Boolean](mask.length)
    for (r <- 0 until rows; c <- 0 until cols)
      horizontal(r * cols + c) = (math.max(0, c - d) to math.min(cols - 1, c + d)).exists(cc => mask(r * cols + cc))
    val out = new Array[Boolean](mask.length)
    for (r <- 0 until rows; c <- 0 until cols)
      out(r * cols + c) = (math.max(0, r - d) to math.min(rows - 1, r + d)).exists(rr => horizontal(rr * cols + c))
    out
  }

  /** Area normalisation factor of the tile in a virtual radar grid (azimuth line x slant range) that covers the
   *  tile's output pixels, from a DEM that is upsampled and buffered by the margins of the RTC config. */
  private def computeAnf(ctx: TileComputeContext,
                         sarLine: Array[Double], sarGr: Array[Double],
                         pGndX: Array[Double], pGndY: Array[Double], pGndZ: Array[Double],
                         pSatX: Array[Double], pSatY: Array[Double], pSatZ: Array[Double],
                         isInScene: Int => Boolean, tSeed: Double): AreaRtc.Anf = {
    val req = ctx.request
    val meta = ctx.metadata
    val config = req.config

    var minAz = Double.MaxValue; var maxAz = Double.MinValue
    var minR = Double.MaxValue; var maxR = Double.MinValue
    var iRef = -1
    var i = 0
    while (i < sarLine.length) {
      if (isInScene(i)) {
        val rSlant = (Vec3(pSatX(i), pSatY(i), pSatZ(i)) - Vec3(pGndX(i), pGndY(i), pGndZ(i))).norm
        minAz = math.min(minAz, sarLine(i)); maxAz = math.max(maxAz, sarLine(i))
        minR = math.min(minR, rSlant); maxR = math.max(maxR, rSlant)
        iRef = i
      }
      i += 1
    }

    // Slant range spacing from the ground range spacing, at a reference pixel.
    val srgr = meta.polarisations(req.polarisations.head).srgr.at(sarLine(iRef) * meta.timing.lineTimeInterval)
    val grMetres = sarGr(iRef) * meta.timing.rangePixelSpacing
    val dr = meta.timing.rangePixelSpacing * srgr.dSlantRange_dGround(grMetres)

    val pad = 3
    val azLine0 = math.floor(minAz) - pad
    val nAz = (math.ceil(maxAz) + pad - azLine0).toInt + 1
    val rMin = minR - pad * dr
    val nRg = ((maxR - rMin) / dr).toInt + pad + 1
    val grid = AreaRtc.RadarGrid(azLine0, nAz, rMin, dr, nRg, meta.timing.lineTimeInterval)
    val MaxGridCells = 16L * 1024 * 1024
    if (grid.size > MaxGridCells)
      throw new IllegalStateException(s"sar_backscatter: area-based RTC for item ${req.sceneId}, ${req.extent}: radar grid of $nAz x $nRg is too large, use smaller tiles")

    val u = config.rtcDemUpsampling
    val mx = config.rtcDemMarginPixels * req.cellSize.width
    val my = config.rtcDemMarginPixels * req.cellSize.height
    val fineExtent = Extent(req.extent.xmin - mx, req.extent.ymin - my, req.extent.xmax + mx, req.extent.ymax + my)
    val fineCell = CellSize(req.cellSize.width / u, req.cellSize.height / u)
    val fineCtx = ctx.copy(request = req.copy(extent = fineExtent, cellSize = fineCell))
    val fineDem = TerrainCorrectionProcessor.readDemEllipsoidal(fineCtx)

    val transform = ThreadLocal.withInitial[(Double, Double) => (Double, Double)](
      () => geotrellis.proj4.Transform(req.crs, LatLng))
    val lonLatRad = (row: Int, col: Int) => {
      val x = fineExtent.xmin + (col + 0.5) * fineCell.width
      val y = fineExtent.ymax - (row + 0.5) * fineCell.height
      val (lon, lat) = transform.get()(x, y)
      (math.toRadians(lon), math.toRadians(lat))
    }

    AreaRtc.computeRtcBilinearDistribution(fineDem, lonLatRad, grid, meta.orbit, tSeed)
  }

  /** Outward terrain surface normal at grid position (col, row) in ECEF,
   *  derived from centred finite differences of the DEM heights.
   *  `demB` is the DEM with a margin of 1 pixel around the tile, so this also works at the tile edge.
   *  Falls back to the ellipsoidal normal where a neighbouring height is unknown. */
  private def terrainSurfaceNormal(demB: Array[Array[Float]],
                                   lonRadGrid: Array[Double], latRadGrid: Array[Double],
                                   row: Int, col: Int, req: org.openeo.sar.TileRequest): Vec3 = {
    val rows = req.rows; val cols = req.cols
    def point(r: Int, c: Int): Vec3 = {
      val h = demB(r + 1)(c + 1)
      if (java.lang.Float.isNaN(h)) return null
      val (lon, lat) =
        if (r >= 0 && r < rows && c >= 0 && c < cols) { val i = r * cols + c; (lonRadGrid(i), latRadGrid(i)) }
        else TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
      Ecef.fromGeodetic(lon, lat, h)
    }

    val pE = point(row, col + 1); val pW = point(row, col - 1)
    val pN = point(row - 1, col); val pS = point(row + 1, col)
    if (pE == null || pW == null || pN == null || pS == null) {
      val (lon, lat) = TerrainCorrectionProcessor.pixelToLonLatRad(col, row, req)
      return Ecef.ellipsoidalNormal(Ecef.fromGeodetic(lon, lat, 0.0))
    }

    // Two tangent vectors spanning the local surface patch.
    val east  = pE - pW   // column direction (×2 spacing, direction only)
    val north = pN - pS   // row direction (×2 spacing, pointing "up" in image)

    // East × north = up: an outward-pointing normal (right-handed East-North-Up frame).
    val n = east.cross(north)
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
