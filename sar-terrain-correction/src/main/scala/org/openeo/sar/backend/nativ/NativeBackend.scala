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

  /** Default Java heap budget for the working memory of one tile computation. Executors have about 1 GB of heap
   *  for working memory, which is also shared with the output tiles and the other tasks of the executor. */
  val DefaultMaxWorkingMemoryBytes: Long = 512L * 1024 * 1024

  /** Maximum number of partial ANF grids that are accumulated in parallel. */
  private val MaxAnfParallelism = 8

  /** Blocks are split on the memory estimate until they are at most this many pixels per side. */
  private val MinBlockSide = 128

  /** Cast shadow: maximum DEM margin towards the sensor, and the height by which terrain must rise above a line of
   *  sight to block it (avoids pixels shadowing themselves on slopes at grazing incidence). */
  private val MaxCastShadowMarginMetres = 10000.0
  private val CastShadowToleranceMetres = 0.01

  /** The radar grid of the area-based RTC of a tile exceeds the memory budget: the tile has to be split. */
  private final class RadarGridTooLargeException(message: String) extends IllegalStateException(message)

  /** A block of output pixels of a tile: offset and size. */
  private[nativ] final case class Block(col0: Int, row0: Int, cols: Int, rows: Int)

  /** Splits a tile of `cols` x `rows` pixels into a regular grid of near-equal blocks, as few as possible so that
   *  each block `fits`; blocks are split along their longest side first, until they are at most `minSide` pixels per
   *  side (so that the overhead per block, e.g. the DEM margins, stays limited). A single block if the tile fits. */
  private[nativ] def blocks(cols: Int, rows: Int, fits: (Int, Int) => Boolean, minSide: Int = 1): Seq[Block] = {
    var nx = 1; var ny = 1
    def blockCols = (cols + nx - 1) / nx
    def blockRows = (rows + ny - 1) / ny
    while (!fits(blockCols, blockRows) && (blockCols > minSide || blockRows > minSide)) {
      if (blockCols > minSide && (blockCols >= blockRows || blockRows <= minSide)) nx += 1 else ny += 1
    }
    val colBounds = (0 to nx).map(k => (k.toLong * cols / nx).toInt)
    val rowBounds = (0 to ny).map(k => (k.toLong * rows / ny).toInt)
    for {
      j <- 0 until ny; i <- 0 until nx
      if colBounds(i + 1) > colBounds(i) && rowBounds(j + 1) > rowBounds(j)
    } yield Block(colBounds(i), rowBounds(j), colBounds(i + 1) - colBounds(i), rowBounds(j + 1) - rowBounds(j))
  }
}
/** Pure-Scala terrain correction backend.
 *
 *  Computes sigma0 or gamma0_RTC backscatter with range-Doppler orthorectification.
 *  Output band layout is determined by [[org.openeo.sar.SarProcessingConfig]] carried
 *  on the [[TileComputeContext]]; see [[TerrainCorrectionBackend]] for the full
 *  band index documentation.
 *
 *  @param maxWorkingMemoryBytes Java heap budget for the working memory of one computation. Larger tiles are
 *                               computed in blocks (one after the other) that each stay within it, and assembled
 *                               into the requested tile. Nothing is cached between blocks or tiles. */
final class NativeBackend(maxWorkingMemoryBytes: Long) extends TerrainCorrectionBackend {

  import NativeBackend._

  def this() = this(NativeBackend.DefaultMaxWorkingMemoryBytes)

  require(maxWorkingMemoryBytes > 0, "sar_backscatter: the working memory budget must be positive")

  override val name = "native"

  override def compute(ctx: TileComputeContext): MultibandTile = {
    val req = ctx.request
    val estimate = estimatedWorkingMemory(ctx) _
    val tileBlocks = blocks(req.cols, req.rows, (cols, rows) => estimate(cols, rows) <= maxWorkingMemoryBytes, MinBlockSide)
    if (tileBlocks.size == 1) computeOrSplit(ctx)
    else {
      logger.info(s"sar_backscatter: item ${req.sceneId}, ${req.extent}: computing the ${req.cols} x ${req.rows} tile " +
        s"in ${tileBlocks.size} blocks of at most ${tileBlocks.map(_.cols).max} x ${tileBlocks.map(_.rows).max} pixels, " +
        s"to stay within ${maxWorkingMemoryBytes / (1024 * 1024)} MB of working memory")
      computeInBlocks(ctx, tileBlocks)
    }
  }

  private def computeOrSplit(ctx: TileComputeContext): MultibandTile = {
    val req = ctx.request
    try computeTile(ctx)
    catch {
      case e: RadarGridTooLargeException if req.cols > 1 || req.rows > 1 =>
        // The memory estimate was too optimistic for this tile.
        logger.info(s"sar_backscatter: item ${req.sceneId}, ${req.extent}: ${e.getMessage}, splitting the ${req.cols} x ${req.rows} tile")
        computeInBlocks(ctx, blocks(req.cols, req.rows, (cols, rows) => cols <= (req.cols + 1) / 2 && rows <= (req.rows + 1) / 2))
    }
  }

  /** Computes the blocks one after the other (each block is computed in parallel itself) and copies them into
   *  the output tile, so that only the working memory of one block is held at a time. */
  private def computeInBlocks(ctx: TileComputeContext, tileBlocks: Seq[Block]): MultibandTile = {
    val req = ctx.request
    val nBands = req.config.bandCount(req.polarisations.size)
    val out = Array.fill(nBands)(FloatArrayTile.empty(req.cols, req.rows))
    val cw = req.cellSize.width; val ch = req.cellSize.height
    for (block <- tileBlocks) {
      val blockExtent = Extent(
        req.extent.xmin + block.col0 * cw, req.extent.ymax - (block.row0 + block.rows) * ch,
        req.extent.xmin + (block.col0 + block.cols) * cw, req.extent.ymax - block.row0 * ch)
      val result = computeOrSplit(ctx.copy(request = req.copy(extent = blockExtent)))
      var b = 0
      while (b < nBands) { out(b).update(block.col0, block.row0, result.band(b)); b += 1 }
    }
    MultibandTile(out.toSeq)
  }

  /** Conservative estimate of the Java heap needed to compute a block of `cols` x `rows` pixels of the tile of
   *  `ctx`, including the output bands. */
  private def estimatedWorkingMemory(ctx: TileComputeContext)(cols: Int, rows: Int): Double = {
    val req = ctx.request
    val config = req.config
    val nPols = req.polarisations.size
    val doGamma0 = config.normalization == BackscatterNormalization.Gamma0RTC
    val needTerrainCheck = doGamma0 || config.shadowLayoverMask || config.localIncidenceAngle
    val needGeometry = needTerrainCheck || config.ellipsoidIncidenceAngle
    val algorithm = if (doGamma0) config.rtcAlgorithm else RtcAlgorithm.Angle
    val castShadow = config.castShadow && (doGamma0 || config.shadowLayoverMask)
    val pixels = cols.toDouble * rows

    val perPixel: Double =
      4.0 * config.bandCount(nPols) + // output bands
        24 +                          // DEM: buffered and unbuffered copies, plus the DEM and geoid tiles while reading
        16 +                          // SAR line / ground range per pixel
        (if (needTerrainCheck) 16 else 0) + // lon/lat
        (if (needGeometry) 48 else 0) +     // ground and satellite positions
        (if (algorithm == RtcAlgorithm.Auto || castShadow) 16 else 0) + // local incidence angles and the steep masks
        (if (algorithm != RtcAlgorithm.Angle) 1 else 0) +
        (if (castShadow) 1 + 24 else 0) // cast shadow mask, DEM with a margin towards the sensor (~3x the block)

    // Radar pixels per output pixel, with a margin for the rotation of the footprint w.r.t. the radar grid and for
    // terrain. Bounded by the scene for the SAR window reads.
    val radarPerPixel = 2.0 * radarPixelsPerOutputPixel(ctx)
    val sceneRadarPixels = ctx.metadata.timing.numberOfLines.toDouble * ctx.metadata.timing.numberOfPixels
    val sarWindow = 4.0 * nPols * math.min(sceneRadarPixels, radarPerPixel * pixels)

    val areaRtc: Double =
      if (algorithm == RtcAlgorithm.Angle) 0.0
      else {
        val u = config.rtcDemUpsampling.toDouble
        val m = 2.0 * config.rtcDemMarginPixels
        val fineDem = 24.0 * (cols + m) * u * (rows + m) * u
        val anfCells = radarPerPixel * (cols + 8.0) * (rows + 8.0)
        fineDem + 8.0 * anfCells * anfParallelism
      }

    perPixel * pixels + sarWindow + areaRtc
  }

  private def anfParallelism: Int = math.max(1, math.min(Runtime.getRuntime.availableProcessors(), MaxAnfParallelism))

  /** Ratio of the ground area of an output pixel to that of a radar pixel (ground range x azimuth), at the centre
   *  of the tile; 1 if the geometry is degenerate (the radar grid size is checked exactly later on). */
  private def radarPixelsPerOutputPixel(ctx: TileComputeContext): Double = {
    val req = ctx.request
    val timing = ctx.metadata.timing
    def ground(c: Int, r: Int): Vec3 = {
      val (lon, lat) = TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
      Ecef.fromGeodetic(lon, lat, 0.0)
    }
    val c = req.cols / 2; val r = req.rows / 2
    val p = ground(c, r)
    val outputArea = (ground(c + 1, r) - p).cross(ground(c, r + 1) - p).norm

    val tMid = 0.5 * timing.numberOfLines * timing.lineTimeInterval
    val (pSat, vel) = ctx.metadata.orbit.stateAt(tMid)
    val azimuthSpacing = vel.norm * p.norm / pSat.norm * timing.lineTimeInterval
    val ratio = outputArea / (timing.rangePixelSpacing * azimuthSpacing)
    if (ratio > 0 && !ratio.isInfinite) ratio else 1.0
  }

  private def computeTile(ctx: TileComputeContext): MultibandTile = {
    logger.debug(s"sar_backscatter: item ${ctx.request.sceneId}, ${ctx.request.extent} ${ctx.request.cellSize}")
    val req    = ctx.request
    val meta   = ctx.metadata
    val config = req.config
    val pols   = req.polarisations.toArray
    val srgrList = meta.polarisations(pols(0)).srgr

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
          val grMetres = srgrList.groundRangeFromSlant(tAz, rSlant)
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
    //     The same steep criteria decide whether a tile can contain cast shadow at all.
    val algorithm = if (doGamma0) config.rtcAlgorithm else RtcAlgorithm.Angle
    val castShadowCheck = config.castShadow && (doGamma0 || doShadow)
    val needSteep = algorithm == RtcAlgorithm.Auto || castShadowCheck
    val isInScene: Int => Boolean = i => {
      val line = sarLine(i); val gr = sarGr(i)
      !java.lang.Double.isNaN(line) && line >= 0 && line < numberOfLines && gr >= 0 && gr < numberOfPixels
    }

    val layoverPre = new Array[Boolean](if (needSteep) rows * cols else 0)
    val thetaLocPre: Array[Double] =
      if (needSteep) {
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

    val steep: Array[Boolean] =
      if (needSteep) {
        val minRad = math.toRadians(config.rtcAutoMinLocalIncidenceDeg)
        val maxRad = math.toRadians(config.rtcAutoMaxLocalIncidenceDeg)
        Array.tabulate(rows * cols)(i => !java.lang.Double.isNaN(thetaLocPre(i)) &&
          (thetaLocPre(i) < minRad || thetaLocPre(i) > maxRad || layoverPre(i)))
      } else null

    val useArea: Array[Boolean] = algorithm match {
      case RtcAlgorithm.BilinearDistribution =>
        Array.tabulate(rows * cols)(isInScene)
      case RtcAlgorithm.Auto =>
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
        val result = computeAnf(ctx, sarLine, pGndX, pGndY, pGndZ, pSatX, pSatY, pSatZ, isInScene, tSeed)
        logger.debug(s"sar_backscatter: item ${req.sceneId}, ${config.rtcAlgorithm.name} RTC took ${(System.nanoTime() - tStart) / 1000000} ms")
        Some(result)
      } else None
    val rtcMinValue = config.rtcMinValueDb.map(db => math.pow(10.0, db / 10.0)).getOrElse(0.0)

    // 6c. Cast shadow: only for tiles with steep terrain.
    val castShadowed: Array[Boolean] =
      if (castShadowCheck && steep.exists(identity)) {
        val tStart = System.nanoTime()
        val result = computeCastShadow(ctx, dem, lonRadGrid, latRadGrid, pGndX, pGndY, pGndZ, pSatX, pSatY, pSatZ, isInScene)
        logger.debug(s"sar_backscatter: item ${req.sceneId}, cast shadow took ${(System.nanoTime() - tStart) / 1000000} ms")
        result
      } else null

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
          val isCastShadow = castShadowed != null && castShadowed(i)
          var rtcFactor = 1.0
          var areaFactor = Double.NaN
          val areaPx = anf.isDefined && useArea(i)
          var sinEl = 1.0

          if (needTerrainCheck) {
            val pGnd = Vec3(pGndX(i), pGndY(i), pGndZ(i))
            val pSat = Vec3(pSatX(i), pSatY(i), pSatZ(i))
            val ellipsoidNorm = Ecef.ellipsoidalNormal(pGnd)
            val thetaEl = RangeDoppler.localIncidence(pGnd, pSat, ellipsoidNorm)
            // The steep pre-pass already has the local incidence angles and the layover test: reuse them.
            val (thetaLoc, rangeLayover) =
              if (thetaLocPre != null) (thetaLocPre(i), layoverPre(i))
              else {
                val terrainNorm = terrainSurfaceNormal(demB, lonRadGrid, latRadGrid, r, c, req)
                (RangeDoppler.localIncidence(pGnd, pSat, terrainNorm),
                  RangeDoppler.localIncidenceInRangePlane(pGnd, pSat, terrainNorm, ellipsoidNorm) < 0.0)
              }
            sinEl = math.sin(thetaEl)

            if (config.localIncidenceAngle) localInc.get.setDouble(c, r, math.toDegrees(thetaLoc))
            if (config.ellipsoidIncidenceAngle) ellipsInc.get.setDouble(c, r, math.toDegrees(thetaEl))

            // Layover: the terrain faces the sensor more steeply than the look direction (range plane).
            // Shadow: the terrain faces away from the sensor, or is hidden from it by terrain closer to it.
            isLayover = rangeLayover || thetaEl > math.Pi / 2.0
            isShadow = !isLayover && (thetaLoc >= math.Pi / 2.0 || isCastShadow)

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
          // Cast shadow is not seen by the area-based algorithms: always masked.
          val maskInvalid = (doGamma0 || doShadow) && !areaPx
          if (!(maskInvalid && (isLayover || isShadow)) && !isCastShadow) {
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
                         sarLine: Array[Double],
                         pGndX: Array[Double], pGndY: Array[Double], pGndZ: Array[Double],
                         pSatX: Array[Double], pSatY: Array[Double], pSatZ: Array[Double],
                         isInScene: Int => Boolean, tSeed: Double): AreaRtc.Anf = {
    val req = ctx.request
    val meta = ctx.metadata
    val config = req.config

    var minAz = Double.MaxValue; var maxAz = Double.MinValue
    var minR = Double.MaxValue; var maxR = Double.MinValue
    var i = 0
    while (i < sarLine.length) {
      if (isInScene(i)) {
        val rSlant = (Vec3(pSatX(i), pSatY(i), pSatZ(i)) - Vec3(pGndX(i), pGndY(i), pGndZ(i))).norm
        minAz = math.min(minAz, sarLine(i)); maxAz = math.max(maxAz, sarLine(i))
        minR = math.min(minR, rSlant); maxR = math.max(maxR, rSlant)
      }
      i += 1
    }

    // The radar grid is a window of one lattice for the whole scene (whole azimuth lines; slant range spacing from
    // the ground range spacing at the scene centre, multiples of it), so that the ANF of adjacent tiles, and of
    // the blocks of a tile, is computed on the same radar pixels: no seams between them.
    val timing = meta.timing
    val srgr = meta.polarisations(req.polarisations.head).srgr
    val dr = timing.rangePixelSpacing * srgr.dSlantRange_dGround(
      0.5 * timing.numberOfLines * timing.lineTimeInterval, 0.5 * timing.numberOfPixels * timing.rangePixelSpacing)

    val pad = 3
    val azLine0 = math.floor(minAz) - pad
    val nAz = (math.ceil(maxAz) + pad - azLine0).toInt + 1
    val rg0 = math.floor(minR / dr) - pad
    val rMin = rg0 * dr
    val nRg = (math.ceil(maxR / dr) + pad - rg0).toInt + 1
    val grid = AreaRtc.RadarGrid(azLine0, nAz, rMin, dr, nRg, timing.lineTimeInterval)
    // Each parallel chunk accumulates in its own copy of the grid: use as many as fit in half of the budget.
    val gridBytes = 8.0 * grid.size
    val nChunks = math.min(anfParallelism.toDouble, math.floor(maxWorkingMemoryBytes / 2.0 / gridBytes)).toInt
    if (nChunks < 1 || grid.size > Int.MaxValue)
      throw new RadarGridTooLargeException(s"radar grid of $nAz x $nRg for area-based RTC is too large")

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

    AreaRtc.computeRtcBilinearDistribution(fineDem, lonLatRad, grid, meta.orbit, tSeed, nChunks)
  }

  /** Pixels of the tile in cast shadow: hidden from the sensor by terrain between them and the sensor. The line of
   *  sight of each pixel is followed towards the sensor (in its zero-Doppler plane) over a DEM with a margin on the
   *  sensor side of the tile, until it is above the highest terrain; the pixel is shadowed if the terrain rises above
   *  it. Equivalent to the elevation angle test of ISCE3 `Topo::setLayoverShadow` along a range line. The margin is
   *  grown until it holds all terrain that is high enough to shadow the tile, up to [[MaxCastShadowMarginMetres]]. */
  private def computeCastShadow(ctx: TileComputeContext, tileDem: Array[Array[Float]],
                                lonRadGrid: Array[Double], latRadGrid: Array[Double],
                                pGndX: Array[Double], pGndY: Array[Double], pGndZ: Array[Double],
                                pSatX: Array[Double], pSatY: Array[Double], pSatZ: Array[Double],
                                isInScene: Int => Boolean): Array[Boolean] = {
    val req = ctx.request
    val rows = req.rows; val cols = req.cols
    val cw = req.cellSize.width; val ch = req.cellSize.height
    val n = rows * cols
    val out = new Array[Boolean](n)

    // Map coordinates per metre east and north, at the tile centre (varies negligibly over the length of a shadow).
    val (xPerE, xPerN, yPerE, yPerN) = {
      val toLonLat = geotrellis.proj4.Transform(req.crs, LatLng)
      val cx = 0.5 * (req.extent.xmin + req.extent.xmax); val cy = 0.5 * (req.extent.ymin + req.extent.ymax)
      val (lon0, lat0) = toLonLat(cx, cy)
      val (lonX, latX) = toLonLat(cx + 1.0, cy)
      val (lonY, latY) = toLonLat(cx, cy + 1.0)
      val sinLat = math.sin(math.toRadians(lat0))
      val w = 1.0 - Ecef.e2 * sinLat * sinLat
      val primeVertical = Ecef.a / math.sqrt(w)
      val meridional = Ecef.a * (1.0 - Ecef.e2) / (w * math.sqrt(w))
      def en(lon: Double, lat: Double) =
        (math.toRadians(lon - lon0) * primeVertical * math.cos(math.toRadians(lat0)), math.toRadians(lat - lat0) * meridional)
      val (eX, nX) = en(lonX, latX); val (eY, nY) = en(lonY, latY)
      val det = eX * nY - eY * nX
      (nY / det, -eY / det, -nX / det, eX / det)
    }

    // Horizontal direction of the line of sight towards the sensor, in DEM grid cells (columns, rows) per metre, and
    // the rise of the line of sight per horizontal metre. Null where the sensor is below the horizon.
    def look(i: Int): Array[Double] = {
      val lon = lonRadGrid(i); val lat = latRadGrid(i)
      val sLon = math.sin(lon); val cLon = math.cos(lon); val sLat = math.sin(lat); val cLat = math.cos(lat)
      val lx = pSatX(i) - pGndX(i); val ly = pSatY(i) - pGndY(i); val lz = pSatZ(i) - pGndZ(i)
      val le = -sLon * lx + cLon * ly
      val ln = -sLat * cLon * lx - sLat * sLon * ly + cLat * lz
      val lu = cLat * cLon * lx + cLat * sLon * ly + sLat * lz
      val hz = math.hypot(le, ln)
      if (hz <= 0.0 || lu <= 0.0) return null
      val de = le / hz; val dn = ln / hz
      Array((xPerE * de + xPerN * dn) / cw, -(yPerE * de + yPerN * dn) / ch, lu / hz)
    }

    var minTanE = Double.MaxValue
    var sumGc = 0.0; var sumGr = 0.0
    var i = 0
    while (i < n) {
      if (isInScene(i)) {
        val l = look(i)
        if (l != null) { sumGc += l(0); sumGr += l(1); minTanE = math.min(minTanE, l(2)) }
      }
      i += 1
    }
    if (minTanE == Double.MaxValue) return out
    val dirNorm = math.hypot(sumGc, sumGr)
    if (dirNorm == 0.0) return out
    val dirC = sumGc / dirNorm; val dirR = sumGr / dirNorm // unit direction towards the sensor, in grid cells

    def minMax(dem: Array[Array[Float]]): (Double, Double) = {
      var lo = Double.MaxValue; var hi = -Double.MaxValue
      for (row <- dem; h <- row if !java.lang.Float.isNaN(h)) { lo = math.min(lo, h); hi = math.max(hi, h) }
      (lo, hi)
    }

    // DEM extended with `margin` metres towards the sensor (2 cells on the other sides), aligned with the tile grid.
    def readWithMargin(margin: Double): (Array[Array[Float]], Int, Int) = {
      val cellMetres = math.min(cw, ch)
      val extraC = math.ceil(margin / cellMetres * math.abs(dirC)).toInt + 2
      val extraR = math.ceil(margin / cellMetres * math.abs(dirR)).toInt + 2
      val left = if (dirC < 0) extraC else 2; val right = if (dirC > 0) extraC else 2
      val top = if (dirR < 0) extraR else 2; val bottom = if (dirR > 0) extraR else 2
      val e = req.extent
      val extent = Extent(e.xmin - left * cw, e.ymin - bottom * ch, e.xmax + right * cw, e.ymax + top * ch)
      (TerrainCorrectionProcessor.readDemEllipsoidal(ctx.copy(request = req.copy(extent = extent))), left, top)
    }

    val (hMinTile, hMaxTile) = minMax(tileDem)
    if (hMinTile == Double.MaxValue) return out
    // Start from the relief of the tile itself, grow the margin while higher terrain is found in it.
    var margin = math.min(MaxCastShadowMarginMetres, (hMaxTile - hMinTile) / minTanE * 1.2 + 4 * math.max(cw, ch))
    var (dem, offC, offR) = readWithMargin(margin)
    var hMax = minMax(dem)._2
    var done = false
    while (!done) {
      val needed = (hMax - hMinTile) / minTanE
      if (needed <= margin || margin >= MaxCastShadowMarginMetres) done = true
      else {
        margin = math.min(MaxCastShadowMarginMetres, needed * 1.2 + 4 * math.max(cw, ch))
        val (d, c0, r0) = readWithMargin(margin)
        dem = d; offC = c0; offR = r0
        hMax = minMax(dem)._2
      }
    }

    val demRows = dem.length; val demCols = dem(0).length
    def sample(col: Double, row: Double): Double = {
      val c0 = math.floor(col).toInt; val r0 = math.floor(row).toInt
      if (c0 < 0 || r0 < 0 || c0 + 1 >= demCols || r0 + 1 >= demRows) return Double.NaN
      val tx = col - c0; val ty = row - r0
      val a = dem(r0)(c0) + (dem(r0)(c0 + 1) - dem(r0)(c0)) * tx
      val b = dem(r0 + 1)(c0) + (dem(r0 + 1)(c0 + 1) - dem(r0 + 1)(c0)) * tx
      a + (b - a) * ty
    }

    val twoR = 2.0 * Ecef.a
    Array.range(0, rows).par.foreach { r =>
      var c = 0
      while (c < cols) {
        val i = r * cols + c
        val hP = dem(r + offR)(c + offC).toDouble
        if (isInScene(i) && !java.lang.Double.isNaN(hP) && hP < hMax) {
          val l = look(i)
          if (l != null) {
            val gc = l(0); val gr = l(1); val tanE = l(2)
            val step = 1.0 / math.max(math.abs(gc), math.abs(gr)) // metres per cell along the dominant axis
            val c0 = c + offC; val r0 = r + offR
            var k = 1
            var marching = true
            while (marching) {
              val d = k * step
              // straight line of sight above the curved Earth
              val rayH = hP + d * tanE + d * d / twoR
              if (rayH >= hMax) marching = false
              else {
                val col = c0 + d * gc; val row = r0 + d * gr
                if (col < 0 || row < 0 || col >= demCols - 1 || row >= demRows - 1) marching = false
                else {
                  val z = sample(col, row)
                  if (z > rayH + CastShadowToleranceMetres) { out(i) = true; marching = false }
                }
              }
              k += 1
            }
          }
        }
        c += 1
      }
    }
    out
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
