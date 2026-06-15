package org.openeo.sar.backend.onnx

import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import geotrellis.raster.{FloatArrayTile, GridBounds, MultibandTile, Tile}
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.geom.{Ecef, Vec3}
import org.openeo.sar.metadata.S1GrdMetadata
import org.openeo.sar.{TerrainCorrectionProcessor, TileComputeContext}

import java.nio.{FloatBuffer, LongBuffer}
import scala.jdk.CollectionConverters._

/** ONNX-backed terrain correction. The ONNX graph implements the per-pixel
 *  inner loop on flat [N] tensors (one element per output pixel):
 *
 *  Inputs:
 *    lonRad           : float32 [N]
 *    latRad           : float32 [N]
 *    h_ellipsoidal_m  : float32 [N]
 *    sar_window       : float32 [H, W]            (DN, single polarisation, single windowed read)
 *    sar_win_origin   : int64   [2]               (winMinLine, winMinPx)
 *    sigma_lut        : float32 [Li, Pi]
 *    sigma_lines      : int64   [Li]
 *    sigma_pixels     : int64   [Pi]
 *    noise_lut        : float32 [Li, Pi]
 *    orbit_t          : float32 [K]               (K state-vector times, scene-local seconds)
 *    orbit_pos        : float32 [K, 3]
 *    orbit_vel        : float32 [K, 3]
 *    srgr_gr0         : float32 ()
 *    srgr_coeffs      : float32 [M]               (selected nearest-time poly)
 *    line_time_interval, range_px_spacing, tSeed  : float32 ()
 *    nLines, nPixels  : int64 ()
 *    ecef_origin      : float32 [3]               (tile-local frame origin for fp32 precision)
 *
 *  Outputs:
 *    sigma0    : float32 [N]
 *    incidence : float32 [N]    (degrees)
 *    mask      : float32 [N]
 *
 *  Per-polarisation execution: one session.run() per polarisation, reusing
 *  the constant tensors via I/O bindings. */
final class OnnxBackend(modelPath: String) extends TerrainCorrectionBackend with AutoCloseable {
  override val name = "onnx"

  private val env: OrtEnvironment = OrtEnvironment.getEnvironment()
  private val session: OrtSession = {
    val opts = new OrtSession.SessionOptions()
    opts.setIntraOpNumThreads(1)            // leave parallelism to the caller (Spark)
    env.createSession(modelPath, opts)
  }

  override def compute(ctx: TileComputeContext): MultibandTile = {
    val req = ctx.request
    val meta = ctx.metadata
    val pols = req.polarisations.toArray
    val n = req.cols * req.rows

    val (sigmas, ellipsInc, localInc, mask, shadowLayover) =
      TerrainCorrectionBackend.allocate(req.cols, req.rows, pols.length, req.config)
    // The ONNX model only outputs ellipsoidal incidence; localInc and shadowLayover are not
    // available. Fill localInc with NaN to signal "no terrain normal computed".
    java.util.Arrays.fill(localInc.array, Float.NaN)

    // ---- 1. Build pixel-level inputs that are shared across polarisations ----
    val dem = TerrainCorrectionProcessor.readDemEllipsoidal(ctx)
    val lon  = new Array[Float](n); val lat = new Array[Float](n); val hEll = new Array[Float](n)
    var idx = 0; var r = 0
    while (r < req.rows) {
      var c = 0
      while (c < req.cols) {
        val (lo, la) = TerrainCorrectionProcessor.pixelToLonLatRad(c, r, req)
        lon(idx) = lo.toFloat; lat(idx) = la.toFloat
        val h = dem(r)(c); hEll(idx) = if (java.lang.Double.isNaN(h)) Float.NaN else h.toFloat
        idx += 1; c += 1
      }
      r += 1
    }

    // ---- 2. Tile-local ECEF origin (use centroid lon/lat at h=0) ----
    val cx = req.extent.center
    val (cLonRad, cLatRad) = TerrainCorrectionProcessor.pixelToLonLatRad(req.cols / 2, req.rows / 2, req)
    val origin: Vec3 = Ecef.fromGeodetic(cLonRad, cLatRad, 0.0)

    // ---- 3. Orbit constant tensors (K, K x 3) ----
    val (orbitT, orbitPos, orbitVel) = packOrbit(meta, origin)

    // ---- 4. Forward-geocode one pol on CPU just to compute the SAR window bbox. ----
    //         (cheap; reuses native helpers; could also be moved into ONNX as a separate model.)
    val window = computeSarWindow(ctx, dem, lon, lat, hEll)

    // ---- 5. Per-polarisation: read SAR window, run the model, scatter outputs. ----
    var p = 0
    while (p < pols.length) {
      val pol = pols(p); val polMeta = meta.polarisations(pol)
      val sarTile: Tile = ctx.sarSources(pol).read(window.gb).get.tile.band(0)

      val sarBuf = tileToFloatArray(sarTile)
      val (sigLines, sigPixels, sigVals) = packLut(polMeta.sigmaLut)
      val (_,        _,         noiseVals) = packLut(polMeta.noiseLut)

      // Pick SRGR poly at scene-centre azimuth time as a constant (good enough at single-tile scale).
      val tCenter = 0.5 * meta.timing.numberOfLines * meta.timing.lineTimeInterval
      val srgr = polMeta.srgr.at(tCenter)

      val inputs = Map[String, OnnxTensor](
        "lonRad"          -> tensor1d(lon),
        "latRad"          -> tensor1d(lat),
        "h_ellipsoidal_m" -> tensor1d(hEll),
        "sar_window"      -> tensor2d(sarBuf, sarTile.rows, sarTile.cols),
        "sar_win_origin"  -> tensorLong(Array(window.winMinLine.toLong, window.winMinPx.toLong)),
        "sigma_lut"       -> tensor2d(sigVals, sigLines.length, sigPixels.length),
        "sigma_lines"     -> tensorLong(sigLines.map(_.toLong)),
        "sigma_pixels"    -> tensorLong(sigPixels.map(_.toLong)),
        "noise_lut"       -> tensor2d(noiseVals, sigLines.length, sigPixels.length),
        "orbit_t"         -> tensor1d(orbitT),
        "orbit_pos"       -> tensor2d(orbitPos, orbitT.length, 3),
        "orbit_vel"       -> tensor2d(orbitVel, orbitT.length, 3),
        "srgr_gr0"        -> scalar(srgr.gr0.toFloat),
        "srgr_coeffs"     -> tensor1d(srgr.coeffs.map(_.toFloat)),
        "line_time_interval" -> scalar(meta.timing.lineTimeInterval.toFloat),
        "range_px_spacing"   -> scalar(meta.timing.rangePixelSpacing.toFloat),
        "tSeed"              -> scalar(tCenter.toFloat),
        "nLines"             -> scalarLong(meta.timing.numberOfLines.toLong),
        "nPixels"            -> scalarLong(meta.timing.numberOfPixels.toLong),
        "ecef_origin"        -> tensor1d(Array(origin.x.toFloat, origin.y.toFloat, origin.z.toFloat))
      )

      val result = session.run(inputs.asJava)
      try {
        val sigma0 = result.get(0).asInstanceOf[OnnxTensor].getFloatBuffer.array()
        val incOut = result.get(1).asInstanceOf[OnnxTensor].getFloatBuffer.array()
        val mskOut = result.get(2).asInstanceOf[OnnxTensor].getFloatBuffer.array()
        scatter(sigma0, sigmas(p), req.cols, req.rows)
        if (p == 0) {
          scatter(incOut, ellipsInc, req.cols, req.rows)
          scatter(mskOut, mask, req.cols, req.rows)
        }
      } finally {
        result.close()
        inputs.values.foreach(_.close())
      }
      p += 1
    }

    TerrainCorrectionBackend.assemble(sigmas, ellipsInc, localInc, mask, shadowLayover)
  }

  override def close(): Unit = { session.close() }

  // --- helpers ---------------------------------------------------------------

  private case class Window(winMinLine: Int, winMinPx: Int, gb: GridBounds[Long])

  /** Light forward-geocoding pass to bound the SAR window. Mirrors NativeBackend. */
  private def computeSarWindow(ctx: TileComputeContext,
                               dem: Array[Array[Double]],
                               lon: Array[Float], lat: Array[Float], h: Array[Float]): Window = {
    import org.openeo.sar.geom.RangeDoppler
    val req = ctx.request; val meta = ctx.metadata
    val tSeed = 0.5 * meta.timing.numberOfLines * meta.timing.lineTimeInterval
    var minLine = Int.MaxValue; var maxLine = Int.MinValue
    var minPx   = Int.MaxValue; var maxPx   = Int.MinValue
    val anyPol = ctx.metadata.polarisations.values.head
    var i = 0
    while (i < lon.length) {
      val hv = h(i)
      if (!java.lang.Float.isNaN(hv)) {
        val p = Ecef.fromGeodetic(lon(i), lat(i), hv.toDouble)
        val t = RangeDoppler.zeroDopplerTime(p, meta.orbit, tSeed)
        val pSat = meta.orbit.positionAt(t)
        val rSlant = (pSat - p).norm
        val srgr = anyPol.srgr.at(t)
        val grM = srgr.groundRangeFromSlant(rSlant, math.max(0.0, rSlant - srgr.sr0))
        val grPx = (grM / meta.timing.rangePixelSpacing).toInt
        val azLine = (t / meta.timing.lineTimeInterval).toInt
        if (azLine >= 0 && azLine < meta.timing.numberOfLines &&
            grPx   >= 0 && grPx   < meta.timing.numberOfPixels) {
          if (azLine < minLine) minLine = azLine; if (azLine > maxLine) maxLine = azLine
          if (grPx   < minPx)   minPx   = grPx;   if (grPx   > maxPx)   maxPx   = grPx
        }
      }
      i += 1
    }
    val mnL = math.max(0, minLine - 1); val mnP = math.max(0, minPx - 1)
    val mxL = math.min(meta.timing.numberOfLines - 1,  maxLine + 1)
    val mxP = math.min(meta.timing.numberOfPixels - 1, maxPx   + 1)
    Window(mnL, mnP, GridBounds[Long](mnP.toLong, mnL.toLong, mxP.toLong, mxL.toLong))
  }

  private def packOrbit(meta: S1GrdMetadata, origin: Vec3): (Array[Float], Array[Float], Array[Float]) = {
    // We cannot reach the private OrbitInterpolator.svs field; instead expose it via a
    // typed accessor would be cleaner. For now, re-sample orbit state at fixed times.
    val nLines = meta.timing.numberOfLines
    val dt = meta.timing.lineTimeInterval
    val tStart = -10.0; val tEnd = nLines * dt + 10.0
    val K = 32
    val t = Array.tabulate(K)(i => tStart + (tEnd - tStart) * i / (K - 1).toFloat).map(_.toFloat)
    val pos = new Array[Float](K * 3); val vel = new Array[Float](K * 3)
    var i = 0
    while (i < K) {
      val (p, v) = meta.orbit.stateAt(t(i).toDouble)
      val pl = p - origin // tile-local for fp32
      pos(3*i)   = pl.x.toFloat; pos(3*i+1) = pl.y.toFloat; pos(3*i+2) = pl.z.toFloat
      vel(3*i)   = v.x .toFloat; vel(3*i+1) = v.y .toFloat; vel(3*i+2) = v.z .toFloat
      i += 1
    }
    (t, pos, vel)
  }

  private def packLut(lut: org.openeo.sar.metadata.Lut2D): (Array[Int], Array[Int], Array[Float]) = {
    val flat = new Array[Float](lut.lines.length * lut.pixels.length)
    var i = 0
    while (i < lut.lines.length) {
      System.arraycopy(lut.values(i), 0, flat, i * lut.pixels.length, lut.pixels.length)
      i += 1
    }
    (lut.lines, lut.pixels, flat)
  }

  private def tileToFloatArray(t: Tile): Array[Float] = {
    val arr = new Array[Float](t.cols * t.rows)
    var i = 0; var r = 0
    while (r < t.rows) {
      var c = 0
      while (c < t.cols) { arr(i) = t.getDouble(c, r).toFloat; i += 1; c += 1 }
      r += 1
    }
    arr
  }

  private def scatter(src: Array[Float], dst: FloatArrayTile, cols: Int, rows: Int): Unit = {
    var i = 0; var r = 0
    while (r < rows) {
      var c = 0
      while (c < cols) { dst.setDouble(c, r, src(i)); i += 1; c += 1 }
      r += 1
    }
  }

  private def tensor1d(a: Array[Float]): OnnxTensor =
    OnnxTensor.createTensor(env, FloatBuffer.wrap(a), Array[Long](a.length))
  private def tensor2d(a: Array[Float], h: Int, w: Int): OnnxTensor =
    OnnxTensor.createTensor(env, FloatBuffer.wrap(a), Array[Long](h, w))
  private def tensorLong(a: Array[Long]): OnnxTensor =
    OnnxTensor.createTensor(env, LongBuffer.wrap(a), Array[Long](a.length))
  private def scalar(v: Float): OnnxTensor =
    OnnxTensor.createTensor(env, FloatBuffer.wrap(Array(v)), Array[Long]())
  private def scalarLong(v: Long): OnnxTensor =
    OnnxTensor.createTensor(env, LongBuffer.wrap(Array(v)), Array[Long]())
}
