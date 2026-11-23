package org.openeo.sar.metadata

/** Slant-Range / Ground-Range conversion polynomial as encoded in the
 *  Sentinel-1 GRD annotation product (`coordinateConversion/coordinateConversionList`).
 *
 *  For each azimuth time t_az, the SAFE product carries:
 *
 *    sr0:      slant range at the first (zero) ground-range pixel
 *    coeffs:   polynomial in ground range (metres from sr0) giving slant range
 *
 *  i.e.  slantRange(gr) = sum_k coeffs(k) * (gr - gr0)^k
 *
 *  We invert this with Newton iteration to go slantRange -> groundRange. */
final case class SrgrPoly(azimuthTime: Double, sr0: Double, gr0: Double, coeffs: Array[Double]) {

  /** Forward: ground-range distance (m) -> slant range (m). */
  def slantRangeFromGround(grMetres: Double): Double = {
    val u = grMetres - gr0
    var k = coeffs.length - 1; var acc = 0.0
    while (k >= 0) { acc = acc * u + coeffs(k); k -= 1 }
    acc
  }

  /** Derivative d(slantRange)/d(gr). */
  def dSlantRange_dGround(grMetres: Double): Double = {
    val u = grMetres - gr0
    var k = coeffs.length - 1; var acc = 0.0
    while (k >= 1) { acc = acc * u + coeffs(k) * k; k -= 1 }
    acc
  }

  /** Newton: slant range -> ground-range distance (m). */
  def groundRangeFromSlant(rSlant: Double, gSeed: Double, maxIter: Int = 8, tol: Double = 1e-3): Double = {
    var g = gSeed; var i = 0
    while (i < maxIter) {
      val f  = slantRangeFromGround(g) - rSlant
      val fp = dSlantRange_dGround(g)
      val dg = f / fp
      g -= dg
      if (math.abs(dg) < tol) return g
      i += 1
    }
    g
  }
}

/** A time-ordered set of SRGR polynomials, linearly interpolated in azimuth time between the two
 *  bracketing records (clamped to the first/last record outside the covered interval), as SNAP does.
 *  Records are ~1 s apart and, over steep terrain, differ enough that picking one per interval
 *  produces a range sawtooth of several pixels. */
final class SrgrPolyList(val records: IndexedSeq[SrgrPoly]) {

  /** Index of the last record with azimuthTime <= tAz, clamped to [0, n-2]. */
  private def lowerIndex(tAz: Double): Int = {
    var lo = 0; var hi = records.length - 1
    while (hi - lo > 1) {
      val mid = (lo + hi) >>> 1
      if (records(mid).azimuthTime <= tAz) lo = mid else hi = mid
    }
    lo
  }

  private def weight(p0: SrgrPoly, p1: SrgrPoly, tAz: Double): Double = {
    val dt = p1.azimuthTime - p0.azimuthTime
    if (dt <= 0.0) 0.0 else math.min(1.0, math.max(0.0, (tAz - p0.azimuthTime) / dt))
  }

  /** Slant range -> ground-range distance (m) using the polynomial interpolated at `tAz`.
   *  Allocation-free: this is called once per output pixel. */
  def groundRangeFromSlant(tAz: Double, rSlant: Double, maxIter: Int = 8, tol: Double = 1e-3): Double = {
    val p0 = records.head
    if (records.length == 1) return p0.groundRangeFromSlant(rSlant, math.max(0.0, rSlant - p0.sr0), maxIter, tol)
    val i = lowerIndex(tAz)
    val a = records(i); val b = records(i + 1)
    val w = weight(a, b, tAz)
    val sr0 = (1.0 - w) * a.sr0 + w * b.sr0
    var g = math.max(0.0, rSlant - sr0); var it = 0
    while (it < maxIter) {
      val f  = (1.0 - w) * a.slantRangeFromGround(g) + w * b.slantRangeFromGround(g) - rSlant
      val fp = (1.0 - w) * a.dSlantRange_dGround(g) + w * b.dSlantRange_dGround(g)
      val dg = f / fp
      g -= dg
      if (math.abs(dg) < tol) return g
      it += 1
    }
    g
  }

  /** Derivative d(slantRange)/d(gr) of the polynomial interpolated at `tAz`. */
  def dSlantRange_dGround(tAz: Double, grMetres: Double): Double = {
    if (records.length == 1) return records.head.dSlantRange_dGround(grMetres)
    val a = records(lowerIndex(tAz)); val b = records(lowerIndex(tAz) + 1)
    val w = weight(a, b, tAz)
    (1.0 - w) * a.dSlantRange_dGround(grMetres) + w * b.dSlantRange_dGround(grMetres)
  }
}

