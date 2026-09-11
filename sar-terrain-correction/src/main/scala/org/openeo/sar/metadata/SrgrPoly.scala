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

/** A time-ordered set of SRGR polynomials. Pick the one nearest the queried
 *  azimuth time (S1TBX does the same; the polynomials are spaced ~1 s and
 *  the change between adjacent records is small). */
final class SrgrPolyList(val records: IndexedSeq[SrgrPoly]) {
  def at(tAz: Double): SrgrPoly = {
    var best = records.head; var bestDt = math.abs(best.azimuthTime - tAz)
    var i = 1
    while (i < records.length) {
      val d = math.abs(records(i).azimuthTime - tAz)
      if (d < bestDt) { best = records(i); bestDt = d }
      i += 1
    }
    best
  }
}
