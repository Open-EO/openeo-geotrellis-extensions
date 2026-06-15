package org.openeo.sar.geom

import org.openeo.sar.orbit.OrbitInterpolator

/** Range-Doppler geometry: solve for zero-Doppler azimuth time, slant range,
 *  and local incidence angle. All inputs in ECEF / seconds-from-epoch. */
object RangeDoppler {

  /** Newton iteration for f(t) = (P_sat(t) - P_gnd) . V_sat(t) = 0.
   *
   *  f'(t) = |V|^2 + (P_sat - P_gnd) . A_sat
   *
   *  Converges in 3-5 iterations from a reasonable seed. Returns the
   *  zero-Doppler time in the same epoch as the OrbitInterpolator. */
  def zeroDopplerTime(pGround: Vec3, orbit: OrbitInterpolator,
                      tSeed: Double, maxIter: Int = 8, tol: Double = 1e-6): Double = {
    var t = tSeed
    var i = 0
    while (i < maxIter) {
      val (p, v) = orbit.stateAt(t)
      val r = p - pGround
      val f  = r.dot(v)
      val a  = orbit.accelerationAt(t)
      val fp = v.dot(v) + r.dot(a)
      val dt = f / fp
      t -= dt
      if (math.abs(dt) < tol) return t
      i += 1
    }
    t
  }

  def slantRange(pGround: Vec3, pSat: Vec3): Double = (pSat - pGround).norm

  /** Local incidence angle (radians) between the look direction (sat -> ground)
   *  and a given surface normal at the ground point. */
  def localIncidence(pGround: Vec3, pSat: Vec3, normal: Vec3): Double = {
    val look = (pGround - pSat).normalize
    math.acos(math.min(1.0, math.max(-1.0, math.abs(look.dot(normal)))))
  }
}
