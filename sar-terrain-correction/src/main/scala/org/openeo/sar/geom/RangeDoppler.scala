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

  /** Local incidence angle (radians, 0 to pi) between the reversed look direction (ground -> sat) and a given
   *  outward surface normal at the ground point. Values above pi/2 mean the surface faces away from the sensor
   *  (radar shadow). */
  def localIncidence(pGround: Vec3, pSat: Vec3, normal: Vec3): Double = {
    val look = (pGround - pSat).normalize
    math.acos(math.min(1.0, math.max(-1.0, -look.dot(normal))))
  }

  /** Signed local incidence angle (radians) in the range plane (the plane spanned by the look direction and the
   *  ellipsoid normal, i.e. perpendicular to the azimuth direction at zero Doppler): the ellipsoid incidence angle
   *  minus the slope of the terrain towards the sensor, in that plane.
   *
   *  Negative: the slope faces the sensor more steeply than the look direction (layover).
   *  Above pi/2: the slope faces away from the sensor (shadow). */
  def localIncidenceInRangePlane(pGround: Vec3, pSat: Vec3, normal: Vec3, ellipsoidNormal: Vec3): Double = {
    val toSensor = (pSat - pGround).normalize
    val m = toSensor.cross(ellipsoidNormal)
    val mNorm = m.norm
    if (mNorm < 1e-9) return 0.0
    val mHat = m * (1.0 / mNorm)
    val nProj = (normal - mHat * normal.dot(mHat)).normalize
    // horizontal unit vector in the range plane, pointing towards the sensor
    val eH = (toSensor - ellipsoidNormal * toSensor.dot(ellipsoidNormal)).normalize
    val thetaEl = math.acos(math.min(1.0, math.max(-1.0, toSensor.dot(ellipsoidNormal))))
    val tilt = math.atan2(nProj.dot(eH), nProj.dot(ellipsoidNormal))
    thetaEl - tilt
  }
}
