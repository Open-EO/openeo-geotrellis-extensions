package org.openeo.sar.orbit

import org.openeo.sar.geom.Vec3

/** A single Sentinel-1 OSV record (orbit state vector). Times are seconds
 *  relative to a scene-local epoch (typically firstLineUtc) to keep numerical
 *  precision well within Float64. */
final case class StateVector(t: Double, pos: Vec3, vel: Vec3)

/** Polynomial interpolation of orbit state vectors. SNAP / S1TBX uses an
 *  8th-order polynomial fit over the nearest 9 OSVs; we use Lagrange
 *  interpolation, which is equivalent and easier to express.
 *
 *  For GRD products, OSVs are spaced ~10 s apart; an 8th order fit is
 *  numerically well-behaved over ~minute-scale scenes. */
final class OrbitInterpolator(private val svs: IndexedSeq[StateVector]) {

  require(svs.length >= 8, s"need at least 8 state vectors, got ${svs.length}")

  /** Pick the 8 OSVs nearest in time and Lagrange-interpolate position & velocity. */
  def stateAt(t: Double): (Vec3, Vec3) = {
    val window = pickWindow(t, n = 8)
    val xs = window.map(_.t)
    val (px, py, pz) = (window.map(_.pos.x), window.map(_.pos.y), window.map(_.pos.z))
    val (vx, vy, vz) = (window.map(_.vel.x), window.map(_.vel.y), window.map(_.vel.z))
    val p = Vec3(lagrange(xs, px, t), lagrange(xs, py, t), lagrange(xs, pz, t))
    val v = Vec3(lagrange(xs, vx, t), lagrange(xs, vy, t), lagrange(xs, vz, t))
    (p, v)
  }

  def positionAt(t: Double): Vec3 = stateAt(t)._1
  def velocityAt(t: Double): Vec3 = stateAt(t)._2

  /** Numerically differentiate to get acceleration (only used for Newton f'(t)). */
  def accelerationAt(t: Double, dt: Double = 0.5): Vec3 = {
    val vp = velocityAt(t + dt); val vm = velocityAt(t - dt)
    (vp - vm) * (1.0 / (2.0 * dt))
  }

  private def pickWindow(t: Double, n: Int): IndexedSeq[StateVector] = {
    val idx = svs.indexWhere(_.t >= t) match {
      case -1 => svs.length - 1
      case i  => i
    }
    val half = n / 2
    val lo = math.max(0, math.min(svs.length - n, idx - half))
    svs.slice(lo, lo + n)
  }

  private def lagrange(xs: IndexedSeq[Double], ys: IndexedSeq[Double], x: Double): Double = {
    var sum = 0.0
    val n = xs.length
    var i = 0
    while (i < n) {
      var num = 1.0; var den = 1.0; var j = 0
      while (j < n) {
        if (j != i) {
          num *= (x  - xs(j))
          den *= (xs(i) - xs(j))
        }
        j += 1
      }
      sum += ys(i) * (num / den)
      i += 1
    }
    sum
  }
}
