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

  private val WindowSize = 8

  // Flattened, contiguous storage of the OSV series: stateAt/accelerationAt
  // are called several times per output pixel (multiple Newton iterations,
  // each needing position, velocity, and formerly acceleration), so avoiding
  // the per-call slicing + six `.map`s of the naive IndexedSeq[StateVector]
  // window matters a lot in the hot path.
  private val numStates = svs.length
  private val ts = Array.tabulate(numStates)(i => svs(i).t)
  private val px = Array.tabulate(numStates)(i => svs(i).pos.x)
  private val py = Array.tabulate(numStates)(i => svs(i).pos.y)
  private val pz = Array.tabulate(numStates)(i => svs(i).pos.z)
  private val vx = Array.tabulate(numStates)(i => svs(i).vel.x)
  private val vy = Array.tabulate(numStates)(i => svs(i).vel.y)
  private val vz = Array.tabulate(numStates)(i => svs(i).vel.z)

  /** Pick the 8 OSVs nearest in time and Lagrange-interpolate position & velocity. */
  def stateAt(t: Double): (Vec3, Vec3) = {
    val lo = windowStart(t)
    val p = Vec3(lagrange(lo, px, t), lagrange(lo, py, t), lagrange(lo, pz, t))
    val v = Vec3(lagrange(lo, vx, t), lagrange(lo, vy, t), lagrange(lo, vz, t))
    (p, v)
  }

  def positionAt(t: Double): Vec3 = stateAt(t)._1
  def velocityAt(t: Double): Vec3 = stateAt(t)._2

  /** Acceleration as the analytic derivative of the Lagrange-interpolated
   *  velocity polynomial, evaluated at t. Equivalent to central-differencing
   *  velocityAt(t +/- dt) (the previous implementation), but ~3x cheaper:
   *  that approach needed two extra stateAt calls, each itself an
   *  O(WindowSize^2) Lagrange evaluation, only to discard the position half
   *  of the result. Only used to feed Newton's method in
   *  [[org.openeo.sar.geom.RangeDoppler.zeroDopplerTime]], so approximating
   *  the derivative analytically (rather than numerically) only affects
   *  convergence speed, not the converged result. */
  def accelerationAt(t: Double): Vec3 = {
    val lo = windowStart(t)
    Vec3(lagrangeDerivative(lo, vx, t), lagrangeDerivative(lo, vy, t), lagrangeDerivative(lo, vz, t))
  }

  private def windowStart(t: Double): Int = {
    var i = 0
    while (i < numStates && ts(i) < t) i += 1
    val idx = if (i >= numStates) numStates - 1 else i
    val half = WindowSize / 2
    math.max(0, math.min(numStates - WindowSize, idx - half))
  }

  /** Lagrange interpolation at x, over the node window [lo, lo+WindowSize). */
  private def lagrange(lo: Int, ys: Array[Double], x: Double): Double = {
    var sum = 0.0
    var i = 0
    while (i < WindowSize) {
      var num = 1.0; var den = 1.0; var j = 0
      while (j < WindowSize) {
        if (j != i) {
          num *= x - ts(lo + j)
          den *= ts(lo + i) - ts(lo + j)
        }
        j += 1
      }
      sum += ys(lo + i) * (num / den)
      i += 1
    }
    sum
  }

  /** Derivative dL/dx of the Lagrange interpolant at x, over the node window
   *  [lo, lo+WindowSize), using l_i'(x) = l_i(x) * sum_{j != i} 1/(x - x_j). */
  private def lagrangeDerivative(lo: Int, ys: Array[Double], x: Double): Double = {
    var sum = 0.0
    var i = 0
    while (i < WindowSize) {
      var num = 1.0; var den = 1.0; var invSum = 0.0; var j = 0
      while (j < WindowSize) {
        if (j != i) {
          val dx = x - ts(lo + j)
          num *= dx
          den *= ts(lo + i) - ts(lo + j)
          invSum += 1.0 / dx
        }
        j += 1
      }
      sum += ys(lo + i) * (num / den) * invSum
      i += 1
    }
    sum
  }
}
