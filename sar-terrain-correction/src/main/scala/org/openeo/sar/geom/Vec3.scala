package org.openeo.sar.geom

/** Tiny 3-vector. Doubles throughout; SAR geometry needs ~mm precision in ECEF. */
final case class Vec3(x: Double, y: Double, z: Double) {
  def +(o: Vec3): Vec3 = Vec3(x + o.x, y + o.y, z + o.z)
  def -(o: Vec3): Vec3 = Vec3(x - o.x, y - o.y, z - o.z)
  def *(s: Double): Vec3 = Vec3(x * s, y * s, z * s)
  def dot(o: Vec3): Double = x * o.x + y * o.y + z * o.z
  def cross(o: Vec3): Vec3 =
    Vec3(y * o.z - z * o.y, z * o.x - x * o.z, x * o.y - y * o.x)
  def norm: Double = math.sqrt(dot(this))
  def normalize: Vec3 = { val n = norm; if (n == 0) this else this * (1.0 / n) }
}

object Vec3 {
  val Zero: Vec3 = Vec3(0, 0, 0)
}
