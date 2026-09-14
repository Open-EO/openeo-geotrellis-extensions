package org.openeo.sar.geom

/** WGS84 ellipsoid + geodetic <-> ECEF conversions. */
object Ecef {
  // WGS84
  val a: Double  = 6378137.0
  val f: Double  = 1.0 / 298.257223563
  val b: Double  = a * (1.0 - f)
  val e2: Double = f * (2.0 - f)
  val ep2: Double = (a * a - b * b) / (b * b)

  /** Geodetic (lon, lat in radians, h_ellipsoidal in metres) -> ECEF. */
  def fromGeodetic(lonRad: Double, latRad: Double, h: Double): Vec3 = {
    val sinLat = math.sin(latRad); val cosLat = math.cos(latRad)
    val sinLon = math.sin(lonRad); val cosLon = math.cos(lonRad)
    val N = a / math.sqrt(1.0 - e2 * sinLat * sinLat)
    Vec3(
      (N + h) * cosLat * cosLon,
      (N + h) * cosLat * sinLon,
      (N * (1.0 - e2) + h) * sinLat
    )
  }

  /** ECEF -> geodetic (lon, lat in radians, h_ellipsoidal in metres) via Bowring. */
  def toGeodetic(p: Vec3): (Double, Double, Double) = {
    val r = math.sqrt(p.x * p.x + p.y * p.y)
    val lon = math.atan2(p.y, p.x)
    val theta = math.atan2(p.z * a, r * b)
    val sinT = math.sin(theta); val cosT = math.cos(theta)
    val lat = math.atan2(p.z + ep2 * b * sinT * sinT * sinT,
                         r - e2  * a * cosT * cosT * cosT)
    val sinLat = math.sin(lat)
    val N = a / math.sqrt(1.0 - e2 * sinLat * sinLat)
    val h = r / math.cos(lat) - N
    (lon, lat, h)
  }

  /** WGS84 ellipsoidal surface normal (outward) at given ECEF point. */
  def ellipsoidalNormal(p: Vec3): Vec3 = {
    val (lonRad, latRad, _) = toGeodetic(p)
    val cl = math.cos(latRad); val sl = math.sin(latRad)
    val co = math.cos(lonRad); val so = math.sin(lonRad)
    Vec3(cl * co, cl * so, sl)
  }
}
