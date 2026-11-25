package org.openeo.sar.backend.nativ.rtc

import org.openeo.sar.geom.{Ecef, RangeDoppler, Vec3}
import org.openeo.sar.orbit.OrbitInterpolator

import scala.collection.parallel.CollectionConverters._

/** Area-based radiometric terrain correction (RTC): the "bilinear distribution" algorithm.
 *
 *  This is a Scala port of `isce3::geometry::computeRtcBilinearDistribution` in
 *  `cxx/isce3/geometry/RTC.cpp` of ISCE3 (https://github.com/isce-framework/isce3), restricted to the
 *  `AREA_FACTOR` mode with gamma0 output and beta0 input. Function and variable names follow the original where
 *  possible. ISCE3 is Copyright (C) 2008-2019 California Institute of Technology, licensed under the Apache License,
 *  Version 2.0; see the NOTICE file of this repository.
 *
 *  The method is the one of
 *   - D. Small, "Flattening Gamma: Radiometric Terrain Correction for SAR Imagery",
 *     IEEE Trans. Geosci. Remote Sens. 49(8):3081-3093, 2011, doi:10.1109/TGRS.2011.2120616, and
 *   - M. Shiroma, M. Lavalle, S. Buckley, "An Area-Based Projection Algorithm for SAR Radiometric Terrain
 *     Correction and Geocoding", IEEE Trans. Geosci. Remote Sens. 60:5222723, 2022,
 *     doi:10.1109/TGRS.2022.3147472 (the area projection variant, not ported yet),
 *  and is what OPERA RTC-S1 / NISAR GCOV use.
 *
 *  The terrain is split into triangular facets (two per DEM cell). The centre of each cell is mapped into radar
 *  geometry (zero-Doppler azimuth line, slant range), and the facet's "gamma" area (facet area times the cosine of
 *  its incidence angle) is distributed over the 4 surrounding radar pixels with bilinear weights. Dividing by the
 *  beta-plane pixel area gives the area normalisation factor (ANF) per radar pixel. Gamma0 = beta0 / ANF. In
 *  contrast to the angle-based method (see [[org.openeo.sar.backend.TerrainCorrectionBackend.gamma0RtcFactor]]) all
 *  facets that fall in one radar pixel are summed, which is correct in layover and on steep slopes.
 *
 *  Divergences from ISCE3: zero-Doppler geometry only (as Sentinel-1 GRD is zero-Doppler); facet corners are the
 *  DEM samples themselves rather than biquintic-interpolated DEM values; the facet centre is the mean of the 4
 *  corners; facet normals are oriented away from the Earth's centre instead of by the sign convention of
 *  `normalPlane`.
 */
object AreaRtc {

  /** Virtual radar grid in which the area normalisation factor is accumulated.
   *
   *  Index axes: azimuth `line - azLine0` and slant range `(r - rMin) / dr`; integer indices are pixel centres,
   *  as in ISCE3. */
  final case class RadarGrid(azLine0: Double, nAz: Int, rMin: Double, dr: Double, nRg: Int,
                             lineTimeInterval: Double) {
    def size: Long = nAz.toLong * nRg
  }

  /** The area normalisation factor per radar pixel (row-major, `nAz` rows). 0 where no illuminated facet fell. */
  final case class Anf(grid: RadarGrid, values: Array[Double]) {

    /** Bilinear sample at a fractional (azimuth, range) index; NaN if outside the grid, in shadow
     *  (any of the 4 neighbours has an ANF <= 0) or below `minValue` (ISCE3 `rtc_min_value_db`). */
    def sample(azIdx: Double, rgIdx: Double, minValue: Double): Double = {
      val a0 = math.floor(azIdx).toInt; val r0 = math.floor(rgIdx).toInt
      if (a0 < 0 || r0 < 0 || a0 + 1 >= grid.nAz || r0 + 1 >= grid.nRg) return Double.NaN
      val ta = azIdx - a0; val tr = rgIdx - r0
      val i00 = a0 * grid.nRg + r0
      val v00 = values(i00); val v01 = values(i00 + 1)
      val v10 = values(i00 + grid.nRg); val v11 = values(i00 + grid.nRg + 1)
      if (!(v00 > 0 && v01 > 0 && v10 > 0 && v11 > 0)) return Double.NaN
      val v = (v00 * (1 - tr) + v01 * tr) * (1 - ta) + (v10 * (1 - tr) + v11 * tr) * ta
      if (v < minValue) Double.NaN else v
    }
  }

  /** Port of `isce3::geometry::computeRtcBilinearDistribution` (RTC.cpp, ISCE3 v0.26).
   *
   *  @param heights   ellipsoidal heights (m) of the DEM vertices, `[row][col]`; NaN = unknown. Rows run
   *                   north to south. Each 4 neighbouring vertices form a cell of 2 facets.
   *  @param lonLatRad (row, col) of a vertex => (lon, lat) in radians
   *  @param grid      radar grid to accumulate in
   *  @param orbit     platform orbit; its time axis is the one of `grid.lineTimeInterval`
   *  @param tSeed     seed for the zero-Doppler solve (azimuth time)
   *  @param nChunks   number of independent row blocks (each with its own accumulation grid) to process in parallel
   */
  def computeRtcBilinearDistribution(heights: Array[Array[Float]],
                                     lonLatRad: (Int, Int) => (Double, Double),
                                     grid: RadarGrid,
                                     orbit: OrbitInterpolator,
                                     tSeed: Double,
                                     nChunks: Int = Runtime.getRuntime.availableProcessors()): Anf = {
    val nVertexRows = heights.length
    if (nVertexRows < 2 || heights(0).length < 2) return Anf(grid, new Array[Double](grid.size.toInt))
    val nCellRows = nVertexRows - 1
    val chunkCount = math.max(1, math.min(nChunks, nCellRows))
    val bounds = (0 to chunkCount).map(k => (k.toLong * nCellRows / chunkCount).toInt)

    val partials = Array.range(0, chunkCount).par.map { k =>
      val out = new Array[Double](grid.size.toInt)
      accumulateRows(heights, lonLatRad, grid, orbit, tSeed, bounds(k), bounds(k + 1), out)
      out
    }.toArray

    val total = partials.head
    var k = 1
    while (k < partials.length) {
      val p = partials(k)
      var i = 0
      while (i < total.length) { total(i) += p(i); i += 1 }
      k += 1
    }
    Anf(grid, total)
  }

  private def vertexXyz(heights: Array[Array[Float]], lonLatRad: (Int, Int) => (Double, Double),
                        row: Int): Array[Vec3] = {
    val h = heights(row)
    Array.tabulate(h.length) { c =>
      if (java.lang.Float.isNaN(h(c))) null
      else { val (lon, lat) = lonLatRad(row, c); Ecef.fromGeodetic(lon, lat, h(c)) }
    }
  }

  /** The loop over the DEM facets of `computeRtcBilinearDistribution`, for the cell rows `[iBegin, iEnd)`. */
  private def accumulateRows(heights: Array[Array[Float]], lonLatRad: (Int, Int) => (Double, Double),
                             grid: RadarGrid, orbit: OrbitInterpolator, tSeed: Double,
                             iBegin: Int, iEnd: Int, out: Array[Double]): Unit = {
    val nCols = heights(0).length
    val xbound = grid.nRg - 1.0
    val ybound = grid.nAz - 1.0

    var upper = vertexXyz(heights, lonLatRad, iBegin)
    var ii = iBegin
    while (ii < iEnd) {
      val lower = vertexXyz(heights, lonLatRad, ii + 1)
      // The previous solution is the initial guess for the next one, as in ISCE3.
      var a = tSeed

      var jj = 0
      while (jj < nCols - 1) {
        // xyzRC: corner at row offset R, column offset C (xyz01 is the south-west corner, as in ISCE3).
        val xyz00 = upper(jj); val xyz10 = upper(jj + 1)
        val xyz01 = lower(jj); val xyz11 = lower(jj + 1)

        if (xyz00 != null && xyz10 != null && xyz01 != null && xyz11 != null) {
          val xyzMid = (xyz00 + xyz01 + xyz10 + xyz11) * 0.25

          // geo2rdr of the facet centre (zero-Doppler).
          a = RangeDoppler.zeroDopplerTime(xyzMid, orbit, a)
          val (xyzPlat, vel) = orbit.stateAt(a)
          val r = (xyzPlat - xyzMid).norm

          val azpix = a / grid.lineTimeInterval - grid.azLine0
          val ranpix = (r - grid.rMin) / grid.dr

          val x1 = math.floor(ranpix).toInt; val x2 = x1 + 1
          val y1 = math.floor(azpix).toInt; val y2 = y1 + 1

          // Check to see if the pixel lies in the valid radar grid
          if (!(ranpix < -1 || x2 > xbound + 1 || azpix < -1 || y2 > ybound + 1)) {

            // Normal vectors of the two facets, pointing away from the Earth's centre.
            val normalFacet1 = outwardNormal(xyz00, xyz01, xyz10, xyzMid)
            val normalFacet2 = outwardNormal(xyz01, xyz11, xyz10, xyzMid)

            // Heron's formula for the area of the facets in XYZ coordinates
            val AP1 = triangleArea(xyz00, xyz01, xyz10)
            val AP2 = triangleArea(xyz01, xyz11, xyz10)

            // Look vector from the ground to the sensor
            val lookXYZ = (xyzPlat - xyzMid).normalize

            val cosIncFacet1 = lookXYZ.dot(normalFacet1)
            val cosIncFacet2 = lookXYZ.dot(normalFacet2)

            // If the facets are not illuminated by the radar, skip.
            if (!(cosIncFacet1 <= 0.0 && cosIncFacet2 <= 0.0)) {
              // Projected ("gamma") area
              var area = 0.0
              if (cosIncFacet1 > 0) area += AP1 * cosIncFacet1
              if (cosIncFacet2 > 0) area += AP2 * cosIncFacet2

              if (area != 0.0) {
                // AREA_FACTOR: normalise by the beta-plane pixel area. Cosine law: c^2 = a^2 + b^2 - 2ab cos(AB)
                val radiusTarget = xyzMid.norm
                val radiusPlatform = xyzPlat.norm
                val cosAlpha = (radiusTarget * radiusTarget + radiusPlatform * radiusPlatform - r * r) /
                  (2 * radiusTarget * radiusPlatform)
                val groundVelocity = cosAlpha * radiusTarget * vel.norm / radiusPlatform
                val areaBeta = grid.dr * groundVelocity * grid.lineTimeInterval
                area /= areaBeta

                // Fractional weights from the indices
                val Wr = ranpix - x1; val Wa = azpix - y1
                val Wrc = 1.0 - Wr; val Wac = 1.0 - Wa
                val nRg = grid.nRg

                if (y1 >= 0 && x1 >= 0) out(y1 * nRg + x1) += area * Wrc * Wac
                if (y1 >= 0 && x2 <= xbound) out(y1 * nRg + x2) += area * Wr * Wac
                if (y2 <= ybound && x1 >= 0) out(y2 * nRg + x1) += area * Wrc * Wa
                if (y2 <= ybound && x2 <= xbound) out(y2 * nRg + x2) += area * Wr * Wa
              }
            }
          }
        }
        jj += 1
      }
      upper = lower
      ii += 1
    }
  }

  private def triangleArea(p0: Vec3, p1: Vec3, p2: Vec3): Double = {
    val a = (p0 - p1).norm; val b = (p0 - p2).norm; val c = (p2 - p1).norm
    val h = 0.5 * (a + b + c)
    math.sqrt(math.max(0.0, h * (h - a) * (h - b) * (h - c)))
  }

  private def outwardNormal(p0: Vec3, p1: Vec3, p2: Vec3, up: Vec3): Vec3 = {
    val n = (p1 - p0).cross(p2 - p0).normalize
    if (n.dot(up) < 0) n * -1.0 else n
  }
}
