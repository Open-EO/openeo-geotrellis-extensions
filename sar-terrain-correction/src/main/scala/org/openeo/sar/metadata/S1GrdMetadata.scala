package org.openeo.sar.metadata

import org.openeo.sar.orbit.OrbitInterpolator

/** Polarisation enum mapped to SAFE band naming. */
sealed trait Polarisation { def code: String }
object Polarisation {
  case object VV extends Polarisation { val code = "vv" }
  case object VH extends Polarisation { val code = "vh" }
  case object HH extends Polarisation { val code = "hh" }
  case object HV extends Polarisation { val code = "hv" }
  def parse(s: String): Polarisation = s.toLowerCase match {
    case "vv" => VV case "vh" => VH case "hh" => HH case "hv" => HV
    case other => throw new IllegalArgumentException(s"unknown polarisation $other")
  }
}

/** Per-polarisation metadata bundle. */
final case class PolarisationMetadata(
  pol: Polarisation,
  measurementUri: String,        // URI of measurement/*.tiff (for GeoTrellis RasterSource)
  sigmaLut: Lut2D,               // sigmaNought calibration LUT
  noiseLut: Lut2D,               // thermal noise LUT (same coord system)
  srgr: SrgrPolyList             // slant/ground range polynomials
)

/** Image timing & sampling parameters shared by all polarisations of one scene. */
final case class ImageTiming(
  firstLineUtcSecs: Double,      // azimuth time of line 0 (seconds relative to scene epoch)
  lineTimeInterval: Double,      // seconds per azimuth line
  numberOfLines: Int,
  numberOfPixels: Int,
  rangePixelSpacing: Double      // metres per ground-range pixel (GRD)
)

/** Full scene-level metadata required by the terrain-correction core. */
final case class S1GrdMetadata(
  sceneEpochUtcSecs: Double,                 // absolute UTC epoch (seconds since J2000 or similar)
  timing: ImageTiming,
  orbit: OrbitInterpolator,
  polarisations: Map[Polarisation, PolarisationMetadata]
)
