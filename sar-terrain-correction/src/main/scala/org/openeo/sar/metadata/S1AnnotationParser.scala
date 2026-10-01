package org.openeo.sar.metadata

import org.openeo.sar.geom.Vec3
import org.openeo.sar.io.UriIO
import org.openeo.sar.orbit.{OrbitInterpolator, StateVector}

import java.net.URI
import java.time.{ZoneOffset, ZonedDateTime}
import scala.xml.Elem

/** Parses the Sentinel-1 GRD annotation XML files
 *  (annotation/s1*-grd-{pol}-*.xml, annotation/calibration/calibration-*.xml,
 *  annotation/calibration/noise-*.xml).
 *
 *  Reads them as plain HTTP(S)/file URIs - no SAFE-zip handling here. When the
 *  STAC item exposes the SAFE as an unzipped object-store prefix (CDSE does
 *  for many collections), all annotation paths are simple URI joins. */
object S1AnnotationParser {

  private def parseUtc(s: String): Double = {
    // S1 UTC strings look like "2023-04-05T05:12:34.567890"
    val z = if (s.endsWith("Z")) s else s + "Z"
    ZonedDateTime.parse(z).withZoneSameInstant(ZoneOffset.UTC).toInstant.toEpochMilli / 1000.0
  }

  /** Parse one product annotation XML (one per polarisation). */
  def parseProductAnnotation(uri: URI, measurementUri: URI,
                             calibrationUri: URI, noiseUri: URI,
                             pol: Polarisation): (PolarisationMetadata, ImageTiming, IndexedSeq[StateVector], Double) = {
    val root: Elem = UriIO.loadXml(uri)
    val imgInfo = root \ "imageAnnotation" \ "imageInformation"
    val firstLineUtc = parseUtc((imgInfo \ "productFirstLineUtcTime").text)
    val lineTimeInterval = (imgInfo \ "azimuthTimeInterval").text.toDouble
    val nLines  = (imgInfo \ "numberOfLines").text.toInt
    val nPixels = (imgInfo \ "numberOfSamples").text.toInt
    val rgSpacing = (imgInfo \ "rangePixelSpacing").text.toDouble

    val timing = ImageTiming(0.0, lineTimeInterval, nLines, nPixels, rgSpacing)

    val orbitNodes = root \ "generalAnnotation" \ "orbitList" \ "orbit"
    val svs: IndexedSeq[StateVector] = orbitNodes.map { o =>
      val t = parseUtc((o \ "time").text) - firstLineUtc
      val p = o \ "position"; val v = o \ "velocity"
      StateVector(
        t,
        Vec3((p \ "x").text.toDouble, (p \ "y").text.toDouble, (p \ "z").text.toDouble),
        Vec3((v \ "x").text.toDouble, (v \ "y").text.toDouble, (v \ "z").text.toDouble)
      )
    }.toIndexedSeq

    val srgr = parseSrgr(root, firstLineUtc)
    val sigmaLut = parseCalibrationLut(calibrationUri, "sigmaNought")
    val noiseLut = parseNoiseLut(noiseUri)

    val polMeta = PolarisationMetadata(pol, measurementUri.toString, sigmaLut, noiseLut, srgr)
    (polMeta, timing, svs, firstLineUtc)
  }

  private def parseSrgr(root: Elem, firstLineUtc: Double): SrgrPolyList = {
    val recs = (root \ "coordinateConversion" \ "coordinateConversionList" \ "coordinateConversion").map { r =>
      val t = parseUtc((r \ "azimuthTime").text) - firstLineUtc
      val sr0 = (r \ "sr0").text.toDouble
      val gr0 = (r \ "gr0").text.toDouble
      // The SAFE field "grsrCoefficients" maps GROUND range to SLANT range:
      // slantRange(gr) = sum_k a_k * (gr - gr0)^k
      val coeffs = (r \ "grsrCoefficients").text.trim.split("\\s+").map(_.toDouble)
      SrgrPoly(t, sr0, gr0, coeffs)
    }.toIndexedSeq
    new SrgrPolyList(recs)
  }

  private def parseCalibrationLut(uri: URI, field: String): Lut2D = {
    val root = UriIO.loadXml(uri)
    val vectors = root \ "calibrationVectorList" \ "calibrationVector"
    val lines = vectors.map(v => (v \ "line").text.toInt).toArray
    val pixels = vectors.head \ "pixel" match {
      case n => n.text.trim.split("\\s+").map(_.toInt)
    }
    val values: Array[Array[Float]] = vectors.map { v =>
      (v \ field).text.trim.split("\\s+").map(_.toFloat)
    }.toArray
    new Lut2D(lines, pixels, values)
  }

  private def parseNoiseLut(uri: URI): Lut2D = {
    val root = UriIO.loadXml(uri)
    // S1 IPF >= 2.9 uses noiseRangeVectorList; older products used noiseVectorList.
    val vectors = (root \ "noiseRangeVectorList" \ "noiseRangeVector") match {
      case s if s.nonEmpty => s
      case _               => root \ "noiseVectorList" \ "noiseVector"
    }
    val lines = vectors.map(v => (v \ "line").text.toInt).toArray
    val pixels = vectors.head \ "pixel" match {
      case n => n.text.trim.split("\\s+").map(_.toInt)
    }
    val values: Array[Array[Float]] = vectors.map { v =>
      val tag = if ((v \ "noiseRangeLut").nonEmpty) "noiseRangeLut" else "noiseLut"
      (v \ tag).text.trim.split("\\s+").map(_.toFloat)
    }.toArray
    new Lut2D(lines, pixels, values)
  }

  /** Merge per-polarisation parses into the unified scene metadata. */
  def assemble(perPol: Seq[(PolarisationMetadata, ImageTiming, IndexedSeq[StateVector], Double)]): S1GrdMetadata = {
    require(perPol.nonEmpty)
    val (_, timing, svs, firstLineUtc) = perPol.head
    val orbit = new OrbitInterpolator(svs)
    val polMap = perPol.map { case (pm, _, _, _) => pm.pol -> pm }.toMap
    S1GrdMetadata(firstLineUtc, timing.copy(firstLineUtcSecs = 0.0), orbit, polMap)
  }
}
