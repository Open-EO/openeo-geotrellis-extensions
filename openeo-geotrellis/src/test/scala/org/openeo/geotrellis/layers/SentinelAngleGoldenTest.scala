package org.openeo.geotrellis.layers

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Assumptions, Test}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields.{Angle, sunField, viewField}
import org.openeo.geotrellis.layers.raster_source.SentinelAngleRasterSource.{tileAngles, viewReferenceBandId}
import org.openeo.geotrellis.layers.raster_source.SentinelDetectorMask.detectorLabels

import scala.io.Source

/**
 * Tier-1 checks of issue #916: the per-pixel angles must equal those of an independent NumPy implementation of the
 * same method (`sentinel2-angle-golden/generate_golden.py`) at 93 points of two real L2A tiles.
 */
object SentinelAngleGoldenTest {
  private val layers = "/org/openeo/geotrellis/layers"
  private val tolerance = 1e-4

  private def xmlPath(tile: String): String = getClass.getResource(
    s"$layers/S2B_MSIL2A_20220502T103619_N0400_R008_${tile}_20220502T123919.SAFE" +
      s"/GRANULE/L2A_${tile}_A026918_20220502T103700/MTD_TL.xml").getPath

  final case class GoldenPoint(tile: String, kind: String, x: Double, y: Double, label: Int,
                               sunZenith: Double, sunAzimuth: Double, viewZenith: Double, viewAzimuth: Double)

  private lazy val golden: Seq[GoldenPoint] = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(s"$layers/sentinel2-angle-golden/golden.csv"), "UTF-8")
    try {
      val lines = source.getLines().toList
      assertEquals("tile,kind,x,y,label,sun_zenith,sun_azimuth,view_zenith,view_azimuth", lines.head)
      lines.tail.map { line =>
        val f = line.split(",", -1)
        assertEquals(9, f.length, line)
        GoldenPoint(f(0), f(1), f(2).toDouble, f(3).toDouble, f(4).toInt, f(5).toDouble, f(6).toDouble, f(7).toDouble,
          f(8).toDouble)
      }
    } finally source.close()
  }

  private def byTile: Map[String, Seq[GoldenPoint]] = golden.groupBy(_.tile)

  private def circularDifference(a: Double, b: Double): Double = {
    val d = math.abs(a - b) % 360
    math.min(d, 360 - d)
  }

  /** The angle in degrees between two viewing directions, each given by zenith and azimuth in degrees. */
  private def angleBetween(zenith1: Double, azimuth1: Double, zenith2: Double, azimuth2: Double): Double = {
    def unit(zenith: Double, azimuth: Double): Array[Double] = {
      val (z, a) = (math.toRadians(zenith), math.toRadians(azimuth))
      Array(math.sin(z) * math.sin(a), math.sin(z) * math.cos(a), math.cos(z))
    }
    val (p, q) = (unit(zenith1, azimuth1), unit(zenith2, azimuth2))
    val cross = math.sqrt(
      math.pow(p(1) * q(2) - p(2) * q(1), 2) + math.pow(p(2) * q(0) - p(0) * q(2), 2) + math.pow(p(0) * q(1) - p(1) * q(0), 2))
    math.toDegrees(math.atan2(cross, p(0) * q(0) + p(1) * q(1) + p(2) * q(2)))
  }

  /**
   * Checks every point: NoData at the same points, and `difference` at most [[tolerance]] elsewhere. Returns the
   * largest difference.
   */
  private def compare(what: String, points: Seq[GoldenPoint], expected: GoldenPoint => Double, actual: Int => Double,
                      difference: (Int, GoldenPoint) => Double): Double = {
    val failures = points.indices.flatMap { k =>
      val point = points(k)
      if (expected(point).isNaN || actual(k).isNaN) {
        if (expected(point).isNaN == actual(k).isNaN) None
        else Some(s"$point: $what is ${actual(k)}, expected ${expected(point)}")
      } else {
        val d = difference(k, point)
        if (d <= tolerance) None else Some(s"$point: $what is ${actual(k)}, differs by $d")
      }
    }
    assertTrue(failures.isEmpty, failures.mkString(s"$what: ${failures.size} points differ:\n", "\n", ""))
    points.indices.filterNot(k => expected(points(k)).isNaN).map(k => difference(k, points(k))).foldLeft(0.0)(math.max)
  }

  private lazy val gdalAvailable: Boolean =
    try {
      com.azavea.gdal.GDALWarp.get_version_info("VERSION_NUM")
      true
    } catch {
      case _: LinkageError => false
    }
}

class SentinelAngleGoldenTest {

  import SentinelAngleGoldenTest._

  @Test
  def goldenPointsCoverBothTilesAndTheSpecialCases(): Unit = {
    assertEquals(93, golden.size)
    assertEquals(Set("T31UFS", "T31UGS"), byTile.keySet)
    assertEquals(Set("random", "detector_edge", "pixel_edge", "grid_node", "filled_node", "no_detector", "outside_tile",
      "near_nadir"), golden.map(_.kind).toSet)
    assertTrue(golden.exists(p => p.viewZenith < 1.0), "no viewing zenith below 1 degree")
  }

  @Test
  def sunAnglesMatchTheIndependentReference(): Unit =
    for ((tile, points) <- byTile) {
      val metadata = tileAngles(xmlPath(tile)).metadata
      val sun = metadata.sunGrid.fold(message => throw new AssertionError(message), identity)
      val (xs, ys) = (points.map(_.x).toArray, points.map(_.y).toArray)
      val zenith = sunField(sun, Angle.Zenith, metadata.extent, xs, ys)
      val azimuth = sunField(sun, Angle.Azimuth, metadata.extent, xs, ys)

      val maxZenith = compare(s"$tile sun zenith", points, _.sunZenith, k => zenith(k).toDouble,
        (k, p) => math.abs(zenith(k) - p.sunZenith))
      val maxAzimuth = compare(s"$tile sun azimuth", points, _.sunAzimuth, k => azimuth(k).toDouble,
        (k, p) => circularDifference(azimuth(k), p.sunAzimuth))
      println(f"$tile: largest sun zenith difference $maxZenith%.2e, sun azimuth $maxAzimuth%.2e degrees")
    }

  @Test
  def viewAnglesMatchTheIndependentReferenceForItsDetectorLabels(): Unit =
    for ((tile, points) <- byTile) {
      val angles = tileAngles(xmlPath(tile))
      val grids = angles.filledViewGrids(viewReferenceBandId)
      val (xs, ys, labels) = (points.map(_.x).toArray, points.map(_.y).toArray, points.map(_.label).toArray)
      val zenith = viewField(grids, Angle.Zenith, angles.metadata.extent, xs, ys, labels)
      val azimuth = viewField(grids, Angle.Azimuth, angles.metadata.extent, xs, ys, labels)

      val maxZenith = compare(s"$tile view zenith", points, _.viewZenith, k => zenith(k).toDouble,
        (k, p) => math.abs(zenith(k) - p.viewZenith))
      // Azimuth is ill-conditioned near nadir, so compare viewing directions; NoData must match for azimuth too.
      val maxDirection = compare(s"$tile view direction", points, _.viewAzimuth, k => azimuth(k).toDouble,
        (k, p) => angleBetween(zenith(k), azimuth(k), p.viewZenith, p.viewAzimuth))
      println(f"$tile: largest view zenith difference $maxZenith%.2e, view direction $maxDirection%.2e degrees")
    }

  @Test
  def realJp2MasksGiveTheReferenceDetectorLabels(): Unit = {
    Assumptions.assumeTrue(gdalAvailable, "GDAL (libgdal) is not installed here")
    for ((tile, points) <- byTile) {
      val angles = tileAngles(xmlPath(tile))
      val viewing = angles.metadata.viewing(viewReferenceBandId).fold(message => throw new AssertionError(message), identity)
      val labels = detectorLabels(xmlPath(tile), angles.metadata, viewReferenceBandId, viewing.detectorMask,
        points.map(_.x).toArray, points.map(_.y).toArray)
      assertArrayEquals(points.map(_.label).toArray, labels, s"$tile detector labels")
    }
  }
}
