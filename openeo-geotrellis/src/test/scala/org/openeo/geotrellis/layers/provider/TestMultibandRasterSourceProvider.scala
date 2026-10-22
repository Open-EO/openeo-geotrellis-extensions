package org.openeo.geotrellis.layers.provider

import cats.data.NonEmptyList
import geotrellis.raster.{GridExtent, RasterSource}
import org.openeo.geotrellis.layers.raster_source.IndexedRasterSource

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test-only [[RasterSourceProvider]] that mimics the "whole scene" strategy used by e.g.
 * `Sentinel1GrdRasterSourceProvider`: it opens one physical, multi-band [[RasterSource]] per scene and
 * serves several output bands from it in one go, via `multibandRasterSource`, instead of the default
 * per-band `rasterSource` path.
 *
 * A `RasterSourceDefinition` is recognized by this provider when its `dataPath` (derived from the
 * asset's href) matches `test-multiband://<sceneId>#band=<physicalBandIndex>`. `<physicalBandIndex>`
 * identifies the band within the physical scene, independently of the order in which output bands are
 * requested - this is exactly what allows the test to verify that bands come back in the *requested*
 * order rather than in physical band order.
 *
 * Registered for `ServiceLoader` discovery via
 * `src/test/resources/META-INF/services/org.openeo.geotrellis.layers.provider.RasterSourceProvider`.
 * Because instances are created by `ServiceLoader` (no way for a test to reach into "the" instance used
 * by `FileLayerProvider`), all test-observable state lives in the companion object instead, keyed by
 * scene id.
 */
object TestMultibandRasterSourceProvider {
  private val HrefPattern = "test-multiband://([^#]+)#band=(\\d+)".r

  /** Constant per-band values of the physical scene: band 0 -> 10.0, band 1 -> 20.0, band 2 -> 30.0. */
  val PhysicalBandValues: Seq[Double] = Seq(10.0, 20.0, 30.0)

  private val physicalSources = new ConcurrentHashMap[String, RasterSource]()
  private val openCounts = new ConcurrentHashMap[String, AtomicInteger]()

  /** `test-multiband://<sceneId>#band=<physicalBandIndex>` */
  def href(sceneId: String, physicalBandIndex: Int): URI = URI.create(s"test-multiband://$sceneId#band=$physicalBandIndex")

  private def parse(dataPath: String): Option[(String, Int)] = dataPath match {
    case HrefPattern(sceneId, bandIndex) => Some((sceneId, bandIndex.toInt))
    case _ => None
  }

  /**
   * Clears all cached physical sources/open counts/recorded read calls. Call before each test run so
   * that scenes from earlier tests (typically using different scene ids anyway) can never leak into a
   * later assertion.
   */
  def reset(): Unit = {
    physicalSources.clear()
    openCounts.clear()
    ConstantMultibandRasterSource.resetReadCalls()
  }

  /** Number of times the physical source for `sceneId` has been constructed (expected to be exactly 1). */
  def openCount(sceneId: String): Int = Option(openCounts.get(sceneId)).map(_.get()).getOrElse(0)

  /** The physical [[RasterSource]] backing `sceneId`, if it has been opened yet. */
  def physicalSource(sceneId: String): Option[RasterSource] = Option(physicalSources.get(sceneId))

  private def openScene(sceneId: String, gridExtent: GridExtent[Long], crs: geotrellis.proj4.CRS, bandValues: Seq[Double]): RasterSource =
    physicalSources.computeIfAbsent(sceneId, _ => {
      openCounts.computeIfAbsent(sceneId, _ => new AtomicInteger(0)).incrementAndGet()
      ConstantMultibandRasterSource(sceneId, gridExtent, crs, bandValues)
    })
}

class TestMultibandRasterSourceProvider extends RasterSourceProvider {
  import TestMultibandRasterSourceProvider._

  override def canProcess(definition: RasterSourceDefinition): Boolean =
    parse(definition.dataPath).isDefined

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val (sceneId, physicalBandIndex) = parse(definition.dataPath).get
    val physical = openScene(sceneId, gridExtentOf(definition), definition.targetExtent.crs, bandValuesOf(definition))
    IndexedRasterSource(physical, physicalBandIndex)
  }

  override def multibandRasterSource(definitions: NonEmptyList[RasterSourceDefinition]): Option[(RasterSource, Seq[Int])] = {
    val defs = definitions.toList
    val parsed = defs.map(d => parse(d.dataPath))
    if (parsed.exists(_.isEmpty)) None
    else {
      val sceneId = parsed.head.get._1
      val physical = openScene(sceneId, gridExtentOf(defs.head), defs.head.targetExtent.crs, bandValuesOf(defs.head))
      Some((physical, parsed.map(_.get._2)))
    }
  }

  private def gridExtentOf(definition: RasterSourceDefinition): GridExtent[Long] = {
    val re = geotrellis.raster.RasterExtent(definition.targetExtent.extent, definition.theResolution)
    re.toGridType[Long]
  }

  // The number of bands (and their constant values) making up the physical scene is fixed for this test
  // provider: band 0 -> 10.0, band 1 -> 20.0, band 2 -> 30.0.
  private def bandValuesOf(definition: RasterSourceDefinition): Seq[Double] = TestMultibandRasterSourceProvider.PhysicalBandValues
}
