package org.openeo.geotrellis.layers.provider

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ArrayTile, CellSize, CellType, DoubleConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent

import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import scala.jdk.CollectionConverters._

/**
 * Records physical `read(..., bands)` calls made to [[ConstantMultibandRasterSource]] instances, keyed
 * by scene id. A plain Mockito spy/mock cannot be used for this purpose here: raster sources end up
 * inside Spark task closures (via `CompositeRasterSource`/`Feature`) that get Kryo-serialized even in
 * local mode, and Mockito's dynamic proxies carry non-serializable invocation-history state (regardless
 * of `withSettings().serializable()`) that breaks Kryo. Recording calls directly in the source's own
 * (plain, Kryo-friendly) code, into a JVM-static companion-object registry, sidesteps that entirely -
 * this works for `local[*]` Spark tests because driver and "executors" share one JVM.
 */
object ConstantMultibandRasterSource {
  private val reads = new ConcurrentHashMap[String, CopyOnWriteArrayList[Seq[Int]]]()

  private def recordsFor(id: String): CopyOnWriteArrayList[Seq[Int]] =
    reads.computeIfAbsent(id, _ => new CopyOnWriteArrayList[Seq[Int]]())

  /** All `bands` argument lists passed to `read(...)` for scene `id`, in call order. */
  def readCalls(id: String): Seq[Seq[Int]] = recordsFor(id).asScala.toSeq

  def resetReadCalls(): Unit = reads.clear()
}

/**
 * Minimal test-only [[RasterSource]] with a fixed number of bands, each filled with a distinct constant
 * value. Used by [[TestMultibandRasterSourceProvider]] to simulate a "whole scene" physical source that
 * serves several output bands together, so that tests can verify:
 *  - that output bands end up in the requested order (not the physical band order), and
 *  - (via [[ConstantMultibandRasterSource.readCalls]]) how many times, and with which band indices,
 *    the physical source is actually read.
 */
case class ConstantMultibandRasterSource(id: String, gridExtent: GridExtent[Long], override val crs: CRS, bandValues: Seq[Double])
  extends RasterSource {

  override def metadata: RasterMetadata = this

  override val targetCellType: Option[TargetCellType] = None

  override def bandCount: Int = bandValues.size

  override def cellType: CellType = DoubleConstantNoDataCellType

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map()

  override def attributesForBand(band: Int): Map[String, String] = Map()

  override def name: SourceName = id

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    ConstantMultibandRasterSource(id, gridExtent.reproject(crs, targetCRS), targetCRS, bandValues)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    ConstantMultibandRasterSource(id, resampleTarget(gridExtent), crs, bandValues)

  override def convert(targetCellType: TargetCellType): RasterSource = this

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    ConstantMultibandRasterSource.recordsFor(id).add(bands)
    extent.intersection(gridExtent.extent).map { clipped =>
      val gb = gridExtent.gridBoundsFor(clipped).toGridType[Int]
      Raster(constantTile(gb.width, gb.height, bands), clipped)
    }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    ConstantMultibandRasterSource.recordsFor(id).add(bands)
    bounds.intersection(gridExtent.dimensions).map { intersection =>
      val gb = intersection.toGridType[Int]
      Raster(constantTile(gb.width, gb.height, bands), gridExtent.extentFor(intersection))
    }
  }

  private def constantTile(cols: Int, rows: Int, bands: Seq[Int]): MultibandTile =
    MultibandTile(bands.map(b => ArrayTile(Array.fill(cols * rows)(bandValues(b)), cols, rows): Tile))

  override def toString: String = f"${getClass.getSimpleName}($id, $gridExtent, $crs, $bandValues)"
}

