package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{GridExtent, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import org.openeo.geotrellis.layers.raster_source.NoDataRasterSource
import org.slf4j.LoggerFactory

import scala.collection.mutable

object CompositeRasterSource {
  private val logger = LoggerFactory.getLogger(classOf[CompositeRasterSource])
}

/**
 * A [[BandCompositeRasterSource]] that additionally exposes, per output band, which underlying physical
 * datasource ([[RasterSource.name]]) provides its pixel data.
 *
 * FileLayerProvider builds one of these per feature/date: `sources(i)` supplies the pixel data for output
 * band `i`, in exactly the order requested by the user (openSearchLinkTitlesWithBandId), with missing bands
 * already filled in as `NoDataRasterSource` placeholders. This replaces the former split between
 * BandCompositeRasterSource (1 band per source) and MultibandCompositeRasterSource (N bands per source) that
 * FileLayerProvider used to construct: both cases are now represented uniformly as "one RasterSource per
 * output band" (a source can simply appear more than once, e.g. wrapped as an IndexedRasterSource per band,
 * when several output bands come from the same physical multi-band file).
 *
 * `groupedBySource` lets RasterTileLoader regroup the bands by physical file (to avoid re-opening/re-reading
 * the same dataset once per band) without having to pattern-match on the concrete RasterSource subtype and
 * reverse-engineer the output band order, as it used to.
 *
 * Note on alignment: all bands of the same feature are constructed by FileLayerProvider against the same
 * target extent/resolution/CRS (see RasterSourceDefinition), so a single GridBounds/RasterRegion can safely
 * be shared across every underlying source of this composite, even when their native resolutions differ
 * (see ResampledRasterSource, which normalizes that transparently). `gridExtent` below asserts this invariant
 * defensively rather than silently relying on it.
 */
class CompositeRasterSource(override val sources: NonEmptyList[RasterSource],
                             override val crs: CRS,
                             override val attributes: Map[String, String] = Map.empty,
                             override val predefinedExtent: Option[GridExtent[Long]] = None,
                             val readFullTile: Boolean = false,
                             val softErrors: Boolean = false
                            ) extends BandCompositeRasterSource(sources, crs, attributes, predefinedExtent,
  readFullTile = readFullTile, softErrors = softErrors) {

  // Reading relies on every band sharing the same pixel grid (a single GridBounds/RasterRegion is reused
  // across all of them, see RasterTileLoader). This holds by construction in FileLayerProvider (every band
  // of a feature targets the same extent/resolution/CRS, see RasterSourceDefinition; differing native
  // resolutions are normalized away transparently by ResampledRasterSource). Verify this once per instance,
  // logging (rather than failing the job) if it's ever violated, so a future regression is diagnosable
  // instead of silently producing misaligned pixels.
  private lazy val alignmentWarningLogged: Boolean = {
    val ge = super.gridExtent
    val misaligned = sources.toList.filterNot(s => s.isInstanceOf[NoDataRasterSource] || s.gridExtent == ge)
    if (misaligned.nonEmpty) {
      CompositeRasterSource.logger.warn(
        s"CompositeRasterSource: expected all bands to share grid extent $ge, but found misaligned source(s): " +
          misaligned.map(s => s"${s.name} -> ${s.gridExtent}").mkString(", ")
      )
    }
    true
  }

  override def gridExtent: GridExtent[Long] = {
    alignmentWarningLogged
    super.gridExtent
  }

  /**
   * Groups the output bands by their underlying physical datasource name, preserving the order in which
   * datasources are first encountered. Each group carries the true output band index (position in `sources`,
   * i.e. the final position in the resulting MultibandTile) alongside the RasterSource for that band, so
   * callers never need to re-derive it.
   */
  def groupedBySource: Seq[(SourceName, Seq[(Int, RasterSource)])] = {
    val order = mutable.LinkedHashMap[SourceName, mutable.ArrayBuffer[(Int, RasterSource)]]()
    sources.toList.zipWithIndex.foreach { case (source, outputBandIndex) =>
      order.getOrElseUpdate(source.name, mutable.ArrayBuffer()) += ((outputBandIndex, source))
    }
    order.toSeq.map { case (name, buf) => (name, buf.toSeq) }
  }

  // Preserve the CompositeRasterSource subtype (and hence groupedBySource) across these transformations;
  // BandCompositeRasterSource's inherited overrides would otherwise downcast to plain BandCompositeRasterSource,
  // e.g. via RasterSource.tileToLayout, silently disabling the per-product read optimization in RasterTileLoader.
  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new CompositeRasterSource(sources map {
      _.resample(resampleTarget, method, strategy)
    }, crs, attributes, predefinedExtent, readFullTile = readFullTile, softErrors = softErrors)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new CompositeRasterSource(sources map {
      _.convert(targetCellType)
    }, crs, attributes, predefinedExtent, readFullTile = readFullTile, softErrors = softErrors)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new CompositeRasterSource(sources map {
      _.reproject(targetCRS, resampleTarget, method, strategy)
    }, crs, attributes, predefinedExtent, readFullTile = readFullTile, softErrors = softErrors)

  override def toString: String = s"CompositeRasterSource(${sources.toList}, $crs, $gridExtent, $name)"
}
