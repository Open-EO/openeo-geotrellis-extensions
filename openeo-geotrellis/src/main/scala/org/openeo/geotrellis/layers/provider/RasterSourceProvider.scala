package org.openeo.geotrellis.layers.provider

import cats.data.NonEmptyList
import geotrellis.raster.RasterSource

trait RasterSourceProvider {

  def canProcess(rasterSourceDefinition: RasterSourceDefinition): Boolean

  def rasterSource(rasterSourceDefinition: RasterSourceDefinition): RasterSource

  def usePredefinedExtent(rasterSourceDefinition: RasterSourceDefinition): Boolean = false

  /**
   * Optional optimization for providers that can serve several output bands of one feature from a
   * single underlying physical source in one go (e.g. all polarisations of a SAR scene, computed
   * together by one terrain-correction pass). When implemented, `FileLayerProvider` calls this once
   * per feature - for the contiguous run of bands that this provider claims via `canProcess` - instead
   * of invoking `rasterSource` once per band, avoiding redundant re-opening/re-processing of the same
   * underlying data per band.
   *
   * Note: unlike the default per-band path, per-band pixel value scale/offset and cell type overrides
   * (see `ValueOffsetRasterSource`) are NOT applied to sources returned here; the provider is expected
   * to produce final, already-calibrated output bands.
   *
   * @param definitions one `RasterSourceDefinition` per requested output band that this provider
   *                     claims via `canProcess`, in output band order.
   * @return `Some((source, bandIndices))` where `source` is a single multi-band `RasterSource` and
   *         `bandIndices(i)` is the band index within `source` corresponding to `definitions.toList(i)`;
   *         `None` to fall back to the default per-band strategy.
   */
  def multibandRasterSource(definitions: NonEmptyList[RasterSourceDefinition]): Option[(RasterSource, Seq[Int])] = None
}
