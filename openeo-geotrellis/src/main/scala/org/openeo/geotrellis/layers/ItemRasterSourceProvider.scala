package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.{RasterExtent, RasterSource}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.Feature

trait ItemRasterSourceProvider {

  def canProcess(item: Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean

  def getRasterSource(item: Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters] = Option.empty): Option[RasterSource]
}
