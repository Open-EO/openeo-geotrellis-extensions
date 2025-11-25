package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, RasterExtent, RasterSource}
import org.openeo.geotrelliscommon.{DataCubeParameters, SyntheticDataOverride}
import org.openeo.opensearch.OpenSearchResponses

object SyntheticDataItemRasterSourceProvider extends SyntheticDataItemRasterSourceProvider

class SyntheticDataItemRasterSourceProvider extends ItemRasterSourceProvider {
  override def canProcess(item: OpenSearchResponses.Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean = {
    datacubeParams.map(d => d.syntheticDataOverride).get.isDefined
  }

  override def getRasterSource(item: OpenSearchResponses.Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters]): Option[RasterSource] = {
    datacubeParams.map(d => d.syntheticDataOverride.get) match {
        case Some(SyntheticDataOverride(cellType, udf)) => Some(SyntheticDataRasterSource(item.id, cellType, targetExtent.toGridType, targetCRS, udf = udf))
        case None => throw new IllegalArgumentException()
      }
  }
}
