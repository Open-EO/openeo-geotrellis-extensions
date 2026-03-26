package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, RasterExtent, RasterSource}
import org.openeo.geotrelliscommon.{DataCubeParameters, SyntheticDataOverride}
import org.openeo.opensearch.OpenSearchResponses
import org.slf4j.{Logger, LoggerFactory}

object SyntheticDataItemRasterSourceProvider extends SyntheticDataItemRasterSourceProvider

class SyntheticDataItemRasterSourceProvider extends ItemRasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[SyntheticDataItemRasterSourceProvider])

  override def canProcess(item: OpenSearchResponses.Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean = {
    datacubeParams.map(d => d.syntheticDataOverride).get.isDefined
  }

  override def getRasterSource(item: OpenSearchResponses.Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters], resolver: BandAssetLinkResolver): Option[RasterSource] = {
    logger.debug(s"Getting raster source for item ${item}")
    datacubeParams.map(d => d.syntheticDataOverride.get) match {
        case Some(SyntheticDataOverride(cellType, udf)) => Some(SyntheticDataRasterSource(item.id, cellType, targetExtent.toGridType, targetCRS, udf = udf))
        case None => throw new IllegalArgumentException()
      }
  }
}
