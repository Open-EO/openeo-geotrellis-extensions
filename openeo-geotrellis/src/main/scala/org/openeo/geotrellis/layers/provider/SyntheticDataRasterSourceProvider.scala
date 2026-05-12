package org.openeo.geotrellis.layers.provider

import geotrellis.raster.{RasterExtent, RasterSource}
import org.openeo.geotrellis.layers.raster_source.SyntheticDataRasterSource
import org.openeo.geotrelliscommon.SyntheticDataOverride
import org.slf4j.{Logger, LoggerFactory}

object SyntheticDataRasterSourceProvider extends SyntheticDataRasterSourceProvider

class SyntheticDataRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[SyntheticDataRasterSourceProvider])

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    definition.datacubeParams.isDefined && definition.datacubeParams.get.syntheticDataOverride.isDefined
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val rasterExtent = RasterExtent(definition.targetExtent.extent, definition.theResolution)
    definition.datacubeParams.map(d => d.syntheticDataOverride.get) match {
      case Some(SyntheticDataOverride(cellType, udf)) => SyntheticDataRasterSource(definition.feature.id, cellType, rasterExtent.toGridType, definition.targetExtent.crs, udf = udf)
      case None => throw new IllegalArgumentException()
    }
  }
}
