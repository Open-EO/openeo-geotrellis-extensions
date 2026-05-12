package org.openeo.geotrellis.layers.provider

import geotrellis.raster.RasterSource
import geotrellis.vector.ProjectedExtent
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource
import org.slf4j.{Logger, LoggerFactory}

object SentinelXmlMetadataRasterSourceProvider extends SentinelXmlMetadataRasterSourceProvider

class SentinelXmlMetadataRasterSourceProvider extends RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[SentinelXmlMetadataRasterSourceProvider])

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    definition.dataPath.endsWith("MTD_TL.xml")
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val targetProjectedExtent = definition.featureExtentInLayout match {
      case None => None
      case Some(featureExtentInLayoutGet) =>
        Some(ProjectedExtent(featureExtentInLayoutGet.extent, definition.targetExtent.crs))
    }
    SentinelXMLMetadataRasterSource.forAngleBand(definition.dataPath, definition.bandIndex, targetProjectedExtent, Some(definition.theResolution))
  }
}
