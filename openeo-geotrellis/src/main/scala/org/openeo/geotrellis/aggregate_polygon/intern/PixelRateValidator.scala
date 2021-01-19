package org.openeo.geotrellis.aggregate_polygon.intern

import java.time.{Duration, ZonedDateTime}

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import javax.ws.rs.BadRequestException
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

object PixelRateValidator {
  
  private val logger = LoggerFactory.getLogger(PixelRateValidator.getClass)
  
  def apply(extent: Extent, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Long = {
    apply(ProjectedExtent(extent, metadata.crs), metadata, sc)
  }

  def apply(boundingBox: ProjectedExtent, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Long = {
    apply(Array(boundingBox), metadata, sc)
  }

  def apply(boundingBoxes: Array[ProjectedExtent], metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Long = {
    apply(boundingBoxes, ZonedDateTime.now(), ZonedDateTime.now(), metadata, sc)
  }
  
  def apply(boundingBox: ProjectedExtent, startDate: ZonedDateTime, endDate: ZonedDateTime, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Long = {
    apply(Array(boundingBox), startDate, endDate, metadata, sc)
  }
  
  def apply(boundingBoxes: Array[ProjectedExtent], startDate: ZonedDateTime, endDate: ZonedDateTime, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Long = {
    val pixels = boundingBoxes.map(pixelsForExtent(startDate, endDate, metadata)).sum

    if (pixels > maxPixels()) {
      throw new BadRequestException("Total of pixels needed for calculations is too large. Try to query a smaller area or to use a lower zoom level")
    }

    logger.info(s"${pixels.toDouble} pixels needed for this request")
    
    pixels
  }

  def exceedsTreshold(boundingBox: ProjectedExtent, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): Boolean = {
    apply(boundingBox, metadata, sc) > pixelThreshold()
  }
  
  private def pixelsForExtent(startDate: ZonedDateTime, endDate: ZonedDateTime, metadata: TileLayerMetadata[SpaceTimeKey])(boundingBox: ProjectedExtent) = {
    val days = Duration.between(startDate, endDate).toDays + 1
    
    if (boundingBox == null) 0L
    else {
      val extent = boundingBox.reproject(metadata.crs)

      if (extent.isEmpty) 0L
      else {
        val cols = math.ceil(extent.width / metadata.cellwidth).toLong
        val rows = math.ceil(extent.height / metadata.cellheight).toLong

        val pixels = cols * rows
        days * pixels
      }
    }
  }

  private def maxPixels(): Long =
    Option(System.getProperty("pixels.max"))
      .map(_.toLong)
      .getOrElse(Long.MaxValue)

  private def pixelThreshold(): Long = {
    Option(System.getProperty("pixels.treshold"))
      .map(_.toLong)
      .getOrElse(0L)
  }
}
