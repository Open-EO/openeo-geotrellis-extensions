package org.openeo.geotrellis.file

import java.time.ZonedDateTime

import be.vito.eodata.extracttimeseries.geotrellis.Sentinel2FileLayerProvider
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext

class Sentinel2PyramidFactory(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String) {
  private val sentinel2FileLayerProvider = new Sentinel2FileLayerProvider(oscarsCollectionId, oscarsLinkTitle, rootPath)

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    for (zoom <- sentinel2FileLayerProvider.maxZoom to 0 by -1)
      yield zoom -> layer(projectedExtent, from, to, zoom)
  }

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = sentinel2FileLayerProvider.maxZoom)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    sentinel2FileLayerProvider.readMultibandTileLayer(from, to, boundingBox, zoom, sc)
}
