package org.openeo.geotrellis.file

import java.time.ZonedDateTime

import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{FloatCellType, MultibandTile}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.openeo.geotrellissentinelhub.Gamma0Bands._
import org.openeo.geotrellissentinelhub._

import scala.collection.JavaConverters._

object Sentinel1Gamma0PyramidFactory {
  private val maxZoom = 14
}

class Sentinel1Gamma0PyramidFactory {
  import Sentinel1Gamma0PyramidFactory._

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)
  
  def layer(uuid: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, bands: Seq[Band] = gamma0Bands)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)
    
    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = dates.flatMap(date =>layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon()).map(key => SpaceTimeKey(key, date)))

    val tilesRdd = sc.parallelize(overlappingKeys)
      .map(key => (key, retrieveS1Gamma0TileFromSentinelHub(uuid, key.spatialKey.extent(layout), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bands)))

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox)

      TileLayerMetadata(
        cellType = FloatCellType,
        layout = layout,
        extent = reprojectedBoundingBox,
        crs = targetCrs,
        KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
      )
    }

    ContextRDD(tilesRdd, metadata)
  }
  
  def pyramid(uuid: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[Band] = gamma0Bands)(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(uuid, boundingBox, from, to, zoom, bands)
    Pyramid(layers.toMap)
  }
  
  def pyramid_seq(uuid: String, bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[Band] =
      if (band_indices == null) gamma0Bands
      else band_indices.asScala.map(gamma0Bands(_))

    pyramid(uuid, projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

}
