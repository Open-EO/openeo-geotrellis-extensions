package org.openeo.geotrellissentinelhub

import java.time.ZonedDateTime

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{UShortConstantNoDataCellType, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class PyramidFactory(datasetId: String, clientId: String, clientSecret: String) extends Serializable {
  private val maxZoom = 14

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)
  
  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, bandNames: Seq[String])(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)
    
    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = dates.flatMap(date =>layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon()).map(key => SpaceTimeKey(key, date)))

    val tilesRdd = sc.parallelize(overlappingKeys)
      .map(key => (key, retrieveTileFromSentinelHub(datasetId, key.spatialKey.extent(layout), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bandNames, clientId, clientSecret)))
      .filter(_._2.bands.exists(b => !b.isNoDataTile))

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox)

      TileLayerMetadata(
        cellType = UShortConstantNoDataCellType,
        layout = layout,
        extent = reprojectedBoundingBox,
        crs = targetCrs,
        KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
      )
    }

    ContextRDD(tilesRdd, metadata)
  }
  
  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String])(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, bandNames)
    Pyramid(layers.toMap)
  }
  
  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_names: java.util.List[String]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to, band_names.asScala).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }
}
