package org.openeo.geotrellissentinelhub

import java.time.ZonedDateTime

import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{FloatCellType, MultibandTile}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.openeo.geotrellissentinelhub.bands.{Band, Sentinel1Bands, Sentinel2Bands}
import org.openeo.geotrellissentinelhub.bands.Sentinel1Bands.Sentinel1Band
import org.openeo.geotrellissentinelhub.bands.Sentinel2Bands.Sentinel2Band

import scala.collection.JavaConverters._

abstract class PyramidFactory[B <: Band](val uuid: String) extends Serializable {
  private val maxZoom = 14
  val allBands: Seq[B] = Seq()
  
  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)
  
  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, bands: Seq[B] = allBands)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)
    
    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = dates.flatMap(date =>layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon()).map(key => SpaceTimeKey(key, date)))

    val tilesRdd = sc.parallelize(overlappingKeys)
      .map(key => (key, retrieveTileFromSentinelHub(uuid, key.spatialKey.extent(layout), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bands)))

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
  
  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[B] = allBands)(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, bands)
    Pyramid(layers.toMap)
  }
  
  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[B] =
      if (band_indices == null) allBands
      else band_indices.asScala.map(allBands(_))

    pyramid(projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }
}

class S1PyramidFactory(override val uuid: String) extends PyramidFactory[Sentinel1Band](uuid) {
  override val allBands: Seq[Sentinel1Band] = Sentinel1Bands.allBands
}

class S2PyramidFactory(override val uuid: String) extends PyramidFactory[Sentinel2Band](uuid) {
  override val allBands: Seq[Sentinel2Band] = Sentinel2Bands.allBands
}
