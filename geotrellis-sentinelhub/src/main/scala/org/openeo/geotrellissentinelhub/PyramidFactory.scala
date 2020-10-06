package org.openeo.geotrellissentinelhub

import java.time.ZonedDateTime

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{FloatUserDefinedNoDataCellType, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.openeo.geotrellissentinelhub.bands.Landsat8Bands.Landsat8Band
import org.openeo.geotrellissentinelhub.bands.Sentinel1Bands.Sentinel1Band
import org.openeo.geotrellissentinelhub.bands.Sentinel2L1CBands.Sentinel2L1CBand
import org.openeo.geotrellissentinelhub.bands.Sentinel2L2ABands.Sentinel2L2ABand
import org.openeo.geotrellissentinelhub.bands._

import scala.collection.JavaConverters._

abstract class PyramidFactory[B <: Band]() extends Serializable {
  private val maxZoom = 14
  val allBands: Seq[B] = Seq()
  val datasetId: String = ""

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
      .map(key => (key, retrieveTileFromSentinelHub(datasetId, key.spatialKey.extent(layout), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bands)))
      .filter(_._2.bands.exists(b => !b.isNoDataTile))

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox)

      TileLayerMetadata(
        cellType = FloatUserDefinedNoDataCellType(1.0f),
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
      if (band_indices == null || band_indices.isEmpty) allBands
      else band_indices.asScala.map(allBands(_))

    pyramid(projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }
}

class S1PyramidFactory() extends PyramidFactory[Sentinel1Band]() {
  override val allBands: Seq[Sentinel1Band] = Sentinel1Bands.allBands
  override val datasetId = "S1GRD"
}

class S2L1CPyramidFactory() extends PyramidFactory[Sentinel2L1CBand]() {
  override val allBands: Seq[Sentinel2L1CBand] = Sentinel2L1CBands.allBands
  override val datasetId = "S2L1C"
}

class S2L2APyramidFactory() extends PyramidFactory[Sentinel2L2ABand]() {
  override val allBands: Seq[Sentinel2L2ABand] = Sentinel2L2ABands.allBands
  override val datasetId = "S2L2A"
}

class L8PyramidFactory() extends PyramidFactory[Landsat8Band]() {
  override val allBands: Seq[Landsat8Band] = Landsat8Bands.allBands
  override val datasetId = "L8L1C"
}
