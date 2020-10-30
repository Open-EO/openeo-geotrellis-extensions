package org.openeo.geotrellissentinelhub

import java.time.ZonedDateTime

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

    // TODO: write as a for-comprehension?
    val overlappingKeys = dates.flatMap(date =>layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon()).map(key => SpaceTimeKey(key, date)))

    val tilesRdd = sc.parallelize(overlappingKeys)
      .map(key => (key, retrieveTileFromSentinelHub(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bandNames, clientId, clientSecret)))
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

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String, band_names: java.util.List[String]): // FIXME: use ProjectedPolygons type
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val cube: MultibandTileLayerRDD[SpaceTimeKey] = {
      implicit val sc: SparkContext = SparkContext.getOrCreate()

      val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)

      val from = ZonedDateTime.parse(from_date)
      val to = ZonedDateTime.parse(to_date)

      // TODO: call into AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

      val layoutScheme = FloatingLayoutScheme(256)
      val LayoutLevel(_, layout) = layoutScheme // TODO: derive from FloatingLayoutScheme like FileLayerProvider#165 instead
        .levelFor(boundingBox.extent, CellSize(10, 10))

      val metadata: TileLayerMetadata[SpaceTimeKey] = {
        val gridBounds = layout.mapTransform.extentToBounds(boundingBox.extent)

        TileLayerMetadata(
          cellType = UShortConstantNoDataCellType,
          layout,
          extent = boundingBox.extent,
          crs = boundingBox.crs,
          bounds = KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
        )
      }

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        val dates = sequentialDates(from)
          .takeWhile(date => !(date isAfter to))

        val overlappingKeys = for {
          date <- dates
          spatialKey <- layout.mapTransform.keysForGeometry(boundingBox.extent.toPolygon())
        } yield SpaceTimeKey(spatialKey, date)

        val tilesRdd = for {
          key <- sc.parallelize(overlappingKeys)
          tile = retrieveTileFromSentinelHub(datasetId, ProjectedExtent(key.spatialKey.extent(layout), boundingBox.crs), key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, band_names.asScala, clientId, clientSecret)
          if !tile.bands.forall(_.isNoDataTile)
        } yield (key, tile)

        tilesRdd
      }

      ContextRDD(tilesRdd, metadata)
    }

    Seq(0 -> cube)
  }
}
