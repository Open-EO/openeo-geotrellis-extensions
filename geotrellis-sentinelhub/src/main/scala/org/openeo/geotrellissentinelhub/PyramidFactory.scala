package org.openeo.geotrellissentinelhub

import java.time.ZonedDateTime
import java.util
import java.util.Collections

import geotrellis.layer.{KeyBounds, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.proj4j.proj.TransverseMercatorProjection
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import org.openeo.geotrellissentinelhub.SampleType.{SampleType, UINT16}

import scala.collection.JavaConverters._

class PyramidFactory(datasetId: String, clientId: String, clientSecret: String,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16) extends Serializable {
  private val maxZoom = 14

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)
  
  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom,
            bandNames: Seq[String], metadataProperties: util.Map[String, Any])(implicit sc: SparkContext):
  MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)
    
    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = dates.flatMap(date =>layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon()).map(key => SpaceTimeKey(key, date)))

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox)

      TileLayerMetadata(
        cellType = sampleType.cellType,
        layout = layout,
        extent = reprojectedBoundingBox,
        crs = targetCrs,
        KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
      )
    }

    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)

    val tilesRdd = sc.parallelize(overlappingKeys)
      .map(key => (key,
        retrieveTileFromSentinelHub(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
          key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, bandNames, sampleType,
          metadataProperties, processingOptions, clientId, clientSecret)))
      .filter(_._2.bands.exists(b => !b.isNoDataTile))
      .partitionBy(partitioner)

    ContextRDD(tilesRdd, metadata)
  }
  
  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
              metadataProperties: util.Map[String, Any])(implicit sc: SparkContext):
  Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1)
      yield zoom -> layer(boundingBox, from, to, zoom, bandNames, metadataProperties)

    Pyramid(layers.toMap)
  }

  @deprecated("remove when openeo-geopyspark-driver is adapted")
  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String,
                  band_names: util.List[String]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    pyramid_seq(bbox, bbox_srs, from_date, to_date, band_names, metadata_properties = Collections.emptyMap[String, Any])
  
  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_names: util.List[String],
                  metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to, band_names.asScala, metadata_properties).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  @deprecated("remove when openeo-geopyspark-driver is adapted")
  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] =
    datacube_seq(polygons, polygons_crs, from_date, to_date, band_names,
      metadata_properties = Collections.emptyMap[String, Any])

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, Any]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: use ProjectedPolygons type
    // TODO: reduce code duplication with pyramid_seq()

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = {
      implicit val sc: SparkContext = SparkContext.getOrCreate()

      val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)

      val from = ZonedDateTime.parse(from_date)
      val to = ZonedDateTime.parse(to_date)

      // TODO: call into AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

      val layout = this.layout(FloatingLayoutScheme(256), boundingBox)

      val metadata: TileLayerMetadata[SpaceTimeKey] = {
        val gridBounds = layout.mapTransform.extentToBounds(boundingBox.extent)

        TileLayerMetadata(
          cellType = sampleType.cellType,
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
          spatialKey <- layout.mapTransform.keysForGeometry(GeometryCollection(polygons))
        } yield SpaceTimeKey(spatialKey, date)

        val tilesRdd = for {
          key <- sc.parallelize(overlappingKeys)
          tile = retrieveTileFromSentinelHub(datasetId, ProjectedExtent(key.spatialKey.extent(layout), boundingBox.crs),
            key.temporalKey, layout.tileLayout.tileCols, layout.tileLayout.tileRows, band_names.asScala, sampleType,
            metadata_properties, processingOptions, clientId, clientSecret)
          if !tile.bands.forall(_.isNoDataTile)
        } yield (key, tile)

        val partitioner = SpacePartitioner(metadata.bounds)
        assert(partitioner.index == SpaceTimeByMonthPartitioner)

        tilesRdd.partitionBy(partitioner)
      }

      ContextRDD(tilesRdd, metadata)
    }

    Seq(0 -> cube)
  }

  private def maxSpatialResolution: CellSize = CellSize(10, 10)

  private def layout(layoutScheme: FloatingLayoutScheme, boundingBox: ProjectedExtent): LayoutDefinition = {
    //Giving the layout a deterministic extent simplifies merging of data with spatial partitioner
    val layoutExtent =
      if (boundingBox.crs.proj4jCrs.getProjection.getName == "utm") {
        //for utm, we return an extent that goes beyound the utm zone bounds, to avoid negative spatial keys
        if (boundingBox.crs.proj4jCrs.getProjection.asInstanceOf[TransverseMercatorProjection].getSouthernHemisphere)
        //official extent: Extent(166021.4431, 1116915.0440, 833978.5569, 10000000.0000) -> round to 10m + extend
          Extent(0.0, 1000000.0, 833970.0 + 100000.0, 10000000.0000 + 100000.0)
        else {
          //official extent: Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825) -> round to 10m + extend
          Extent(0.0, -1000000.0000, 833970.0 + 100000.0, 9329000.0 + 100000.0)
        }
      } else {
        boundingBox.extent
      }

    layoutScheme.levelFor(layoutExtent, maxSpatialResolution).layout
  }
}
