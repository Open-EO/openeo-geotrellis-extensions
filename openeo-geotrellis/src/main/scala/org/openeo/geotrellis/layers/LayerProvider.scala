package org.openeo.geotrellis.layers

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{IntConstantNoDataCellType, Tile}
import geotrellis.spark._
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.sfcurve.zorder.{Z2, ZRange}

import java.time.ZonedDateTime

object LayerProvider{
  def createMaskLayer(features: Seq[Feature[MultiPolygon, Double]], crs: CRS, metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext): RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = {

    val rddCount = math.max(10,features.size / 20)
    val reprojected: RDD[MultiPolygonFeature[Double]] = sc.parallelize(features,rddCount).map(_.reproject(crs, metadata.crs))
    val envelope = reprojected.map(_.geom.extent).reduce{_.combine(_)}

    val partitioner = {
      val gridBounds = metadata.mapTransform(envelope)
      //negative spatial keys means going out of bounds of
      val nonNegativeBounds = gridBounds.copy(colMin = math.max(0,gridBounds.colMin),rowMin = math.max(0,gridBounds.rowMin))

      val spatialPartitioner: PartitionerIndex[SpatialKey] = new PartitionerIndex[SpatialKey] {
        private def toZ(key: SpatialKey): Z2 = Z2(key.col >> 3, key.row >> 3)

        def toIndex(key: SpatialKey): BigInt = toZ(key).z

        def indexRanges(keyRange: (SpatialKey, SpatialKey)): Seq[(BigInt, BigInt)] =
          Z2.zranges(Array(ZRange(toZ(keyRange._1), toZ(keyRange._2))), maxRecurse = Some(100)).map(r => (BigInt(r.lower), BigInt(r.upper)))
      }

      SpacePartitioner(KeyBounds(nonNegativeBounds))(implicitly,implicitly,spatialPartitioner)
    }

    // note: this rasterizes the mask to the resolution of the data. This means that very small polygons that lie
    // very close to each other, will be mapped to the same pixel; one of those small polygons will end up in the
    // mask, and the others are discarded (a pixel can only have a single value). The result is that the output array
    // of means will be smaller than the input array! A solution is to upsample the mask (and the data) to the point
    // where each polygon occupies at least one pixel.

    reprojected.rasterize(IntConstantNoDataCellType, metadata.layout, Rasterizer.Options.DEFAULT, partitioner)
  }
}

trait LayerProvider {
  def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerRDD[SpaceTimeKey]
  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey]

  def readMetadata(zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerMetadata[SpaceTimeKey]

  def deriveMaskLayer(features: Seq[Feature[MultiPolygon, Double]], crs: CRS, zoom: Int = Int.MaxValue, sc: SparkContext): RDD[(SpatialKey, Tile)] with Metadata[LayoutDefinition] = {
    val metadata = readMetadata(zoom, sc)

    LayerProvider.createMaskLayer(features, crs, metadata, sc)
  }



  /** Derives additional metadata and persists it. */
  def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime])

  /** Loads persisted additional metadata. */
  def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])]

}


