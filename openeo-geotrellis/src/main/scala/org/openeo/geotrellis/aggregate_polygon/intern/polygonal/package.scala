package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.layer.{LayoutDefinition, Metadata, SpatialComponent}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.summary.polygonal.PolygonalSummaryHandler
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object polygonal {
  private[polygonal] def polygonalSummaryByKey[K: ClassTag, G <: Geometry, D, T: ClassTag](featureRdd: RDD[(K, Feature[G, D])], polygon: Polygon, zeroValue: T)(handler: PolygonalSummaryHandler[G, D, T]): RDD[(K, T)] = {
    featureRdd.aggregateByKey(zeroValue)(handler.mergeOp(polygon, zeroValue), handler.combineOp)
  }

  private[polygonal] def polygonalSummaryByKey[K: ClassTag, G <: Geometry, D, T: ClassTag](featureRdd: RDD[(K, Feature[G, D])], multiPolygon: MultiPolygon, zeroValue: T)(handler: PolygonalSummaryHandler[G, D, T]): RDD[(K, T)] = {
    featureRdd.aggregateByKey(zeroValue)(handler.mergeOp(multiPolygon, zeroValue), handler.combineOp)
  }

  implicit class WithTilePolygonalSummaryByKey[
    K: SpatialComponent: ClassTag,
    M: ({ type F[E] = GetComponent[E, LayoutDefinition] })#F](layer: RDD[(K, Tile)] with Metadata[M]) {

    def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                         polygon: Polygon,
                                                         zeroValue: T,
                                                         handler: TilePolygonalSummaryHandler[T],
                                                         fKey: K => L): RDD[(L, T)] = {
      val featureRdd = layer
        .asRasters()
        .map { case (key, raster) => (fKey(key), raster.asFeature()) }

      polygonal.polygonalSummaryByKey(featureRdd, polygon, zeroValue)(handler)
    }

    def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                         multiPolygon: MultiPolygon,
                                                         zeroValue: T,
                                                         handler: TilePolygonalSummaryHandler[T],
                                                         fKey: K => L): RDD[(L, T)] = {
      val featureRdd = layer
        .asRasters()
        .map { case (key, raster) => (fKey(key), raster.asFeature()) }

      polygonal.polygonalSummaryByKey(featureRdd, multiPolygon, zeroValue)(handler)
    }
  }

  implicit class WithMultibandTilePolygonalSummaryByKey[
    K: SpatialComponent: ClassTag,
    M: ({ type F[E] = GetComponent[E, LayoutDefinition] })#F](layer: RDD[(K, MultibandTile)] with Metadata[M]) {

    def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                         polygon: Polygon,
                                                         zeroValue: T,
                                                         handler: MultibandTilePolygonalSummaryHandler[T],
                                                         fKey: K => L): RDD[(L, T)] = {
      val featureRdd =
        layer.asRasters()
          .map { case (key, raster) => (fKey(key), raster.asFeature) }

      polygonal.polygonalSummaryByKey(featureRdd, polygon, zeroValue)(handler)
    }

    def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                         multiPolygon: MultiPolygon,
                                                         zeroValue: T,
                                                         handler: MultibandTilePolygonalSummaryHandler[T],
                                                         fKey: K => L): RDD[(L, T)] = {
      val featureRdd =
        layer
          .asRasters()
          .map { case (key, raster) => (fKey(key), raster.asFeature()) }

      polygonal.polygonalSummaryByKey(featureRdd, multiPolygon, zeroValue)(handler)
    }
  }
}
