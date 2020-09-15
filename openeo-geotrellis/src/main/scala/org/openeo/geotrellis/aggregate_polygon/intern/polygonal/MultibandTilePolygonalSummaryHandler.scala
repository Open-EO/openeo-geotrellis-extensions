package org.openeo.geotrellis.aggregate_polygon.intern.polygonal

import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.summary.polygonal.PolygonalSummaryHandler
import geotrellis.vector.{Polygon, PolygonFeature}

trait MultibandTilePolygonalSummaryHandler[T] extends PolygonalSummaryHandler[Polygon, MultibandTile, T] {

    /**
      * Given a PolygonFeature, "handle" the case of an
      * entirly-contained tile.  This falls through to the
      * 'handleFullMultibandTile' handler.
      */
    def handleContains(feature: PolygonFeature[MultibandTile]): T = handleFullMultibandTile(feature.data)

    /**
      * Given a Polygon and a PolygonFeature, "handle" the case of an
      * intersection.  This falls through to the 'handlePartialMultibandTile'
      * handler.
      */
    def handleIntersection(polygon: Polygon, feature: PolygonFeature[MultibandTile]): T = handlePartialMultibandTile(Raster(feature), polygon)

    /**
      * Given a [[Raster]] and an intersection polygon, "handle" the
      * case where there is an intersection between the raster and some
      * polygon.
      */
    def handlePartialMultibandTile(raster: Raster[MultibandTile], intersection: Polygon): T

    /**
      * Given a tile, "handle" the case were the tile is fully
      * enveloped.
      */
    def handleFullMultibandTile(multibandTile: MultibandTile): T

    def combineResults(res: Seq[T]): T

    def combineOp(v1: T, v2: T): T
  }
