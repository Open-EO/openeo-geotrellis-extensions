package org.openeo.geotrellis.aggregate_polygon.intern.polygonal

import geotrellis.raster._
import geotrellis.vector.summary.polygonal.PolygonalSummaryHandler
import geotrellis.vector.{Polygon, PolygonFeature}
import org.openeo.geotrellis.aggregate_polygon.intern.MeanResult

trait TilePolygonalSummaryHandler[T] extends PolygonalSummaryHandler[Polygon, Tile, T] {

  /**
    * Given a PolygonFeature, "handle" the case of an
    * entirly-contained tile.  This falls through to the
    * 'handleFullTile' handler.
    */
  def handleContains(feature: PolygonFeature[Tile]): T = handleFullTile(feature.data)

  /**
    * Given a Polygon and a PolygonFeature, "handle" the case of an
    * intersection.  This falls through to the 'handlePartialTile'
    * handler.
    */
  def handleIntersection(polygon: Polygon, feature: PolygonFeature[Tile]): T = handlePartialTile(Raster(feature), polygon)

  /**
    * Given a [[Raster]] and an intersection polygon, "handle" the
    * case where there is an intersection between the raster and some
    * polygon.
    */
  def handlePartialTile(raster: Raster[Tile], intersection: Polygon): T

  /**
    * Given a tile, "handle" the case were the tile is fully
    * enveloped.
    */
  def handleFullTile(tile: Tile): T

  def combineResults(values: Seq[T]): T

  def combineOp(v1: T, v2: T): T =
    combineResults(Seq(v1, v2))
}



object MeanSummary extends TilePolygonalSummaryHandler[MeanResult] {
  def handlePartialTile(raster: Raster[Tile], polygon: Polygon): MeanResult = {
    val Raster(tile, _) = raster
    val rasterExtent = raster.rasterExtent
    var sum = 0.0
    var valid = 0L
    var total = 0L
    if(tile.cellType.isFloatingPoint) {
      polygon.foreach(rasterExtent)({ (col: Int, row: Int) =>
        val z = tile.getDouble(col, row)
        total += 1
        if (isData(z)) { sum = sum + z; valid = valid + 1 }
      })
    } else {
      polygon.foreach(rasterExtent)({ (col: Int, row: Int) =>
        val z = tile.get(col, row)
        total += 1
        if (isData(z)) { sum = sum + z; valid = valid + 1 }
      })
    }

    MeanResult(sum, valid, total, None, None)
  }

  def handleFullTile(tile: Tile): MeanResult =
    if(tile.cellType.isFloatingPoint) {
      MeanResult.fromFullTileDouble(tile)
    } else {
      MeanResult.fromFullTile(tile)
    }

  def combineResults(rs: Seq[MeanResult]): MeanResult =
    rs.foldLeft(MeanResult(0.0, 0L, 0L, None, None))(_+_)
}