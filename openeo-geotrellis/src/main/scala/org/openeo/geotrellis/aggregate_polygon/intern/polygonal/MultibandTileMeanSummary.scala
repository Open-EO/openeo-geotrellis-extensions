package org.openeo.geotrellis.aggregate_polygon.intern.polygonal

import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.Polygon
import org.openeo.geotrellis.aggregate_polygon.intern.MeanResult

object MultibandTileMeanSummary extends MultibandTilePolygonalSummaryHandler[Array[MeanResult]] {

  /**
    * Given a [[Raster]] which partially intersects the given polygon,
    * find the sum of the Raster elements in the intersection.
    */
  def handlePartialMultibandTile(raster: Raster[MultibandTile], polygon: Polygon): Array[MeanResult] = {
    val Raster(multibandTile, extent) = raster
    multibandTile.bands.map { tile => MeanSummary.handlePartialTile(Raster(tile, extent), polygon) }.toArray
  }

  /**
    * Find the sum of the elements in the [[Raster]].
    */
  def handleFullMultibandTile(multibandTile: MultibandTile): Array[MeanResult] =
    multibandTile.bands.map { MeanSummary.handleFullTile }.toArray

  /**
    * Combine the results into a larger result.
    */
  def combineOp(v1: Array[MeanResult], v2: Array[MeanResult]): Array[MeanResult] =
    v1.zipAll(v2, MeanResult(0.0, 0L), MeanResult(0.0, 0L)) map { case (r1, r2) => MeanSummary.combineOp(r1, r2) }

  def combineResults(res: Seq[Array[MeanResult]]): Array[MeanResult] =
    if (res.isEmpty)
      Array(MeanResult(0.0, 0L))
    else
      res.reduce { (res1, res2) =>
        res1 zip res2 map {
          case (r1: MeanResult, r2: MeanResult) =>
            MeanSummary.combineResults(Seq(r1, r2))
        }
      }
}
