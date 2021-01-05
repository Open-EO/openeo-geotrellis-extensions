package org.openeo.geotrellis.aggregate_polygon.intern

import geotrellis.raster._
import geotrellis.raster.histogram.{FastMapHistogram, Histogram, StreamingHistogram}
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector._
import org.openeo.geotrellis.aggregate_polygon.intern.polygonal.{MultibandTilePolygonalSummaryHandler, TilePolygonalSummaryHandler}

object NoDataIncludingIntHistogramSummary extends TilePolygonalSummaryHandler[Histogram[Int]] {
  override def handlePartialTile(raster: Raster[Tile], polygon: Polygon): Histogram[Int] = {
    val Raster(tile, _) = raster
    val rasterExtent = raster.rasterExtent
    val histogram = FastMapHistogram()
    polygon.foreach(rasterExtent)({ (col: Int, row: Int) =>
      val z = tile.get(col, row)
      histogram.countItem(z, 1)
    })
    histogram
  }

  override def handleFullTile(tile: Tile): Histogram[Int] = {
    val histogram = FastMapHistogram()
    tile.foreach { (z: Int) => histogram.countItem(z, 1) }
    histogram
  }

  override def combineResults(rs: Seq[Histogram[Int]]): Histogram[Int] =
    if (rs.nonEmpty) rs.reduce(_ merge _)
    else FastMapHistogram()
}


object MultibandNoDataIncludingIntHistogramSummary extends MultibandTilePolygonalSummaryHandler[Array[Histogram[Int]]] {

  /**
    * Given a [[Raster]] which partially intersects the given polygon,
    * find the sum of the Raster elements in the intersection.
    */
  def handlePartialMultibandTile(raster: Raster[MultibandTile], polygon: Polygon): Array[Histogram[Int]] = {
    val Raster(multibandTile, extent) = raster
    multibandTile.bands.map { tile => NoDataIncludingIntHistogramSummary.handlePartialTile(Raster(tile, extent), polygon) }.toArray
  }

  /**
    * Find the sum of the elements in the [[Raster]].
    */
  def handleFullMultibandTile(multibandTile: MultibandTile): Array[Histogram[Int]] =
    multibandTile.bands.map { NoDataIncludingIntHistogramSummary.handleFullTile(_) }.toArray

  /**
    * Combine the results into a larger result.
    */
  def combineOp(v1: Array[Histogram[Int]], v2: Array[Histogram[Int]]): Array[Histogram[Int]] =
    v1.zipAll(v2, FastMapHistogram(), FastMapHistogram()) map { case (r1, r2) => NoDataIncludingIntHistogramSummary.combineOp(r1, r2) }

  def combineResults(res: Seq[Array[Histogram[Int]]]): Array[Histogram[Int]] =
    if (res.isEmpty)
      Array(FastMapHistogram())
    else
      res.reduce { (res1, res2) =>
        res1 zip res2 map {
          case (r1: Histogram[Int], r2: Histogram[Int]) =>
            NoDataIncludingIntHistogramSummary.combineResults(Seq(r1, r2))
        }
      }
}


object NoDataIncludingDoubleHistogramSummary extends TilePolygonalSummaryHandler[Histogram[Double]] {
  def handlePartialTile(raster: Raster[Tile], polygon: Polygon): Histogram[Double] = {
    val Raster(tile, _) = raster
    val rasterExtent = raster.rasterExtent
    val histogram = StreamingHistogram(512)
    Rasterizer.foreachCellByGeometry(polygon, rasterExtent) { (col: Int, row: Int) =>
      val z = tile.getDouble(col, row)
      histogram.countItem(z, 1)
    }
    histogram
  }

  def handleFullTile(tile: Tile): Histogram[Double] = {
    val histogram = StreamingHistogram(512)
    tile.foreach { (z: Int) => histogram.countItem(z, 1) }
    histogram
  }

  def combineResults(rs: Seq[Histogram[Double]]): Histogram[Double] =
    if (rs.nonEmpty) rs.reduce(_ merge _)
    else StreamingHistogram(512)
}

object MultibandNoDataIncludingDoubleHistogramSummary extends MultibandTilePolygonalSummaryHandler[Array[Histogram[Double]]]{
  /**
    * Given a [[Raster]] which partially intersects the given polygon,
    * find the sum of the Raster elements in the intersection.
    */
  def handlePartialMultibandTile(raster: Raster[MultibandTile], polygon: Polygon): Array[Histogram[Double]] = {
    val Raster(multibandTile, extent) = raster
    multibandTile.bands.map { tile => NoDataIncludingDoubleHistogramSummary.handlePartialTile(Raster(tile, extent), polygon) }.toArray
  }

  /**
    * Find the sum of the elements in the [[Raster]].
    */
  def handleFullMultibandTile(multibandTile: MultibandTile): Array[Histogram[Double]] =
    multibandTile.bands.map { NoDataIncludingDoubleHistogramSummary.handleFullTile(_) }.toArray

  /**
    * Combine the results into a larger result.
    */
  def combineOp(v1: Array[Histogram[Double]], v2: Array[Histogram[Double]]): Array[Histogram[Double]] =
    v1.zipAll(v2, StreamingHistogram(512), StreamingHistogram(512)) map { case (r1, r2) => NoDataIncludingDoubleHistogramSummary.combineOp(r1, r2) }

  def combineResults(res: Seq[Array[Histogram[Double]]]): Array[Histogram[Double]] =
    if (res.isEmpty)
      Array(StreamingHistogram(512))
    else
      res.reduce { (res1, res2) =>
        res1 zip res2 map {
          case (r1: Histogram[Double], r2: Histogram[Double]) =>
            NoDataIncludingDoubleHistogramSummary.combineResults(Seq(r1, r2))
        }
      }
}