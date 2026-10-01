package org.openeo.geotrellishealpix

import geotrellis.raster.{DoubleArrayTile, DoubleConstantNoDataCellType, MultibandTile, Tile}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

/**
 * Bridge between the HEALPix DataFrame representation and GeoTrellis
 * `MultibandTile`s. The HEALPix cell array is laid out as an N×1 strip in a
 * `DoubleArrayTile`, which is sufficient for purely element-wise openEO
 * processes (e.g. `apply`). Spatially-aware operations (focal/kernel) would
 * need a different bridge that respects HEALPix neighbourhood topology.
 */
object HealpixTileBridge {

  private def toDouble(v: Any): Double = v match {
    case null         => Double.NaN
    case n: Number    => n.doubleValue()
    case b: Boolean   => if (b) 1.0 else 0.0
    case other        => other.toString.toDouble
  }

  /** Convert a sequence of scalar-layout rows into a single MultibandTile. */
  def rowsToTile(rows: Seq[Row],
                 bandNames: Seq[String],
                 schema: StructType): (MultibandTile, Array[Long]) = {
    val n = rows.length
    val cellIds = new Array[Long](n)
    var i = 0
    while (i < n) { cellIds(i) = rows(i).getAs[Long](HealpixSchema.CellId); i += 1 }

    val bands: Seq[Tile] = bandNames.map { b =>
      val idx = schema.fieldIndex(b)
      val arr = new Array[Double](n)
      var k = 0
      while (k < n) {
        val r = rows(k)
        arr(k) = if (r.isNullAt(idx)) Double.NaN else toDouble(r.get(idx))
        k += 1
      }
      DoubleArrayTile(arr, n, 1, DoubleConstantNoDataCellType)
    }
    (MultibandTile(bands), cellIds)
  }

  /** Convert a MultibandTile back into scalar-layout Rows. */
  def tileToRows(tile: MultibandTile,
                 cellIds: Array[Long],
                 ts: java.sql.Timestamp,
                 bandNames: Seq[String],
                 schema: StructType): Iterator[Row] = {
    val n = cellIds.length
    val perBand: IndexedSeq[Array[Double]] = bandNames.indices.map { i =>
      val t = tile.band(i)
      val a = new Array[Double](n)
      var c = 0
      while (c < n) { a(c) = t.getDouble(c, 0); c += 1 }
      a
    }.toIndexedSeq
    (0 until n).iterator.map { i =>
      val values: Seq[Any] = Seq[Any](cellIds(i), ts) ++ perBand.map(_(i): Any)
      Row.fromSeq(values)
    }
  }

  /** Packed-layout row -> MultibandTile (one strip per band, of length chunk size). */
  def packedRowToTile(row: Row,
                      bandNames: Seq[String]): (MultibandTile, Array[Long]) = {
    val start = row.getAs[Long](HealpixSchema.CellIdStart)
    val size  = row.getAs[Int](HealpixSchema.ChunkSize)
    val cellIds = Array.tabulate(size)(i => start + i)
    val bands: Seq[Tile] = bandNames.map { b =>
      val seq = row.getAs[scala.collection.Seq[Any]](b)
      val arr = new Array[Double](size)
      var i = 0
      while (i < size) {
        val v =
          if (seq == null || i >= seq.length || seq(i) == null) Double.NaN
          else toDouble(seq(i))
        arr(i) = v
        i += 1
      }
      DoubleArrayTile(arr, size, 1, DoubleConstantNoDataCellType)
    }
    (MultibandTile(bands), cellIds)
  }

  /** MultibandTile -> single packed-layout Row. */
  def packedTileToRow(tile: MultibandTile,
                      cellIds: Array[Long],
                      ts: java.sql.Timestamp,
                      bandNames: Seq[String]): Row = {
    val start = cellIds.head
    val size  = cellIds.length
    val bandArrays: Seq[Any] = bandNames.indices.map { i =>
      val t = tile.band(i)
      val arr = new Array[java.lang.Double](size)
      var c = 0
      while (c < size) { arr(c) = t.getDouble(c, 0); c += 1 }
      arr.toSeq
    }
    Row.fromSeq(Seq[Any](start, size, ts) ++ bandArrays)
  }

  /**
   * Run an OpenEOProcessScriptBuilder process on a MultibandTile.
   * Reuses the existing openEO process script machinery to avoid duplication.
   */
  def runProcess(tile: MultibandTile,
                 scriptBuilder: OpenEOProcessScriptBuilder,
                 context: java.util.Map[String, Any]): MultibandTile = {
    val ctx: java.util.Map[String, Any] =
      if (context == null) new java.util.HashMap[String, Any]() else context
    val function = scriptBuilder.generateFunction(ctx)
    val resultTiles = function(tile.bands)
    MultibandTile(resultTiles)
  }
}

