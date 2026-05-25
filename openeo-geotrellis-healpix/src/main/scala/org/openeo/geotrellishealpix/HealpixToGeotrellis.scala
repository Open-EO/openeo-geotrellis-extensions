package org.openeo.geotrellishealpix

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.Extent
import healpix.{HealpixBase, Pointing, Scheme}
import org.apache.spark.rdd.RDD

import java.sql.Timestamp

/**
 * Render a [[HealpixDatacube]] as a GeoTrellis `MultibandTileLayerRDD[SpaceTimeKey]`
 * at a chosen target CRS / [[LayoutDefinition]], using nearest-neighbour lookup
 * from raster cell -> HEALPix cell (via `ang2pix` from the Gaia HEALPix Java port,
 * `io.txcl:healpix`).
 *
 * For each output pixel the (lon, lat) is computed in WGS84 and converted to a
 * HEALPix cell id at the cube's NSIDE. Band values are then joined in from the
 * underlying Spark SQL DataFrame.
 *
 * The current implementation uses a broadcast for the cell-id -> value lookup,
 * which is appropriate for small/medium cubes. For large cubes, replace it with
 * a Spark join: emit `(cellId, (SpaceTimeKey, pixelIndex))` from the target
 * layout, join against the DataFrame keyed by `cell_id`, then `groupByKey` to
 * assemble the output tiles.
 */
object HealpixToGeotrellis {

  /** Nearest-neighbour: (lonDeg, latDeg) -> HEALPix NESTED cell id. */
  private def ang2pix(base: HealpixBase, lonDeg: Double, latDeg: Double): Long = {
    val lonNorm = ((lonDeg % 360.0) + 360.0) % 360.0
    val phi     = math.toRadians(lonNorm)
    val theta   = math.toRadians(90.0 - math.max(-90.0, math.min(90.0, latDeg)))
    base.ang2pix(new Pointing(theta, phi))
  }

  def render(cube: HealpixDatacube,
             targetCRS: CRS,
             layout: LayoutDefinition,
             extent: Extent,
             bandIndices: Seq[Int] = Seq(0)): MultibandTileLayerRDD[SpaceTimeKey] = {

    val spark = cube.df.sparkSession
    val sc    = spark.sparkContext
    val base  = new HealpixBase(cube.nside, Scheme.NESTED)
    val bands = cube.bands.map(_._1)
    val ct    = DoubleConstantNoDataCellType

    // --- 1. enumerate timestamps and target spatial keys --------------------
    val timestamps: Array[Timestamp] =
      cube.df.select(HealpixSchema.Timestamp).distinct().collect()
        .map(_.getTimestamp(0))
        .sortBy(_.toInstant.toEpochMilli)

    require(timestamps.nonEmpty, "Cannot render an empty HealpixDatacube (no timestamps).")

    val tileCols = layout.tileLayout.tileCols
    val tileRows = layout.tileLayout.tileRows
    val mapTransform = layout.mapTransform
    val gridBounds = mapTransform.extentToBounds(extent)

    val keyBounds = KeyBounds(
      SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, timestamps.head.toInstant.toEpochMilli),
      SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, timestamps.last.toInstant.toEpochMilli)
    )

    val spatialKeys: Seq[SpatialKey] =
      (for {
        c <- gridBounds.colMin to gridBounds.colMax
        r <- gridBounds.rowMin to gridBounds.rowMax
      } yield SpatialKey(c, r)).toSeq

    // --- 2. per SpatialKey: pixel -> HEALPix cell id ------------------------
    val reproject = Transform(targetCRS, LatLng)

    val keyToCellIds: Map[SpatialKey, Array[Long]] =
      spatialKeys.map { sk =>
        val tileExtent = mapTransform.keyToExtent(sk)
        val cw = tileExtent.width  / tileCols
        val ch = tileExtent.height / tileRows
        val ids = new Array[Long](tileCols * tileRows)
        var r = 0
        while (r < tileRows) {
          var c = 0
          while (c < tileCols) {
            val x = tileExtent.xmin + (c + 0.5) * cw
            val y = tileExtent.ymax - (r + 0.5) * ch
            val (lon, lat) = reproject(x, y)
            ids(r * tileCols + c) = ang2pix(base, lon, lat)
            c += 1
          }
          r += 1
        }
        sk -> ids
      }.toMap

    val keyToCellIdsB = sc.broadcast(keyToCellIds)

    // --- 3. (cellId, ts) -> band values lookup (broadcast variant) ----------
    // TODO: for large cubes, the collect + broadcast approach will not scale.
    //  Instead we need to map cellIDs to spatial keys, groupbyKey on SpacetimeKey, and assemble the tiles
    val bandColIndices = bandIndices.map(i => cube.df.schema.fieldIndex(bands(i)))

    val lookup: Map[(Long, Long), Array[Double]] = cube match {
      case s: ScalarHealpixDatacube =>
        val cidIdx = s.df.schema.fieldIndex(HealpixSchema.CellId)
        val tsIdx  = s.df.schema.fieldIndex(HealpixSchema.Timestamp)
        s.df.collect().iterator.map { row =>
          val cid = row.getLong(cidIdx)
          val ts  = row.getAs[Timestamp](tsIdx).toInstant.toEpochMilli
          val v = bandColIndices.map { i =>
            if (row.isNullAt(i)) doubleNODATA
            else row.get(i).asInstanceOf[Number].doubleValue()
          }.toArray
          (cid, ts) -> v
        }.toMap

      case p: PackedHealpixDatacube =>
        val startIdx = p.df.schema.fieldIndex(HealpixSchema.CellIdStart)
        val sizeIdx  = p.df.schema.fieldIndex(HealpixSchema.ChunkSize)
        val tsIdx    = p.df.schema.fieldIndex(HealpixSchema.Timestamp)
        p.df.collect().iterator.flatMap { row =>
          val start = row.getLong(startIdx)
          val size  = row.getInt(sizeIdx)
          val ts    = row.getAs[Timestamp](tsIdx).toInstant.toEpochMilli
          val arrs  = bandColIndices.map(i => row.getAs[scala.collection.Seq[Any]](i))
          (0 until size).iterator.map { i =>
            val v = arrs.map { a =>
              if (a == null || i >= a.length || a(i) == null) doubleNODATA
              else a(i).asInstanceOf[Number].doubleValue()
            }.toArray
            (start + i, ts) -> v
          }
        }.toMap
    }
    val lookupB = sc.broadcast(lookup)

    // --- 4. build the RDD ---------------------------------------------------
    val keys: RDD[SpaceTimeKey] = sc.parallelize(
      for { sk <- spatialKeys; ts <- timestamps }
        yield SpaceTimeKey(sk, TemporalKey(ts.toInstant.toEpochMilli))
    )

    val nBands = bandIndices.size

    val tilesRDD: RDD[(SpaceTimeKey, MultibandTile)] = keys.map { stk =>
      val ids     = keyToCellIdsB.value(stk.spatialKey)
      val ts      = stk.instant
      val buffers = Array.fill(nBands)(Array.fill(tileCols * tileRows)(doubleNODATA))
      val lk      = lookupB.value
      var i = 0
      while (i < ids.length) {
        lk.get((ids(i), ts)) match {
          case Some(v) =>
            var b = 0
            while (b < nBands) { buffers(b)(i) = v(b); b += 1 }
          case None => // leave NoData
        }
        i += 1
      }
      val mb = MultibandTile(buffers.map(a => DoubleArrayTile(a, tileCols, tileRows, ct)))
      stk -> mb
    }

    val md = TileLayerMetadata(
      cellType = ct,
      layout   = layout,
      extent   = extent,
      crs      = targetCRS,
      bounds   = keyBounds
    )
    ContextRDD(tilesRDD, md)
  }
}

