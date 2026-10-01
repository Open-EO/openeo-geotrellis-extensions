package org.openeo.geotrellishealpix

import org.apache.spark.sql.types._

/**
 * Common column names and schemas for HEALPix-backed openEO datacubes.
 *
 * The GeoTrellis `SpaceTimeKey` is replaced by two columns:
 *   - `cell_id`   : unique HEALPix cell identifier (LongType)
 *   - `timestamp` : observation time (TimestampType)
 *
 * Bands are stored as additional columns. Each band column can have its own data type.
 *
 * Two storage layouts are supported, so they can be benchmarked against each other:
 *   - Scalar: one row per (cell_id, timestamp); band columns are scalar values.
 *   - Packed: one row per (cell_id_start, timestamp); band columns are arrays
 *             of values for a contiguous block of HEALPix cell ids starting at
 *             `cell_id_start`, of length `chunk_size`.
 */
object HealpixSchema {
  val CellId: String      = "cell_id"
  val CellIdStart: String = "cell_id_start"
  val ChunkSize: String   = "chunk_size"
  val Timestamp: String   = "timestamp"

  def scalarSchema(bands: Seq[(String, DataType)]): StructType = {
    val base = Seq(
      StructField(CellId,    LongType,      nullable = false),
      StructField(Timestamp, TimestampType, nullable = false)
    )
    StructType(base ++ bands.map { case (n, t) => StructField(n, t, nullable = true) })
  }

  def packedSchema(bands: Seq[(String, DataType)]): StructType = {
    val base = Seq(
      StructField(CellIdStart, LongType,      nullable = false),
      StructField(ChunkSize,   IntegerType,   nullable = false),
      StructField(Timestamp,   TimestampType, nullable = false)
    )
    StructType(base ++ bands.map {
      case (n, t) => StructField(n, ArrayType(t, containsNull = true), nullable = true)
    })
  }
}

