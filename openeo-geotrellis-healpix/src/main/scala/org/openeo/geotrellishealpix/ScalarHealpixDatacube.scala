package org.openeo.geotrellishealpix

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

/** One HEALPix cell per row. Simple but row-overhead heavy. */
final case class ScalarHealpixDatacube(
  nside: Int,
  bands: Seq[(String, DataType)],
  df: DataFrame
) extends HealpixDatacube {

  override def applyProcess(scriptBuilder: OpenEOProcessScriptBuilder,
                            context: java.util.Map[String, Any]): HealpixDatacube = {
    val bandNames = bands.map(_._1)
    val schema    = df.schema
    val spark     = df.sparkSession

    // Group by timestamp so each "tile" represents a single time-slice.
    // For very large slices this should be further chunked; left as a TODO
    // because the packed layout already provides a natural chunking strategy.
    val grouped = df.rdd
      .map(r => (r.getAs[java.sql.Timestamp](HealpixSchema.Timestamp), r))
      .groupByKey()

    val processedRdd = grouped.flatMap { case (ts, rows) =>
      val rowSeq = rows.toSeq
      val (tile, cellIds) = HealpixTileBridge.rowsToTile(rowSeq, bandNames, schema)
      val processed = HealpixTileBridge.runProcess(tile, scriptBuilder, context)
      HealpixTileBridge.tileToRows(processed, cellIds, ts, bandNames, schema)
    }

    val newDf = spark.createDataFrame(processedRdd, schema)
    copy(df = newDf)
  }
}

object ScalarHealpixDatacube {
  def empty(spark: SparkSession,
            nside: Int,
            bands: Seq[(String, DataType)]): ScalarHealpixDatacube = {
    val schema = HealpixSchema.scalarSchema(bands)
    val empty  = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    ScalarHealpixDatacube(nside, bands, empty)
  }
}

