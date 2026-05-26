package org.openeo.geotrellishealpix

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder

import java.sql.Timestamp
import scala.jdk.CollectionConverters._

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

    val grouped = df.rdd
      .map(r => (r.getAs[Timestamp](HealpixSchema.Timestamp), r))
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

  override def filterTemporal(start: String, end: String): HealpixDatacube = {
    val tsStart = Timestamp.valueOf(start.replace("T", " ").replace("Z", ""))
    val tsEnd   = Timestamp.valueOf(end.replace("T", " ").replace("Z", ""))
    copy(df = df.filter(
      col(HealpixSchema.Timestamp).between(tsStart, tsEnd)))
  }

  override def filterBands(bandNames: java.util.List[String]): HealpixDatacube = {
    val keep = bandNames.asScala.toSeq
    val selectCols = Seq(HealpixSchema.CellId, HealpixSchema.Timestamp) ++ keep
    val newBands = bands.filter { case (name, _) => keep.contains(name) }
    copy(bands = newBands, df = df.select(selectCols.map(col): _*))
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

