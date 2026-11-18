package org.openeo.geotrellis.aggregate_polygon

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Describes where and how the result of an aggregate_spatial computation is written.
 *
 * @param path       output location (a directory)
 * @param format     "csv" or "parquet"
 * @param filesystem "file": a path on the local filesystem (shared volume or driver),
 *                   "distributed": a path on the default Spark filesystem, or a full URI such as s3a://bucket/dir
 */
case class AggregateOutputOptions(path: String,
                                  format: String = AggregateOutputOptions.CSV,
                                  filesystem: String = AggregateOutputOptions.FILE) {
  require(AggregateOutputOptions.formats.contains(format),
    s"unsupported output format '$format', expected one of ${AggregateOutputOptions.formats.mkString(", ")}")
  require(AggregateOutputOptions.filesystems.contains(filesystem),
    s"unsupported output filesystem '$filesystem', expected one of ${AggregateOutputOptions.filesystems.mkString(", ")}")

  def uri: String = if (filesystem == AggregateOutputOptions.FILE) "file://" + path else path

  def write(dataframe: DataFrame): Unit = {
    val writer = dataframe.coalesce(1).write.mode(SaveMode.Overwrite)
    format match {
      case AggregateOutputOptions.CSV => writer.option("header", "true").option("emptyValue", "").csv(uri)
      case AggregateOutputOptions.PARQUET => writer.parquet(uri)
    }
  }
}

object AggregateOutputOptions {
  val CSV = "csv"
  val PARQUET = "parquet"
  val FILE = "file"
  val DISTRIBUTED = "distributed"

  val formats: Seq[String] = Seq(CSV, PARQUET)
  val filesystems: Seq[String] = Seq(FILE, DISTRIBUTED)
}
