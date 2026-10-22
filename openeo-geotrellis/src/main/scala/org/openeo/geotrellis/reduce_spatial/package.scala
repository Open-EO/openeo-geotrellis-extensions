package org.openeo.geotrellis

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.isNoData
import geotrellis.spark.MultibandTileLayerRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder
import org.openeo.geotrelliscommon.DatacubeSupport

package object reduce_spatial {
  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  def reduceSpatiotemporalCube(cube: MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder: SparkAggregateScriptBuilder,
                               outputDir: String): Unit = {
    val isFloatingPoint = cube.metadata.cellType.isFloatingPoint
    val bandCount = new OpenEOProcesses().RDDBandCount(cube)

    val pixelRdd: RDD[Row] = for {
      keyedMultibandTile <- cube
      (spaceTimeKey, multibandTile) = keyedMultibandTile // odd that this doesn't work directly
      date = java.sql.Timestamp.from(spaceTimeKey.time.toInstant)
      row <- 0 until multibandTile.rows
      col <- 0 until multibandTile.cols
      bandValues = multibandTile.bands.map { tile =>
        if (isFloatingPoint) {
          val value = tile.getDouble(col, row)
          if (isNoData(value)) null else value
        } else {
          val value = tile.get(col, row)
          if (isNoData(value)) null else value
        }
      }
    } yield Row.fromSeq(date +: bandValues)

    val dataType = if (isFloatingPoint) DoubleType else IntegerType
    val bandColumns = DatacubeSupport.maybeBandLabels(cube)
      .getOrElse((0 until bandCount).map(bandIndex => s"band_$bandIndex"))

    val bandStructs = bandColumns.map(StructField(_, dataType))
    val dateStruct = StructField("date", TimestampType)

    val df = spark
      .createDataFrame(pixelRdd, schema = StructType(dateStruct +: bandStructs))

    val filteredDf =
      if (scriptBuilder.nodataIsIgnored)
        df.filter(bandColumns
          .map { colName =>
            val col = df.col(colName)
            col.isNotNull and !col.isNaN
          }
          .reduce {_ or _}
        )
      else df

    val expressionBuilder = scriptBuilder.generateFunction()
    val expressionColumns = for {
      colName <- bandColumns
      expressionColumn <- expressionBuilder(filteredDf.col(colName), colName)
    } yield expressionColumn

    val aggregated = filteredDf
      .groupBy("date")
      .agg(expressionColumns.head, expressionColumns.tail: _*)

    aggregated
      .coalesce(1)
      .write
      .option("header", value = true)
      .option("emptyValue", "")
      .mode(SaveMode.Overwrite)
      .csv(s"file://$outputDir")
  }

  def reduceSpatialCube(cube: MultibandTileLayerRDD[SpatialKey], scriptBuilder: SparkAggregateScriptBuilder,
                                  outputDir: String): Unit = {
    // TODO: reduce code duplication with reduce_spatial
    import org.apache.spark.sql._

    val isFloatingPoint = cube.metadata.cellType.isFloatingPoint
    val bandCount = new OpenEOProcesses().RDDBandCount(cube)

    val pixelRdd: RDD[Row] = for {
      multibandTile <- cube.values
      row <- 0 until multibandTile.rows
      col <- 0 until multibandTile.cols
      bandValues = multibandTile.bands.map { tile =>
        if (isFloatingPoint) {
          val value = tile.getDouble(col, row)
          if (isNoData(value)) null else value
        } else {
          val value = tile.get(col, row)
          if (isNoData(value)) null else value
        }
      }
    } yield Row.fromSeq(bandValues)

    val dataType = if (isFloatingPoint) DoubleType else IntegerType
    val bandColumns = DatacubeSupport.maybeBandLabels(cube)
      .getOrElse((0 until bandCount).map(bandIndex => s"band_$bandIndex"))

    val bandStructs = bandColumns.map(StructField(_, dataType))

    val df = spark.createDataFrame(pixelRdd, schema = StructType(bandStructs))

    val filteredDf =
      if (scriptBuilder.nodataIsIgnored)
        df.filter(bandColumns
          .map { colName =>
            val col = df.col(colName)
            col.isNotNull and !col.isNaN
          }
          .reduce {_ or _}
        )
      else df

    val expressionBuilder = scriptBuilder.generateFunction()
    val expressionColumns = for {
      colName <- bandColumns
      expressionColumn <- expressionBuilder(filteredDf.col(colName), colName)
    } yield expressionColumn

    val aggregated = filteredDf.agg(expressionColumns.head, expressionColumns.tail: _*)

    aggregated
      .coalesce(1)
      .write
      .option("header", value = true)
      .option("emptyValue", "")
      .mode(SaveMode.Overwrite)
      .csv(s"file://$outputDir")
  }
}
