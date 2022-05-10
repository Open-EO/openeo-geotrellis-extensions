package org.openeo.geotrellis

import ai.catboost.spark._
import geotrellis.raster.{FloatArrayTile, FloatCellType, Tile}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util
import java.util.Random
import scala.collection.{Seq, mutable}

class OpenEOProcessScriptBuilderTest {

  private def predictWithDefaultCatBoostClassifier(tiles: mutable.Buffer[Tile], random: Random, predict_expression: String = "predict_catboost"): Seq[Tile] = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("CatBoostClassifierTest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Create training data.
    val srcDataSchema = Seq(
      StructField("features", SQLDataTypes.VectorType),
      StructField("label", IntegerType)
    )
    val trainData = Seq(
      Row(Vectors.dense(4.5, 4, 4.8), 0),
      Row(Vectors.dense(3.1, 3.2, 3.8), 2),
      Row(Vectors.dense(0.13, 0.22, 0.23), 1),
      Row(Vectors.dense(9, 9.6, 9.8), 3)
    )
    val trainDf = spark.createDataFrame(spark.sparkContext.parallelize(trainData), StructType(srcDataSchema))
    val trainPool = new Pool(trainDf)

    // Fit classifier.
    val classifier = new CatBoostClassifier
    val model = classifier.fit(trainPool)

    // Reduce tiles using classifier predictions.
    val builder = new OpenEOProcessScriptBuilder
    val arguments = new util.HashMap[String, AnyRef]
    arguments.put("model", new util.HashMap[String, String]() {{put("from_parameter", "context")}})
    arguments.put("data", "dummy")
    builder.expressionStart(predict_expression, arguments)

    builder.argumentStart("data")
    builder.fromParameter("data")
    builder.argumentEnd()

    builder.expressionEnd(predict_expression, arguments)

    val context = Map[String,Any]("context" -> model)
    val result = builder.generateFunction(context).apply(tiles)
    SparkContext.getOrCreate.stop()
    result
  }

  @Test
  def testPredictCatBoost(): Unit = {
    val random = new Random(42)
    val tile0 = FloatArrayTile.empty(4, 4)
    val tile1 = FloatArrayTile.empty(4, 4)
    val tile2 = FloatArrayTile.empty(4, 4)
    for (col <- 0 until 4) {
      for (row <- 0 until 4) {
        tile0.setDouble(col, row, random.nextDouble * 10)
        tile1.setDouble(col, row, random.nextDouble * 10)
        tile2.setDouble(col, row, random.nextDouble * 10)
      }
    }
    val tiles = mutable.Buffer[Tile](tile0, tile1, tile2)
    val result = predictWithDefaultCatBoostClassifier(tiles, random)
    assertEquals(FloatCellType.withDefaultNoData, result.apply(0).cellType)
    assertEquals(3, result.head.get(0, 0))
    assertEquals(0, result.head.get(0, 1))
    assertEquals(2, result.head.get(0, 2))
    assertEquals(0, result.head.get(3, 2))
    assertEquals(2, result.head.get(3, 3))
  }

  @Test
  def testPredictCatBoostProbabilities(): Unit = {
    val random = new Random(42)
    val tile0 = FloatArrayTile.empty(4, 4)
    val tile1 = FloatArrayTile.empty(4, 4)
    val tile2 = FloatArrayTile.empty(4, 4)
    for (col <- 0 until 4) {
      for (row <- 0 until 4) {
        tile0.setDouble(col, row, random.nextDouble * 10)
        tile1.setDouble(col, row, random.nextDouble * 10)
        tile2.setDouble(col, row, random.nextDouble * 10)
      }
    }
    val tiles = mutable.Buffer[Tile](tile0, tile1, tile2)
    val result = predictWithDefaultCatBoostClassifier(tiles, random, "predict_catboost_probabilities")
    // Result = Vector[3] =? Should be Vector[4]
    // result[0] = ArrayTile(4,4,float32) with 16 probabilities => Correct.
    assertEquals(FloatCellType.withDefaultNoData, result.apply(0).cellType)
    assertEquals(3, result.map(t => t.getDouble(0,0)).zipWithIndex.maxBy(_._1)._2)
    assertEquals(0, result.map(t => t.getDouble(0,1)).zipWithIndex.maxBy(_._1)._2)
    assertEquals(2, result.map(t => t.getDouble(0,2)).zipWithIndex.maxBy(_._1)._2)
    assertEquals(0, result.map(t => t.getDouble(3,2)).zipWithIndex.maxBy(_._1)._2)
    assertEquals(2, result.map(t => t.getDouble(3,3)).zipWithIndex.maxBy(_._1)._2)
  }
}
