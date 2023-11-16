package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis._
import geopyspark.util._
import geotrellis.raster.rasterize._
import geotrellis.vector._
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import protos.featureMessages._


object FeatureCellValueWrapper {
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[Feature[Geometry, CellValue], ProtoFeatureCellValue](testRDD(sc))

  def testIn(rdd: RDD[Array[Byte]]) =
    PythonTranslator.fromPython[Feature[Geometry, CellValue], ProtoFeatureCellValue](rdd, ProtoFeatureCellValue.parseFrom)

  def testRDD(sc: SparkContext): RDD[Feature[Geometry, CellValue]] = {
    val points: Seq[Point] = for (x <- 0 until 10) yield Point(x, x + 2)
    val lines: Seq[LineString] = points.grouped(5).map { LineString(_) }.toSeq
    val multiLines = MultiLineString(lines)

    sc.parallelize(
      Array(
        Feature(multiLines, CellValue(1, 0)),
        Feature(lines.head, CellValue(1, 0)),
        Feature(points.head, CellValue(2, 1))
      )
    )
  }
}

