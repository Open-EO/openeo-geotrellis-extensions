package geopyspark.geotrellis

import geotrellis.raster.histogram._
import geotrellis.raster.render._

import scala.jdk.CollectionConverters._

object ColorMapUtils {

  def importInt(x: Any): Int = {
    if (x.isInstanceOf[Int])
      x.asInstanceOf[Int]
    else {
      if (x.isInstanceOf[Long])
        x.asInstanceOf[Long].toInt
      else
        throw new IllegalArgumentException("Expected integral numerical argument")
    }
  }

  def fromMap(
    breaks: java.util.Map[Any, Any],
    noDataColor: Any,
    fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.asScala.toMap.map{ case (k, v) => (importInt(k), importInt(v)) }, opts)
  }

  def fromMapDouble(
    breaks: java.util.Map[Double, Any],
    noDataColor: Any,
    fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.asScala.toMap.view.mapValues(importInt).toMap, opts)
  }

  def fromBreaks(
    breaks: java.util.ArrayList[Any],
    colors: java.util.ArrayList[Any],
    noDataColor: Any, fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.asScala.toVector.map(importInt), ColorRamp(colors.asScala.map(importInt)), opts)
  }

  def fromBreaksDouble(
    breaks: java.util.ArrayList[Double],
    colors: java.util.ArrayList[Any],
    noDataColor: Any,
    fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.asScala.toVector, ColorRamp(colors.asScala.map(importInt)), opts)
  }

  def fromHistogram(
    hist: IntHistogram,
    colors: java.util.ArrayList[Any],
    noDataColor: Any,
    fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    val ramp  = ColorRamp(colors.asScala.map(importInt))
    ColorMap.fromQuantileBreaks(hist, ramp, opts)
  }

  def fromHistogram(
    hist: StreamingHistogram,
    colors: java.util.ArrayList[Any],
    noDataColor: Any,
    fallbackColor: Any,
    boundaryType: String
  ): ColorMap = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    val ramp  = ColorRamp(colors.asScala.map(importInt))
    ColorMap.fromQuantileBreaks(hist, ramp, opts)
  }
}
