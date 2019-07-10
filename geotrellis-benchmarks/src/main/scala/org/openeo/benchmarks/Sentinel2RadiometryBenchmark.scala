package org.openeo.benchmarks

import java.net.URL

import org.openeo.geotrellisaccumulo.{PyramidFactory => AccumuloPyramidFactory}
import java.time.{Duration, Instant, LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util.Arrays

import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark.{MultibandTileLayerRDD, SpaceTimeKey}
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import geotrellis.shapefile.ShapeFileReader
import org.apache.spark.{SparkConf, SparkContext}
import org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory

object Sentinel2RadiometryBenchmark {

  def main(args: Array[String]): Unit = {
    val nFields = if (args.size >= 1) args(0).toInt else 1

    // Kryo can't serialize java.io.Serializable types by default -> broadcast variables end up null in the executors
    // val sc = SparkUtils.createSparkContext("Sentinel2RadiometryBenchmark")
    val sc = new SparkContext(new SparkConf().setAppName("Sentinel2RadiometryBenchmark"))

    try {
      val shapeFile = new URL("file:/data/CropSAR/data/ref/shp/2018_CroptypesFlemishParcels_DeptLV.shp")
      val crs = LatLng

      val bboxes = randomFields(shapeFile, amount = nFields)
        .map(field => ProjectedExtent(field.envelope, crs))

      val startDate = ZonedDateTime.of(LocalDate.of(2019, 7, 5), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      val endDate = startDate

      val bbox_srs = s"EPSG:${crs.epsgCode.get}"
      val (start_date, end_date) = (ISO_OFFSET_DATE_TIME format startDate, ISO_OFFSET_DATE_TIME format endDate)

      def accumuloPyramid(bbox: ProjectedExtent): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
        val fromAccumulo = new AccumuloPyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")
        fromAccumulo.pyramid_seq("CGS_SENTINEL2_RADIOMETRY_V102_EARLY", bbox.extent, bbox_srs, start_date, end_date)
      }

      def filePyramid(bbox: ProjectedExtent): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
        val fromFile = new Sentinel2RadiometryPyramidFactory
        fromFile.pyramid_seq(bbox.extent, bbox_srs, start_date, end_date, Arrays.asList(1, 2, 3, 7))
      }

      for ((bbox, i) <- bboxes.zipWithIndex) {
        val (_, accumuloDuration) = time {
          writeGeoTiff(accumuloPyramid(bbox), bbox, s"/tmp/fromAccumulo_$i.tif")
        }

        val (_, fileDuration) = time {
          writeGeoTiff(filePyramid(bbox), bbox, s"/tmp/fromFile_$i.tif")
        }

        println(s"Stitching an Accumulo pyramid took $accumuloDuration, while a file pyramid took $fileDuration.")
      }
    } finally sc.stop()
  }

  private def writeGeoTiff(pyramid: Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])], bbox: ProjectedExtent, outputFile: String): Unit = {
    val (_, baseLayer) = pyramid.head

    val raster = baseLayer.toSpatial()
      .crop(bbox.reproject(baseLayer.metadata.crs))
      .stitch()

    MultibandGeoTiff(raster, baseLayer.metadata.crs).write(outputFile)
  }

  private def randomFields(shapeFile: URL, amount: Int): Seq[MultiPolygon] = {
    import scala.util.Random

    val largeFields = ShapeFileReader.readMultiPolygonFeatures[Double](shapeFile, "area")
      .filter(_.data > 2500.0)

    Random.shuffle(largeFields)
      .map(_.geom)
      .take(amount)
  }

  private def time[R](body: => R): (R, Duration) = {
    val start = Instant.now()
    val result = body
    val end = Instant.now()

    (result, Duration.between(start, end))
  }
}
