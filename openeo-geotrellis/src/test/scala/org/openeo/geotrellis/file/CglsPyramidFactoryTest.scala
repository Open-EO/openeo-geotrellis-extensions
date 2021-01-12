package org.openeo.geotrellis.file

import com.azavea.gdal.GDALWarp
import geotrellis.proj4.LatLng
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.TestImplicits._

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId}

object CglsPyramidFactoryTest {
  private implicit var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[CglsPyramidFactoryTest].getName)
  }

  @AfterClass
  def tearDown(): Unit = {
    sc.stop()
    GDALWarp.deinit()
  }
}

class CglsPyramidFactoryTest {

  @Test
  def pyramid_seqExtent(): Unit = {
    val faparPyramidFactory = new CglsPyramidFactory(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2020/20200110*/*/*.nc",
      bandName = "FAPAR",
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    )

    val date = LocalDate.of(2020, 1, 10).atStartOfDay(ZoneId.of("UTC"))

    val bbox = ProjectedExtent(Extent(2.5469470024109171, 49.4936141967773295, 6.4038672447205158, 51.5054359436035583), LatLng)
    val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val (maxZoom, baseLayer) = faparPyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date)
      .maxBy { case (zoom, _) => zoom }

    baseLayer.cache()
    println(s"maxZoom: $maxZoom")

    baseLayer
      .toSpatial(date)
      .writeGeoTiff("/tmp/fapar.tif", bbox)
  }

  @Test
  def pyramid_seqMultiPolygons(): Unit = {
    val fapar300PyramidFactory = new CglsPyramidFactory(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_FAPAR300_V1_Global/2017/20170110*/*/*.nc",
      bandName = "FAPAR",
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    )

    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))

    val bbox = ProjectedExtent(Extent(2.5469470024109171, 49.4936141967773295, 6.4038672447205158, 51.5054359436035583), LatLng)
    val polygon_crs = bbox.crs
    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val multiPolygons = Array(MultiPolygon(bbox.extent.toPolygon()))

    val (maxZoom, baseLayer) = fapar300PyramidFactory.pyramid_seq(multiPolygons, polygon_crs, from_date, to_date)
      .maxBy { case (zoom, _) => zoom }

    baseLayer.cache()
    println(s"maxZoom: $maxZoom")

    baseLayer
      .toSpatial(date)
      .writeGeoTiff("/tmp/fapar300.tif", bbox)
  }

  @Test
  def datacube_seq(): Unit = {
    val ndvi300PyramidFactory = new CglsPyramidFactory(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
      bandName = "NDVI",
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox = ProjectedExtent(Extent(124.0466308593749858, -26.4607380431908759, 128.0566406250000000, -22.6748473511885216), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, s"EPSG:${bbox.crs.epsgCode.get}")

    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date)

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    /*spatialLayer
      .writeGeoTiff("/tmp/ndvi300_small.tif", bbox)*/

    val Summary(Array(mean)) = spatialLayer
      .polygonalSummaryValue(bbox.extent.toPolygon(), MeanVisitor)

    val netCdfScalingFactor = 0.00400000018998981
    val netCdfOffset = -0.0799999982118607

    val physicalMean = mean.mean * netCdfScalingFactor + netCdfOffset

    assertEquals(0.21796850248444263, physicalMean, 0.001) // from QGIS with the corresponding geotiff
  }
}
