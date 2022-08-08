package org.openeo.geotrellis.file

import com.azavea.gdal.GDALWarp
import geotrellis.proj4.LatLng
import geotrellis.raster.CellSize
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector._
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNotEquals}
import org.junit.{AfterClass, Test}
import org.openeo.geotrellis.{LocalSparkContext, ProjectedPolygons}
import org.openeo.geotrellis.TestImplicits._

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId}
import java.util

object CglsPyramidFactoryTest extends LocalSparkContext {
  @AfterClass
  def tearDown(): Unit = GDALWarp.deinit()
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

  @Test
  def datacube_seqSparse(): Unit = {
    val ndvi300PyramidFactory = new CglsPyramidFactory(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
      bandName = "NDVI",
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox1 = Extent(124.07958984375001, -23.96115620034201, 125.408935546875, -22.715390019335942)
    val bbox2 = Extent(126.5130615234375, -26.416470240877764, 128.0291748046875, -25.11544539706194)
    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1, bbox2)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date)

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    assert(spatialLayer.metadata.crs == bboxCrs)

    /*val Some(stitchedRaster) = spatialLayer.sparseStitch(bbox1 combine bbox2) // sparseStitch!
    MultibandGeoTiff(stitchedRaster, bboxCrs).write("/tmp/ndvi300_sparse.tif")*/

    def physicalMean(polygon: Polygon): Double = {
      val Summary(Array(mean)) = spatialLayer
        .polygonalSummaryValue(polygon, MeanVisitor)

      val netCdfScalingFactor = 0.00400000018998981
      val netCdfOffset = -0.0799999982118607

      mean.mean * netCdfScalingFactor + netCdfOffset
    }

    // from QGIS with the corresponding geotiff
    assertEquals(0.23275508226336977, physicalMean(bbox1.extent.toPolygon()), 0.001)
    assertEquals(0.21305455611940785, physicalMean(bbox2.extent.toPolygon()), 0.001)
  }

  @Test
  def datacube_seqWithOpensearchClient(): Unit = {
    val ndvi300PyramidFactory = new CglsPyramidFactory2(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
      netcdfVariables = util.Arrays.asList("NDVI"),
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox = ProjectedExtent(Extent(124.0466308593749858, -26.4607380431908759, 128.0566406250000000, -22.6748473511885216), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, s"EPSG:${bbox.crs.epsgCode.get}")

    val emptyMap = new java.util.HashMap[String, Any]()
    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date, emptyMap)

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

  @Test
  def datacube_seqSparseWithOpensearchClient(): Unit = {
    val ndvi300PyramidFactory = new CglsPyramidFactory2(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
      netcdfVariables = util.Arrays.asList("NDVI"),
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox1 = Extent(124.07958984375001, -23.96115620034201, 125.408935546875, -22.715390019335942)
    val bbox2 = Extent(126.5130615234375, -26.416470240877764, 128.0291748046875, -25.11544539706194)
    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1, bbox2)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val emptyMap = new java.util.HashMap[String, Any]()
    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date, emptyMap)

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    assert(spatialLayer.metadata.crs == bboxCrs)

    val Some(stitchedRaster) = spatialLayer.sparseStitch(bbox1 combine bbox2) // sparseStitch!
    MultibandGeoTiff(stitchedRaster, bboxCrs).write("/tmp/factory2_ndvi300_sparse.tif")

    def physicalMean(polygon: Polygon): Double = {
      val Summary(Array(mean)) = spatialLayer
        .polygonalSummaryValue(polygon, MeanVisitor)

      val netCdfScalingFactor = 0.00400000018998981
      val netCdfOffset = -0.0799999982118607

      mean.mean * netCdfScalingFactor + netCdfOffset
    }

    // from QGIS with the corresponding geotiff
    assertEquals(0.23090684885355844, physicalMean(bbox1.extent.toPolygon()), 0.001)
    assertEquals(0.21204510422636474, physicalMean(bbox2.extent.toPolygon()), 0.001)
  }

  @Test
  def compare_factory1_to_factory2(): Unit = {
    val ndvi300PyramidFactory = new CglsPyramidFactory(
    dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
    bandName = "NDVI",
    dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    )

    val ndvi300PyramidFactory2 = new CglsPyramidFactory2(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc",
      netcdfVariables = util.Arrays.asList("NDVI"),
      dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox1 = Extent(124.07958984375001, -23.96115620034201, 125.408935546875, -22.715390019335942)
    val bbox2 = Extent(126.5130615234375, -26.416470240877764, 128.0291748046875, -25.11544539706194)
    val bboxCrs = LatLng

    val multiPolygons = Array(bbox1, bbox2)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val projectedPolygons = ProjectedPolygons(multiPolygons, bboxCrs)

    val emptyMap = new java.util.HashMap[String, Any]()

    val Seq((_, baseLayer1)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date)
    val Seq((_, baseLayer2)) = ndvi300PyramidFactory2.datacube_seq(projectedPolygons, from_date, to_date, emptyMap)
    val collectedLayer1 = baseLayer1.collect()
    val collectedLayer2 = baseLayer2.collect()

    assertEquals(baseLayer1.metadata.crs, baseLayer2.metadata.crs)
    assertEquals(baseLayer1.metadata.cellType, baseLayer2.metadata.cellType)

    // Factory1 uses the entire world as layout, with a small cellsize.
    // Factory2 uses the projectedPolygons.extent as a layout.
    assertNotEquals(baseLayer1.metadata.layout.cellSize, baseLayer2.metadata.layout.cellSize)
    assertNotEquals(baseLayer1.metadata.layout.rows, baseLayer2.metadata.layout.rows)
    assertNotEquals(baseLayer1.metadata.layout.cols, baseLayer2.metadata.layout.cols)
    assertNotEquals(baseLayer1.metadata.bounds, baseLayer2.metadata.bounds)

    assertEquals(collectedLayer1(0)._2.cellType.name, collectedLayer2(0)._2.cellType.name)
    assertEquals(collectedLayer1(0)._2.bandCount, collectedLayer2(0)._2.bandCount)
    assertEquals(collectedLayer1(0)._2.dimensions._1, collectedLayer2(0)._2.dimensions._1)
    assertEquals(collectedLayer1(0)._2.dimensions._2, collectedLayer2(0)._2.dimensions._2)
  }
}
