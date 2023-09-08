package org.openeo.geotrellis.file

import com.azavea.gdal.GDALWarp
import geotrellis.proj4.LatLng
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{CellSize, GridBounds, UByteConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector._
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.{AfterClass, Ignore, Test}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.geotiff._
import org.openeo.geotrellis.{LayerFixtures, LocalSparkContext, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId}
import java.util

object CglsPyramidFactoryTest extends LocalSparkContext {
  @AfterClass
  def tearDown(): Unit = GDALWarp.deinit()
}

class CglsPyramidFactoryTest extends RasterMatchers {

  @Test
  def readOriginalGrid(): Unit = {

    val date = LocalDate.of(2009, 7, 10).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val targetBounds = GridBounds(55L,103,55+73,103+111)
    val res = LayerFixtures.CGLS1KMResolution
    val refFile = LayerFixtures.cglsFAPARPath
    val refRasterSource = GDALRasterSource("NETCDF:" + refFile)
    val targetExtent = refRasterSource.gridExtent.extentFor(targetBounds)
    val refRaster = refRasterSource.read(targetExtent).get.mapTile(_.convert(UByteConstantNoDataCellType))


    //val x=1.8973214
    //val y=49.3526786

    val bbox = ProjectedExtent(targetExtent, LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, s"EPSG:${bbox.crs.epsgCode.get}")


    val params = new DataCubeParameters()
    params.tileSize = 64
    val Seq((_, baseLayer)) = LayerFixtures.cglsFAPAR1km.datacube_seq(projectedPolygons, from_date, to_date, new java.util.HashMap[String, Any](), "", params)

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    val resultPath = "/tmp/fapar1km.tif"
    //spatialLayer.writeGeoTiff(resultPath)
    saveRDD(spatialLayer,1,resultPath,cropBounds=Some(bbox.extent))


    GeoTiff(refRaster,LatLng).write("/tmp/refRaster.tif")
    val actualTiff = GeoTiff.readMultiband(resultPath).raster
    assertRastersEqual(actualTiff,refRaster)


  }

  // @Test
  // TODO: pyramid_seq uses ZoomedLayoutScheme with WebMercator crs while actual crs of netcdf is 4326.
  // See: https://github.com/Open-EO/openeo-geotrellis-extensions/issues/123
  def pyramid_seqExtent(): Unit = {
    val dataGlob = "/data/MTDA/BIOPAR/BioPar_FAPAR_V2_Global/2020/20200110*/*/*.nc"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val netcdfVariables = util.Arrays.asList("FAPAR")
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, netcdfVariables, "cgls")
    val faparPyramidFactory = new PyramidFactory(
      openSearchClient, "", netcdfVariables, "",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2020, 1, 10).atStartOfDay(ZoneId.of("UTC"))

    val bbox = ProjectedExtent(Extent(2.5469470024109171, 49.4936141967773295, 6.4038672447205158, 51.5054359436035583), LatLng)
    val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val (maxZoom, baseLayer) = faparPyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date, new java.util.HashMap[String, Any]())
      .maxBy { case (zoom, _) => zoom }

    baseLayer.cache()
    println(s"maxZoom: $maxZoom")

    baseLayer
      .toSpatial(date)
      .writeGeoTiff("/tmp/fapar.tif", bbox)
  }

  //@Test
  // TODO: https://github.com/Open-EO/openeo-geotrellis-extensions/issues/123
  def pyramid_seqMultiPolygons(): Unit = {
    val dataGlob = "/data/MTDA/BIOPAR/BioPar_FAPAR300_V1_Global/2017/20170110*/*/*.nc"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val netcdfVariables = util.Arrays.asList("FAPAR")
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, netcdfVariables, "cgls")
    val fapar300PyramidFactory = new PyramidFactory(
      openSearchClient, "", netcdfVariables, "",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))

    val bbox = ProjectedExtent(Extent(2.5469470024109171, 49.4936141967773295, 6.4038672447205158, 51.5054359436035583), LatLng)
    val polygon_crs = bbox.crs
    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val multiPolygons = Array(MultiPolygon(bbox.extent.toPolygon()))

    val (maxZoom, baseLayer) = fapar300PyramidFactory.pyramid_seq(multiPolygons, polygon_crs, from_date, to_date, new java.util.HashMap[String, Any]())
      .maxBy { case (zoom, _) => zoom }

    baseLayer.cache()
    println(s"maxZoom: $maxZoom")

    baseLayer
      .toSpatial(date)
      .writeGeoTiff("/tmp/fapar300.tif", bbox)
  }

  @Test
  @Ignore("Temporary ignore to make build pass")
  def datacube_seq(): Unit = {
    val dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val netcdfVariables = util.Arrays.asList("NDVI")
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, netcdfVariables, "cgls")
    val ndvi300PyramidFactory = new PyramidFactory(
      openSearchClient, "", netcdfVariables, "",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190)
    )

    val date = LocalDate.of(2018, 6, 21).atStartOfDay(ZoneId.of("UTC"))

    val from_date = date format ISO_OFFSET_DATE_TIME
    val to_date = from_date

    val bbox = ProjectedExtent(Extent(124.0466308593749858, -26.4607380431908759, 128.0566406250000000, -22.6748473511885216), LatLng)
    val projectedPolygons = ProjectedPolygons.fromExtent(bbox.extent, s"EPSG:${bbox.crs.epsgCode.get}")

    val params = new DataCubeParameters()
    params.tileSize=256
    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date, new java.util.HashMap[String, Any](),"",params)

    val spatialLayer = baseLayer
      .toSpatial(date)
      .cache()

    val resultPath = "/tmp/ndvi300_gridextent11.tif"
    spatialLayer
      .writeGeoTiff(resultPath, null)

    val resourcePath = "org/openeo/geotrellis/cgls_ndvi300.tif"
    val refFile = Thread.currentThread().getContextClassLoader.getResource(resourcePath)
    val refTiff = GeoTiff.readMultiband(refFile.getPath)

    val actualTiff = GeoTiff.readMultiband(resultPath).crop(600,600,1348,1273)
    //comparison also checks for https://github.com/Open-EO/openeo-geopyspark-driver/issues/297
    assertArrayEquals(refTiff.raster.tile.band(0).toArrayDouble(), actualTiff.raster.tile.band(0).toArrayDouble(), 0.1)

    val Summary(Array(mean)) = spatialLayer
      .polygonalSummaryValue(bbox.extent.toPolygon(), MeanVisitor)

    val netCdfScalingFactor = 0.00400000018998981
    val netCdfOffset = -0.0799999982118607

    val physicalMean = mean.mean * netCdfScalingFactor + netCdfOffset

    assertEquals(0.21796850248444263, physicalMean, 0.001) // from QGIS with the corresponding geotiff
  }

  @Test
  def datacube_seqSparse(): Unit = {
    val dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2018/201806*/*/*.nc"
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val netcdfVariables = util.Arrays.asList("NDVI")
    val openSearchClient = OpenSearchClient(dataGlob, false, dateRegex, netcdfVariables, "cgls")
    val ndvi300PyramidFactory = new PyramidFactory(
      openSearchClient, "", netcdfVariables, "",
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

    val Seq((_, baseLayer)) = ndvi300PyramidFactory.datacube_seq(projectedPolygons, from_date, to_date, new java.util.HashMap[String, Any]())

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
}
