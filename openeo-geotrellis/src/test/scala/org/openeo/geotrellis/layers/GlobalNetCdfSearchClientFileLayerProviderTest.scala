package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import com.azavea.gdal.GDALWarp
import geotrellis.layer.FloatingLayoutScheme
import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.CellSize
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{AfterAll, Test}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.{LocalSparkContext, MergeCubesSpec, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.backends.GlobalNetCDFSearchClient

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.jdk.CollectionConverters._

object GlobalNetCdfSearchClientFileLayerProviderTest {
  @AfterAll
  def tearDown(): Unit = GDALWarp.deinit()
}

@EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
class GlobalNetCdfSearchClientFileLayerProviderTest extends LocalSparkContext {

  private val bands: util.List[String] = util.Arrays.asList("LAI", "NOBS")

  private def multibandFileLayerProvider(maxSpatialResolution: CellSize = CellSize(0.002976190476204, 0.002976190476190)) = FileLayerProvider(
    new GlobalNetCDFSearchClient(
      dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/*/*/*/*.nc",
      bands,
      dateRegex = raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored
    ),
    openSearchCollectionId = "BioPar_LAI300_V1_Global",
    openSearchLinkTitles = NonEmptyList.fromListUnsafe(bands.asScala.toList),
    rootPath = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global",
    maxSpatialResolution = maxSpatialResolution,
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = FloatingLayoutScheme(256)
  )

  @Test
  def readTileLayerWithOpensearchClient(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = multibandFileLayerProvider().readTileLayer(from = date, to = date, boundingBox, sc = sc).cache()

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2.tif")
  }

  @Test
  def readDataCubeWithOpensearchClient(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, "EPSG:4326")
    val layer = multibandFileLayerProvider().readMultibandTileLayer(
      date, date, boundingBox, polygons.polygons, polygons.crs, 0, sc, Option(parameters)).cache()

    val (_, arbitraryTile) = layer.first()
    assertEquals(2, arbitraryTile.bandCount)

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2.tif")
  }

  /**
   * Test if fetching an UTM extent from a global LatLng feature works fine
   */
  @Test
  def readDataCubeWithOpensearchClientUTM(@TempDir tmpDir: java.nio.file.Path): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val point = (-86.30859375, 31.5)
    val boundingBoxLatLng = ProjectedExtent(Extent(point._1, point._2, point._1 + 0.01, point._2 + 0.01), LatLng)
    val utmCrs = UTM.getZoneCrs(boundingBoxLatLng.extent.center.getX, boundingBoxLatLng.extent.center.getY)
    val boundingBox = ProjectedExtent(boundingBoxLatLng.reproject(utmCrs), utmCrs)

    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, boundingBox.crs.toString())
    val layer = multibandFileLayerProvider(CellSize(868.4867662708275, 994.5852785776369)).readMultibandTileLayer(
      date, date, boundingBox, polygons.polygons, polygons.crs, 0, sc, Option(parameters)).cache()

    val (_, arbitraryTile) = layer.first()
    assertEquals(2, arbitraryTile.bandCount)

    layer
      .toSpatial(date)
      .writeGeoTiff(s"$tmpDir/readDataCubeWithOpensearchClientUTM.tif")

    val refFile = Thread.currentThread().getContextClassLoader.getResource(
      "org/openeo/geotrellis/GlobalNetCdfSearchClientFileLayerProviderTest/readDataCubeWithOpensearchClientUTM.tif")
    val refTiff = GeoTiff.readMultiband(refFile.getPath)
    val geotiff = GeoTiff.readMultiband(s"$tmpDir/readDataCubeWithOpensearchClientUTM.tif")

    val mse = MergeCubesSpec.simpleMeanSquaredError(geotiff.tile.band(0), refTiff.tile.band(0))
    println("MSE = " + mse)
    assertTrue(mse < 0.1)
  }

  /**
   * Test if fetching an LAEA extent from a global LatLng feature works fine
   */
  @Test
  def readDataCubeWithOpensearchClientLAEA(@TempDir tmpDir: java.nio.file.Path): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(3778000, 2937000, 4078000, 3181000), CRS.fromName("EPSG:3035"))
    val parameters = new DataCubeParameters()
    parameters.layoutScheme = "FloatingLayoutScheme"

    val polygons = ProjectedPolygons.fromExtent(boundingBox.extent, boundingBox.crs.toString())
    val layer = multibandFileLayerProvider(CellSize(868.4867662708275, 994.5852785776369)).readMultibandTileLayer(
      date, date, boundingBox, polygons.polygons, polygons.crs, 0, sc, Option(parameters)).cache()

    val (_, arbitraryTile) = layer.first()
    assertEquals(2, arbitraryTile.bandCount)
    layer
      .toSpatial(date)
      .writeGeoTiff(s"$tmpDir/readDataCubeWithOpensearchClientLAEA.tif")

    val refFile = Thread.currentThread().getContextClassLoader.getResource(
      "org/openeo/geotrellis/GlobalNetCdfSearchClientFileLayerProviderTest/readDataCubeWithOpensearchClientLAEA.tif")
    val refTiff = GeoTiff.readMultiband(refFile.getPath)
    val geotiff = GeoTiff.readMultiband(s"$tmpDir/readDataCubeWithOpensearchClientLAEA.tif")

    val mse = MergeCubesSpec.simpleMeanSquaredError(geotiff.tile.band(0), refTiff.tile.band(0))
    println("MSE = " + mse)
    // TODO: Only a few pixels can change depending on intersection method.
    //  Use max_nonmatch_ratio + tolerance like in Python
    assertTrue(mse < 3)
  }

  @Test
  def zonalMeanWithOpensearchClient(): Unit = {
    val from = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val to = LocalDate.of(2017, 2, 1).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = multibandFileLayerProvider().readTileLayer(from, to, boundingBox, sc = sc).cache()

    def mean(at: ZonedDateTime): Double = {
      val Summary(mean) = layer
        .toSpatial(at)
        .polygonalSummaryValue(boundingBox.extent.toPolygon(), MeanVisitor)

      val netCdfScalingFactor = 0.0333329997956753
      mean.mean * netCdfScalingFactor
    }

    assertEquals(1.0040181153452403, mean(from), 0.001)
    assertEquals(1.0082429593164433, mean(to.minusDays(1)), 0.001)
  }

}
