package org.openeo.geotrellis.layers

import com.azavea.gdal.GDALWarp
import geotrellis.layer.{KeyBounds, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.UByteUserDefinedNoDataCellType
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{AfterClass, Test}
import org.openeo.geotrellis.LocalSparkContext
import org.openeo.geotrellis.TestImplicits._

import java.time.{LocalDate, ZoneId, ZonedDateTime}

object GlobalNetCdfFileLayerProviderTest extends LocalSparkContext {
  @AfterClass
  def tearDown(): Unit = GDALWarp.deinit()
}

class GlobalNetCdfFileLayerProviderTest {
  import GlobalNetCdfFileLayerProviderTest._

  private def layerProvider = new GlobalNetCdfFileLayerProvider(
    dataGlob = "/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/*/*/*/*.nc",
    bandName = "LAI",
    dateRegex = raw"_(\d{4})(\d{2})(\d{2})0000_".r.unanchored
  )

  @Test
  def readTileLayer(): Unit = {
    val date = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = layerProvider.readTileLayer(from = date, to = date, boundingBox, sc = sc).cache()

    layer
      .toSpatial(date)
      .writeGeoTiff("/tmp/lai300_georgia2.tif")
  }

  @Test
  def loadMetadata(): Unit = {
    val Some((ProjectedExtent(Extent(xmin, ymin, xmax, ymax), crs), dates)) = layerProvider.loadMetadata(sc)

    assertEquals(LatLng, crs)
    assertEquals(-180.0014881, xmin, 0.001)
    assertEquals(-59.9985119, ymin, 0.001)
    assertEquals(179.9985119, xmax, 0.001)
    assertEquals(80.0014881, ymax, 0.001)

    val years = dates.map(_.getYear).distinct

    assertTrue(years contains 2014)
    assertTrue(years contains 2020)
  }

  @Test
  def readMetadata(): Unit = {
    val TileLayerMetadata(cellType, layout, Extent(xmin, ymin, xmax, ymax), crs, KeyBounds(minKey, maxKey)) =
      layerProvider.readMetadata(sc = sc)

    assertEquals(UByteUserDefinedNoDataCellType(255.toByte), cellType)

    assertEquals(LatLng, crs)
    assertEquals(-180.0014881, xmin, 0.001)
    assertEquals(-59.9985119, ymin, 0.001)
    assertEquals(179.9985119, xmax, 0.001)
    assertEquals(80.0014881, ymax, 0.001)

    val expectedZoom = 9
    val expectedLayoutCols = math.pow(2, expectedZoom).toInt

    assertEquals(expectedLayoutCols, layout.layoutCols)
    assertEquals(minKey.col, 0)
    assertEquals(maxKey.col, expectedLayoutCols - 1)
  }

  @Test
  def zonalMean(): Unit = {
    val from = LocalDate.of(2017, 1, 10).atStartOfDay(ZoneId.of("UTC"))
    val to = LocalDate.of(2017, 1, 31).atStartOfDay(ZoneId.of("UTC"))
    val boundingBox = ProjectedExtent(Extent(-86.30859375, 29.84064389983441, -80.33203125, 35.53222622770337), LatLng)

    val layer = layerProvider.readTileLayer(from, to, boundingBox, sc = sc).cache()

    def mean(at: ZonedDateTime): Double = {
      val Summary(mean) = layer
        .toSpatial(at)
        .polygonalSummaryValue(boundingBox.extent.toPolygon(), MeanVisitor)

      val netCdfScalingFactor = 0.0333329997956753
      mean.mean * netCdfScalingFactor
    }

    // physical means derived from the corresponding geotiff in QGIS
    assertEquals(1.0038442394683125, mean(from), 0.001)
    assertEquals(1.0080865841250772, mean(to), 0.001)
  }
}
