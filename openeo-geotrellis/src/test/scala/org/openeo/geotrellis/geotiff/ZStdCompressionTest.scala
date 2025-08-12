package org.openeo.geotrellis.geotiff

import better.files.File.apply
import cats.data.NonEmptyList
import geotrellis.layer.{CRSWorldExtent, FloatingLayoutScheme, SpaceTimeKey, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiff, Tiled}
import geotrellis.raster.render.ColorMap.Options
import geotrellis.raster.render.DoubleColorMap
import geotrellis.raster.resample.Min
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{ByteArrayTile, ByteConstantNoDataCellType, ByteConstantTile, CellSize, ColorMaps, MultibandTile, Raster, Tile, TileLayout, UByteArrayTile, isData}
import geotrellis.spark._
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.junit.Assert._
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeAll, Test}
import org.junit.rules.TemporaryFolder
import org.junit.{AfterClass, Rule}
import org.openeo.geotrellis.LayerFixtures.loadFeaturesWithArtifactoryMock
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrellis.{LayerFixtures, OpenEOProcesses, ProjectedPolygons}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.zip.Deflater._
import scala.annotation.meta.getter
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.io.Directory


object ZStdCompressionTest {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[ZStdCompressionTest])
}

class ZStdCompressionTest extends RasterMatchers {

  import ZStdCompressionTest._

  @Test
  def testWrite(@TempDir tempDir: Path): Unit ={
    val compression = ZStdCompression
    val compressor = compression.createCompressor(1)
    val expected = "Dikke Zever"
    val bytes = compressor.compress(expected.getBytes, 0)
    val bytes1 = compressor.createDecompressor().decompress(bytes, 0);
    val actual = new String(bytes1)
    assertEquals(expected, actual)

  }


}
