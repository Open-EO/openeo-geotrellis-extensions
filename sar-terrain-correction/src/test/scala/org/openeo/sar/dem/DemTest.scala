package org.openeo.sar.dem

import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellSize, FloatArrayTile, Raster, RasterExtent}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Assumptions, Test}
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.metadata.Polarisation
import org.openeo.sar.{SarProcessingConfig, TerrainCorrectionProcessor, TileComputeContext, TileRequest}

import java.net.URI
import java.nio.file.Files

class DemTest {

  private def writeDem(extent: Extent, cellSize: Double, height: Float): DemItem = {
    val cols = math.round(extent.width / cellSize).toInt
    val rows = math.round(extent.height / cellSize).toInt
    val path = Files.createTempFile("dem-", ".tif")
    path.toFile.deleteOnExit()
    GeoTiff(Raster(FloatArrayTile.fill(height, cols, rows), extent), LatLng).write(path.toString, true)
    DemItem(extent, path.toString)
  }

  private def request(extent: Extent, config: SarProcessingConfig = SarProcessingConfig.default) =
    TileRequest(extent, CellSize(0.01, 0.01), LatLng, Seq(Polarisation.VV), config)

  private def heights(items: Seq[DemItem], req: TileRequest): geotrellis.raster.Tile =
    DemReader.read(items, req, RasterExtent(req.extent, req.cellSize.width, req.cellSize.height, req.cols, req.rows))

  private val unreadable = DemItem(Extent(0, 0, 1, 1), "/does/not/exist.tif")

  @Test
  def elevationModelNoneMeansDefaultCopernicus30(): Unit =
    assertEquals(ElevationModel.catalog(Some("COPERNICUS_30"), "data").toString, ElevationModel.catalog(None, "data").toString)

  @Test
  def elevationModelNullStringMeansNoDem(): Unit =
    assertSame(NoDemCatalog, ElevationModel.catalog(Some("null"), "data"))

  @Test
  def elevationModelAliasAndUrlGiveStacCatalog(): Unit = {
    val expected = "StacDemCatalog(https://stac.dataspace.copernicus.eu/v1, cop-dem-glo-30-dged-cog, data)"
    assertEquals(expected, ElevationModel.catalog(Some("copernicus_30"), "data").toString)
    assertEquals(expected, ElevationModel.catalog(Some(ElevationModel.Copernicus30Url + "/"), "data").toString)
  }

  @Test
  def unknownElevationModelIsRejected(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => ElevationModel.catalog(Some("SRTMGL1"), "data"))

  @Test
  def splitAtAntimeridian(): Unit = {
    assertEquals(Seq(Extent(10, 20, 11, 21)), DemGeometry.splitAtAntimeridian(10, 20, 11, 21))
    assertEquals(Seq(Extent(179, 0, 180, 1), Extent(-180, 0, -179, 1)), DemGeometry.splitAtAntimeridian(179, 0, 181, 1))
    assertEquals(Seq(Extent(-180, 0, -179, 1), Extent(179, 0, 180, 1)), DemGeometry.splitAtAntimeridian(-181, 0, -179, 1))
    // STAC bbox convention: xmin > xmax
    assertEquals(Seq(Extent(179, 0, 180, 1), Extent(-180, 0, -179, 1)), DemGeometry.splitAtAntimeridian(179, 0, -179, 1))
    // latitude is clamped
    assertEquals(90.0, DemGeometry.splitAtAntimeridian(0, 89, 1, 89.9, buffer = 0.2).head.ymax, 0.0)
  }

  @Test
  def staticCatalogReturnsOverlappingItems(): Unit = {
    val west = DemItem(Extent(0, 0, 1, 1), "west")
    val east = DemItem(Extent(1, 0, 2, 1), "east")
    val far = DemItem(Extent(10, 10, 11, 11), "far")
    assertEquals(Seq(west, east), StaticDemCatalog(Seq(west, east, far)).items(Seq(Extent(0.5, 0.5, 1.5, 0.6))))
  }

  @Test
  def itemsAreMosaickedAcrossTileBoundaryAtFinestResolution(): Unit = {
    val west = writeDem(Extent(0, 0, 1, 1), 0.1, 100f)
    val east = writeDem(Extent(1, 0, 2, 1), 0.05, 200f) // finer than the first

    val req = request(Extent(0.8, 0.4, 1.2, 0.6))
    val dem = heights(Seq(west, east), req)

    assertEquals(req.cols, dem.cols)
    assertEquals(req.rows, dem.rows)
    assertEquals(100.0, dem.getDouble(2, 5), 1e-3)
    assertEquals(200.0, dem.getDouble(req.cols - 3, 5), 1e-3)
  }

  @Test
  def withoutCoverageHeightIsZero(): Unit = {
    val dem = heights(Seq.empty, request(Extent(10, 10, 10.1, 10.1)))
    assertEquals(0.0, dem.getDouble(0, 0), 0.0)
    assertEquals(0.0, dem.getDouble(dem.cols - 1, dem.rows - 1), 0.0)
  }

  @Test
  def partialCoverageHeightIsZeroOutsideTheItem(): Unit = {
    val item = writeDem(Extent(0, 0, 1, 1), 0.1, 100f)
    val req = request(Extent(0.8, 0.4, 1.2, 0.6)) // east half is beyond the item
    val dem = heights(Seq(item), req)

    assertEquals(100.0, dem.getDouble(2, 5), 1e-3)
    assertEquals(0.0, dem.getDouble(req.cols - 3, 5), 0.0)
  }

  @Test
  def unreadableItemFailsTheRead(): Unit =
    assertThrows(classOf[Exception], () => heights(Seq(unreadable), request(Extent(0.1, 0.1, 0.2, 0.2))))

  private def context(req: TileRequest, items: Seq[DemItem]) =
    TileComputeContext(req, null, Map.empty, items, geoidSource = None)

  @Test
  def unreadableItemIsASoftErrorGivingNaN(): Unit = {
    val dem = TerrainCorrectionProcessor.readDemEllipsoidal(context(request(Extent(0.1, 0.1, 0.2, 0.2)), Seq(unreadable)))
    assertTrue(dem.forall(_.forall(java.lang.Float.isNaN)))
  }

  @Test
  def unreadableItemFailsWhenSoftErrorsAreDisabled(): Unit = {
    val req = request(Extent(0.1, 0.1, 0.2, 0.2), SarProcessingConfig(softErrors = false))
    assertThrows(classOf[IllegalStateException],
      () => TerrainCorrectionProcessor.readDemEllipsoidal(context(req, Seq(unreadable))))
  }

  @Test
  def noItemsGivesZeroHeightWithoutSoftError(): Unit = {
    val dem = TerrainCorrectionProcessor.readDemEllipsoidal(context(request(Extent(0.1, 0.1, 0.2, 0.2)), Seq.empty))
    assertTrue(dem.forall(_.forall(_ == 0.0)))
  }

  @Test
  def missingGeoidFailsWhenOpeningAScene(): Unit = {
    val processor = new TerrainCorrectionProcessor(new NativeBackend, geoidSourceFactory = None)
    val e = assertThrows(classOf[IllegalStateException],
      () => processor.openScene(new URI("file:///unused.json"), CellSize(10, 10), LatLng, Seq(Polarisation.VV)))
    assertTrue(e.getMessage.contains("geoid"))
  }

  @Test
  def stacCatalogFindsCopernicusItemsAcrossTiles(): Unit = {
    Assumptions.assumeTrue(System.getProperty("runOnlineTests") != null, "online test disabled (-DrunOnlineTests=true)")

    val catalog = ElevationModel.catalog(Some("COPERNICUS_30"), "data")
    val search = Seq(Extent(3.9, 50.9, 4.1, 51.1))
    val items = catalog.items(search)

    assertEquals(4, items.size)
    assertTrue(items.forall(_.href.startsWith("s3://eodata/")), items.toString)
    assertSame(items, catalog.items(search), "second lookup should be cached")
  }
}
