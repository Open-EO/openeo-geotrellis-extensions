package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, TargetCellSize, UByteCellType}
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows => assertThrowsJunit, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.layers.raster_source.{BandSelectionRasterSource, IndexedRasterSource, NoDataRasterSource, OpenEoSourcePath, SentinelXMLMetadataRasterSource, ValueOffsetRasterSource}
import software.amazon.awssdk.core.exception.AbortedException

import java.io.IOException

class BandCompositeRasterSourceTest extends RasterMatchers {

  // extent: 125.8323451450973920, -26.4635378273783921,
  //         128.0585356212979775, -24.4605616369025221
  private val singleBandGeotiffPath =
    Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/cgls_ndvi300.tif").getPath

  @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasGdalInstalled")
  @Test
  def readManyBounds(): Unit = {

    val warpOptions = GDALWarpOptions(alignTargetPixels = true)
    val rs = GDALRasterSource("../testdata/org/openeo/geotrellis/layers/Sentinel2FileLayerProviderTest/T11TNM_20210101T184759_B02.jp2", warpOptions)

    val rasterSources = NonEmptyList.of(
      rs
    )

    def compareForBounds(bounds: Seq[GridBounds[Long]] ) = {
      val composite = new BandCompositeRasterSource(rasterSources, crs = rs.crs, readFullTile = true, parallelRead = false)
      val result = composite.readBounds(bounds)

      val refComposite = new BandCompositeRasterSource(rasterSources, crs = rs.crs, readFullTile = false, parallelRead = false)
      val ref = refComposite.readBounds(bounds)

      result.zip(ref).foreach{case (a,b) => assertRastersEqual(a,b)}

    }

    compareForBounds(Seq(GridBounds(-20, 10, 200, 400), GridBounds(200, 10, 400, 400)))
    compareForBounds(Seq(GridBounds(20, 10, 200, 400), GridBounds(400, 300, 500, 400)))

  }

  @Test
  def singleBandGeoTiffRasterSource(): Unit = {
    val bbox = ProjectedExtent(Extent(126.0, -26.0, 127.0, -25.0), LatLng)

    val rasterSources = NonEmptyList.of(
      GeoTiffRasterSource(singleBandGeotiffPath),
    )

    val composite = new BandCompositeRasterSource(rasterSources, crs = bbox.crs)

    val Some(compositeRasterByExtent) = composite.read(bbox.extent)
    assertEquals(1, compositeRasterByExtent.tile.bandCount)
    assertFalse(compositeRasterByExtent.tile.band(0).isNoDataTile)
    assertTrue(compositeRasterByExtent.extent.equalsExact(bbox.extent, 0.01))

    val Some(compositeRasterByGridBounds) = composite.read(bounds = composite.gridExtent.gridBoundsFor(bbox.extent))
    assertEquals(1, compositeRasterByGridBounds.tile.bandCount)
    assertFalse(compositeRasterByGridBounds.tile.band(0).isNoDataTile)
    assertTrue(compositeRasterByGridBounds.extent.equalsExact(bbox.extent, 0.01))
  }

  @Test
  def emptyBand(): Unit = {
    val bbox = ProjectedExtent(Extent(126.0, -26.0, 127.0, -25.0), LatLng)

    val geoTiffRasterSource = GeoTiffRasterSource(singleBandGeotiffPath)

    val rasterSources = NonEmptyList.of(
      geoTiffRasterSource,
      NoDataRasterSource.instance(geoTiffRasterSource.gridExtent, geoTiffRasterSource.crs)
    )

    val composite = new BandCompositeRasterSource(rasterSources, crs = bbox.crs)

    val Some(compositeRasterByExtent) = composite.read(bbox.extent)
    assertEquals(2, compositeRasterByExtent.tile.bandCount)
    assertFalse(compositeRasterByExtent.tile.band(0).isNoDataTile)
    assertTrue(compositeRasterByExtent.tile.band(1).isNoDataTile)
    assertTrue(compositeRasterByExtent.extent.equalsExact(bbox.extent, 0.01))

    val Some(compositeRasterByGridBounds) = composite.read(bounds = composite.gridExtent.gridBoundsFor(bbox.extent))
    assertEquals(2, compositeRasterByGridBounds.tile.bandCount)
    assertFalse(compositeRasterByGridBounds.tile.band(0).isNoDataTile)
    assertTrue(compositeRasterByGridBounds.tile.band(1).isNoDataTile)
    assertTrue(compositeRasterByGridBounds.extent.equalsExact(bbox.extent, 0.01))
  }

  @Test
  def XmlBand(): Unit = {
    val bbox = ProjectedExtent(Extent(126.0, -26.0, 127.0, -25.0), LatLng)

    val geoTiffRasterSource = GeoTiffRasterSource(singleBandGeotiffPath)

    val rasterSources = NonEmptyList.of(
      geoTiffRasterSource,
      new SentinelXMLMetadataRasterSource(42, geoTiffRasterSource.crs, geoTiffRasterSource.gridExtent, OpenEoSourcePath("test")),
    )

    val composite = new BandCompositeRasterSource(rasterSources, crs = bbox.crs)

    val Some(compositeRasterByExtent) = composite.read(bbox.extent)
    assertEquals(2, compositeRasterByExtent.tile.bandCount)
    assertFalse(compositeRasterByExtent.tile.band(0).isNoDataTile)
    assertEquals(compositeRasterByExtent.tile.band(1).get(0, 0), 42, 0.0001)
    assertTrue(compositeRasterByExtent.extent.equalsExact(bbox.extent, 0.01))

    val Some(compositeRasterByGridBounds) = composite.read(bounds = composite.gridExtent.gridBoundsFor(bbox.extent))
    assertEquals(2, compositeRasterByGridBounds.tile.bandCount)
    assertFalse(compositeRasterByGridBounds.tile.band(0).isNoDataTile)
    assertEquals(compositeRasterByGridBounds.tile.band(1).get(0, 0), 42, 0.0001)
    assertTrue(compositeRasterByGridBounds.extent.equalsExact(bbox.extent, 0.01))
  }

  private def roundTrip[T](o: T): T = {
    val bytes = new java.io.ByteArrayOutputStream()
    val out = new java.io.ObjectOutputStream(bytes)
    out.writeObject(o)
    out.close()
    new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray)).readObject().asInstanceOf[T]
  }

  @Test
  def equalityAndHashCodeSurviveSerialization(): Unit = {
    // used as shuffle key: hashCode must not depend on object identity, see FileLayerProvider.convertToRasterRegions
    val b02 = GDALRasterSource("/data/does_not_exist_B02.jp2")
    val b03 = GDALRasterSource("/data/does_not_exist_B03.jp2")

    val composite = new BandCompositeRasterSource(NonEmptyList.of(b02, b03), crs = LatLng)
    val copy = roundTrip(composite)
    assertTrue(composite ne copy)
    assertEquals(composite, copy)
    assertEquals(composite.hashCode(), copy.hashCode())

    assertFalse(composite == new BandCompositeRasterSource(NonEmptyList.of(b02), crs = LatLng))
    assertFalse(composite == new BandCompositeRasterSource(NonEmptyList.of(b02, b03), crs = LatLng, readFullTile = true))
  }

  @Test
  def groupedBySourceAndSettingsSurviveTransformations(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val other = GeoTiffRasterSource(singleBandGeotiffPath.replace("cgls_ndvi300", "chl_masked"))
    val predefinedExtent = Some(physical.gridExtent)
    val composite = new BandCompositeRasterSource(
      NonEmptyList.of(IndexedRasterSource(physical, 0), other, IndexedRasterSource(physical, 0)),
      crs = physical.crs, attributes = Map("date" -> "2020-01-01"), predefinedExtent = predefinedExtent,
      softErrors = true, readFullTile = true)

    val groups = composite.groupedBySource.map { case (name, outputBands, _) => name -> outputBands }
    assertEquals(Seq(physical.name -> Seq(0, 2), other.name -> Seq(1)), groups)

    val converted = composite.convert(FloatConstantNoDataCellType).asInstanceOf[BandCompositeRasterSource]
    assertEquals(composite.attributes, converted.attributes)
    assertEquals(predefinedExtent, converted.predefinedExtent)
    assertTrue(converted.softErrors)
    assertTrue(converted.readFullTile)
    assertEquals(groups, converted.groupedBySource.map { case (name, outputBands, _) => name -> outputBands })
  }

  @Test
  def reprojectionUpdatesCrsAndPredefinedExtent(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val composite = new BandCompositeRasterSource(
      NonEmptyList.of(IndexedRasterSource(physical, 0), physical),
      crs = physical.crs, predefinedExtent = Some(physical.gridExtent))

    val reprojected = composite.reproject(WebMercator).asInstanceOf[BandCompositeRasterSource]
    assertEquals(WebMercator, reprojected.crs)
    assertTrue(reprojected.sources.forall(_.crs == WebMercator))
    assertEquals(reprojected.sources.head.gridExtent, reprojected.gridExtent)
  }

  @Test
  def resampleUpdatesPredefinedExtent(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val composite = new BandCompositeRasterSource(
      NonEmptyList.of(IndexedRasterSource(physical, 0), physical),
      crs = physical.crs, predefinedExtent = Some(physical.gridExtent))

    val target = TargetCellSize(CellSize(physical.cellSize.width * 2, physical.cellSize.height * 2))
    val resampled = composite.resample(target, geotrellis.raster.resample.NearestNeighbor, geotrellis.raster.io.geotiff.Auto(0)).asInstanceOf[BandCompositeRasterSource]
    assertEquals(physical.crs, resampled.crs)
    assertEquals(target.cellSize, resampled.gridExtent.cellSize)
    assertEquals(resampled.sources.head.gridExtent, resampled.gridExtent)
  }

  @Test
  def sameNamedBandsWithDifferentValueTransformationsAreReadSeparately(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val scaled = new ValueOffsetRasterSource(physical, 2.0, 0.0)
    val offset = new ValueOffsetRasterSource(physical, 1.0, 5.0)
    assertEquals(scaled.name, offset.name)

    val composite = new BandCompositeRasterSource(
      NonEmptyList.of(IndexedRasterSource(scaled, 0), IndexedRasterSource(offset, 0)), crs = physical.crs)
    val bounds = GridBounds(100L, 100L, 131L, 131L)
    val result = composite.read(bounds, Seq(0, 1)).get.tile

    val expectedScaled = scaled.read(bounds, Seq(0)).get.tile.band(0)
    val expectedOffset = offset.read(bounds, Seq(0)).get.tile.band(0)
    assertTrue(expectedScaled.toArrayDouble().toSeq != expectedOffset.toArrayDouble().toSeq)
    assertEquals(expectedScaled.toArrayDouble().toSeq, result.band(0).toArrayDouble().toSeq)
    assertEquals(expectedOffset.toArrayDouble().toSeq, result.band(1).toArrayDouble().toSeq)
  }

  @Test
  def softReadFailuresPreserveSelectedBandCountAndRawCellTypeBehavior(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val bounds = GridBounds(100L, 100L, 131L, 131L)
    for (cellType <- Seq[CellType](FloatConstantNoDataCellType, UByteCellType)) {
      val failed = new ValueOffsetRasterSource(physical.convert(cellType), 1.0, 0.0) {
        override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] =
          throw new java.io.IOException("unreadable selected bands")
      }
      val composite = new BandCompositeRasterSource(
        NonEmptyList.of(BandSelectionRasterSource(failed, Seq(0, 0)), physical),
        physical.crs, parallelRead = false, softErrors = true)
      val result = composite.read(bounds)
      if (cellType == FloatConstantNoDataCellType) {
        val tile = result.get.tile
        assertEquals(3, tile.bandCount)
        assertTrue(tile.band(0).isNoDataTile && tile.band(1).isNoDataTile)
        assertEquals(physical.read(bounds).get.tile.band(0).toArrayDouble().toSeq, tile.band(2).toArrayDouble().toSeq)
      } else {
        // A raw type cannot encode an empty band: retain the existing None result instead of inventing valid zeroes.
        assertEquals(None, result)
      }
    }
  }

  @Test
  def boundsSoftReadFailuresWithoutNoDataFailInsteadOfDroppingWindows(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val failed = new ValueOffsetRasterSource(physical.convert(UByteCellType), 1.0, 0.0) {
      override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] =
        throw new IOException("unreadable raw window")
    }
    val composite = new BandCompositeRasterSource(NonEmptyList.of(failed), physical.crs,
      parallelRead = false, softErrors = true)
    val bounds = Seq(GridBounds[Long](0, 0, 31, 31), GridBounds[Long](32, 0, 63, 31))
    val error = assertThrowsJunit(classOf[IOException], () => { composite.readBounds(bounds).toList; () })
    assertTrue(error.getMessage.contains("cannot be represented as NoData"), error.getMessage)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def extentSoftReadFailuresPreserveSelectedBandsAndRawCellTypeBehavior(parallelRead: Boolean): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val bounds = GridBounds(100L, 100L, 131L, 131L)
    val extent = physical.gridExtent.extentFor(bounds)
    val expected = physical.read(extent).get
    for (cellType <- Seq[CellType](FloatConstantNoDataCellType, UByteCellType)) {
      val failed = new ValueOffsetRasterSource(physical.convert(cellType), 1.0, 0.0) {
        override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
          throw new IOException("unreadable selected bands by extent")
      }
      val composite = new BandCompositeRasterSource(
        NonEmptyList.of(BandSelectionRasterSource(failed, Seq(0, 0)), physical),
        physical.crs, parallelRead = parallelRead, softErrors = true)
      val result = composite.read(extent, Seq(1, 2, 0))
      if (cellType == FloatConstantNoDataCellType) {
        val raster = result.get
        assertEquals(expected.extent, raster.extent)
        assertEquals((expected.cols, expected.rows, 3), (raster.cols, raster.rows, raster.tile.bandCount))
        assertEquals(FloatConstantNoDataCellType, raster.tile.cellType)
        assertTrue(raster.tile.band(0).toArrayDouble().forall(_.isNaN))
        assertEquals(expected.tile.band(0).toArrayDouble().toSeq, raster.tile.band(1).toArrayDouble().toSeq)
        assertTrue(raster.tile.band(2).toArrayDouble().forall(_.isNaN))
      } else {
        assertEquals(None, result, "raw cell types must not invent valid zeroes for failed bands")
      }
    }
  }

  @Test
  def extentReadFailuresStillFailInStrictMode(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val failure = new IOException("unreadable extent")
    val failed = new ValueOffsetRasterSource(physical, 1.0, 0.0) {
      override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = throw failure
    }
    val composite = new BandCompositeRasterSource(NonEmptyList.of(physical, failed), physical.crs,
      parallelRead = false, softErrors = false)
    val extent = physical.gridExtent.extentFor(GridBounds(100L, 100L, 131L, 131L))
    val error = assertThrowsJunit(classOf[IOException], () => { composite.read(extent); () })
    assertTrue(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists(_ eq failure))
  }

  @Test
  def extentReadCancellationIsNotASoftError(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    val cancelled = AbortedException.builder().message("cancelled extent read").build()
    val failed = new ValueOffsetRasterSource(physical, 1.0, 0.0) {
      override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = throw cancelled
    }
    val extent = physical.gridExtent.extentFor(GridBounds(100L, 100L, 131L, 131L))
    for (softErrors <- Seq(false, true)) {
      val composite = new BandCompositeRasterSource(NonEmptyList.of(physical, failed), physical.crs,
        parallelRead = false, softErrors = softErrors)
      assertSame(cancelled, assertThrowsJunit(classOf[AbortedException], () => { composite.read(extent); () }))
    }
  }

  @Test
  def bandSelectionServesConsecutiveBandsInOneRead(): Unit = {
    val physical = GeoTiffRasterSource(singleBandGeotiffPath)
    var reads = 0
    val counting = new ValueOffsetRasterSource(physical, 1.0, 0.0) {
      override def read(bounds: GridBounds[Long], bands: Seq[Int]) = { reads += 1; super.read(bounds, bands) }
    }
    val placeholder = NoDataRasterSource.instance(physical.gridExtent, physical.crs)
    val composite = new BandCompositeRasterSource(
      NonEmptyList.of(BandSelectionRasterSource(counting, Seq(0, 0)), placeholder),
      crs = physical.crs, parallelRead = false)
    assertEquals(3, composite.bandCount)

    val bounds = GridBounds(100L, 100L, 131L, 131L)
    val result = composite.read(bounds, Seq(2, 1, 0)).get.tile
    assertEquals(1, reads)
    assertEquals(3, result.bandCount)
    val expected = physical.read(bounds, Seq(0)).get.tile.band(0).toArrayDouble().toSeq
    assertTrue(result.band(0).isNoDataTile)
    assertEquals(expected, result.band(1).toArrayDouble().toSeq)
    assertEquals(expected, result.band(2).toArrayDouble().toSeq)

    val groups = composite.groupedBySource.map { case (name, outputBands, groupSources) => (name, outputBands, groupSources.size) }
    assertEquals(Seq((physical.name, Seq(0, 1), 1), (placeholder.name, Seq(2), 1)), groups)
  }
}
