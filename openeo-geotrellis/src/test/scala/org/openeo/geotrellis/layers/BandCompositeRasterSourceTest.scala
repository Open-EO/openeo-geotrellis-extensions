package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class BandCompositeRasterSourceTest {

  // extent: 125.8323451450973920, -26.4635378273783921,
  //         128.0585356212979775, -24.4605616369025221
  private val singleBandGeotiffPath =
    Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/cgls_ndvi300.tif").getPath

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
}
