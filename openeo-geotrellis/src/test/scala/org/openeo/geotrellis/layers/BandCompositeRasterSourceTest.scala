package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.LatLng
import geotrellis.raster.{GridBounds, MultibandTile, Raster}
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class BandCompositeRasterSourceTest extends RasterMatchers {

  // extent: 125.8323451450973920, -26.4635378273783921,
  //         128.0585356212979775, -24.4605616369025221
  private val singleBandGeotiffPath =
    Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/cgls_ndvi300.tif").getPath

  @Test
  def readManyBounds(): Unit = {

    val warpOptions = GDALWarpOptions(alignTargetPixels = true)
    val rs = GDALRasterSource("/vsicurl/https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/Sentinel-2/MSI/L1C/2021/01/01/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401/S2B_MSIL1C_20210101T184759_N0209_R070_T11TNM_20210101T202401.SAFE/GRANULE/L1C_T11TNM_A019973_20210101T184756/IMG_DATA/T11TNM_20210101T184759_B02.jp2",warpOptions)

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
}
