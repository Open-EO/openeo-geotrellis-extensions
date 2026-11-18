package org.openeo.sar.dem

import cats.data.NonEmptyList
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, GridExtent, MosaicRasterSource, RasterExtent, RasterSource, TargetRegion, Tile}
import org.openeo.sar.TileRequest

import java.util.concurrent.TimeUnit

/** Reads the DEM heights for one output tile from the (few) [[DemItem]]s that overlap it. */
object DemReader {

  // A margin for the bilinear resampling at the border of the tile.
  private val TileBufferDegrees = 0.02

  private val sourceCache: Cache[String, RasterSource] = Caffeine.newBuilder()
    .maximumSize(64)
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .build()

  /** Heights for the tile described by `req`/`targetRe`, in metres, NaN where unknown.
   *
   *  - no item overlaps the tile (e.g. the sea): 0 m;
   *  - nodata within the items: 0 m;
   *  - items overlap but cannot be read: the exception is propagated, so the caller can apply its error policy.
   *
   *  Items that overlap each other or differ in resolution are mosaicked at the finest resolution (bilinear). */
  def read(items: Seq[DemItem], req: TileRequest, targetRe: RasterExtent): Tile = {
    val tileWgs84 = req.extent.reproject(req.crs, LatLng)
    val heights = DoubleArrayTile.fill(Double.NaN, req.cols, req.rows)

    // Tiles at the antimeridian are read per side, as the sources on either side are ~360° apart in LatLng.
    for {
      part <- DemGeometry.splitAtAntimeridian(tileWgs84, TileBufferDegrees)
      overlapping = items.filter(_.bboxWgs84.intersects(part))
      if overlapping.nonEmpty
      raster <- readItems(overlapping, req.crs, targetRe)
    } heights.merge(req.extent, raster.extent, raster.tile.band(0))

    heights.mapDouble(height => if (java.lang.Double.isNaN(height)) 0.0 else height)
  }

  private def readItems(items: Seq[DemItem], crs: CRS, targetRe: RasterExtent) = {
    val sources = items.map(item => sourceCache.get(item.href, GeoTiffRasterSource(_): RasterSource))
    val finest = sources.map(_.cellSize).minBy(cellSize => cellSize.width * cellSize.height)

    val sameResolution = sources.map { source =>
      if (source.cellSize == finest) source else source.resampleToGrid(GridExtent[Long](source.extent, finest), Bilinear)
    }

    val source: RasterSource = sameResolution match {
      case Seq(single) => single
      case multiple => MosaicRasterSource(NonEmptyList.fromListUnsafe(multiple.toList), LatLng)
    }

    source
      .reproject(crs, TargetRegion(targetRe.toGridType[Long]), Bilinear)
      .read()
  }
}
