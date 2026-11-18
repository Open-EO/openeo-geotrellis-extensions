package org.openeo.sar.dem

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{DoubleArrayTile, RasterExtent, RasterSource, TargetRegion, Tile}
import org.openeo.sar.TileRequest
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/** Reads the DEM heights for one output tile from the (few) [[DemItem]]s that overlap it. */
object DemReader {
  private val logger = LoggerFactory.getLogger(DemReader.getClass)

  // A margin for the bilinear resampling at the border of the tile.
  private val TileBufferDegrees = 0.02

  private val sourceCache: Cache[String, RasterSource] = Caffeine.newBuilder()
    .maximumSize(64)
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .build()

  /** Heights for the tile described by `req`/`targetRe`, in metres (0 where unknown).
   *
   *  - no item overlaps the tile (e.g. the sea): 0 m;
   *  - nodata within the items: 0 m;
   *  - items overlap but cannot be read: the exception is propagated, so the caller can apply its error policy.
   *
   *  Each item is reprojected (bilinear) to the target grid, so items may have any CRS. Where items overlap,
   *  the one with the finest resolution wins. */
  def read(items: Seq[DemItem], req: TileRequest, targetRe: RasterExtent): Tile = {
    val tileWgs84 = req.extent.reproject(req.crs, LatLng)
    val heights = DoubleArrayTile.fill(Double.NaN, req.cols, req.rows)

    // Tiles at the antimeridian are searched per side, as items on either side are ~360° apart in LatLng.
    val overlapping = DemGeometry.splitAtAntimeridian(tileWgs84, TileBufferDegrees)
      .flatMap(part => items.filter(_.bboxWgs84.intersects(part)))
      .distinct

    if (overlapping.isEmpty) logger.debug(s"No DEM item overlaps tile ${req.extent} ($tileWgs84): using 0 m")

    val sources = overlapping.map(item => sourceCache.get(item.href, GeoTiffRasterSource(_): RasterSource))
    val ordered =
      if (sources.map(_.crs).distinct.size == 1) sources.sortBy(s => s.cellSize.width * s.cellSize.height)
      else sources

    // merge only fills cells that are still NaN
    for {
      source <- ordered
      raster <- source.reproject(req.crs, TargetRegion(targetRe.toGridType[Long]), Bilinear).read()
    } heights.merge(req.extent, raster.extent, raster.tile.band(0))

    heights.mapDouble(height => if (java.lang.Double.isNaN(height)) 0.0 else height)
  }
}
