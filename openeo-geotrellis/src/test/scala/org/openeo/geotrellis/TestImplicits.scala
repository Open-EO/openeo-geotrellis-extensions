package org.openeo.geotrellis

import geotrellis.layer.SpatialKey
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, SinglebandGeoTiff, Tags}
import geotrellis.spark._
import geotrellis.vector.ProjectedExtent

import java.util.zip.Deflater.BEST_COMPRESSION

object TestImplicits {
  implicit class TileGeoTiffOutputMethods(spatialLayer: TileLayerRDD[SpatialKey]) {
    def writeGeoTiff(path: String, bbox: ProjectedExtent = null): Unit = {
      val Raster(tile, extent) =
        (if (bbox != null) spatialLayer.crop(bbox.reproject(spatialLayer.metadata.crs)) else spatialLayer).stitch()

      val options = GeoTiffOptions(DeflateCompression(BEST_COMPRESSION))

      SinglebandGeoTiff(tile, extent, spatialLayer.metadata.crs, Tags.empty, options)
        .write(path)
    }
  }

  implicit class MultibandTileGeoTiffOutputMethods(spatialLayer: MultibandTileLayerRDD[SpatialKey]) {
    def writeGeoTiff(path: String, bbox: ProjectedExtent = null): Unit = {
      val maybeBBox = Option(bbox).map(_.reproject(spatialLayer.metadata.crs))
      org.openeo.geotrellis.geotiff.saveRDD(spatialLayer,-1,path,6,maybeBBox)

    }
  }
}
