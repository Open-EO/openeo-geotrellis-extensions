package org.openeo.geotrelliscommon

import geotrellis.layer.{Bounds, LayoutDefinition, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, MultibandTile}
import geotrellis.spark.ContextRDD
import geotrellis.vector.Extent
import org.apache.spark.rdd._

class TileLayerMetadataOpenEO[K](
                                  override val cellType: CellType,
                                  override val layout: LayoutDefinition,
                                  override val extent: Extent,
                                  override val crs: CRS,
                                  override val bounds: Bounds[K],
                                  bandsArg: Seq[String],
                                ) extends TileLayerMetadata[K](cellType, layout, extent, crs, bounds) {
  val bands: Seq[String] = bandsArg
}

object TileLayerMetadataOpenEO {
  def apply[K](metadata: TileLayerMetadata[K], openEOMetadata: OpenEORasterCubeMetadata): TileLayerMetadataOpenEO[K] = {
    metadata match {
      case m: TileLayerMetadataOpenEO[K] =>
        assert(m.bands == openEOMetadata.bands, "Bands in metadata should match.")
        m
      case _ =>
        new TileLayerMetadataOpenEO(
          metadata.cellType,
          metadata.layout,
          metadata.extent,
          metadata.crs,
          metadata.bounds,
          openEOMetadata.bands
        )
    }
  }
}

class OpenEORasterCube[K](rdd: RDD[(K, MultibandTile)], metadata: TileLayerMetadata[K], val openEOMetadata: OpenEORasterCubeMetadata) extends ContextRDD[K, MultibandTile, TileLayerMetadata[K]](rdd, TileLayerMetadataOpenEO(metadata, openEOMetadata)) {


}
