package org.openeo.geotrellis.geotiff

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.layer.{KeyBounds, LayoutDefinition, SpaceTimeKey, TemporalKeyExtractor}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.{RasterSource, TileLayout}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.{MultibandTileLayerRDD, RasterSourceRDD}
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RasterRegionPyramidFactory(implicit  sc: SparkContext) {
  private def layer(zoom: Int, geotiff: String): MultibandTileLayerRDD[SpaceTimeKey] = {
    // all fixed
    val (layoutCols, layoutRows) = (10, 10)

    val layout = LayoutDefinition(
      Extent(xmin = 399960.000, ymin = 5390220.000, xmax = 509760.000, ymax = 5500020.000),
      TileLayout(layoutCols = layoutCols, layoutRows = layoutRows, tileCols = 10980 / layoutCols, tileRows = 10980 / layoutRows)
    )

    val date = ZonedDateTime.of(LocalDate.of(2020, 1, 19), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val bounds = KeyBounds(minKey = SpaceTimeKey(0, 0, date), maxKey = SpaceTimeKey(layoutCols - 1, layoutRows - 1, date))

    val rasterSources: RDD[RasterSource] = sc.parallelize(Seq(GeoTiffRasterSource(geotiff)))

    // spatial joins (geotrellis.spark.join.SpatialJoin) create a SpacePartitioner; use one immediately to
    // avoid a subsequent shuffle (it will only shuffle the RasterRegions and not the MultibandTiles)
    RasterSourceRDD.tiledLayerRDD(rasterSources, layout, TemporalKeyExtractor.fromPath(_ => date), partitioner = Some(SpacePartitioner(bounds)))
  }

  def data(zoom: Int): MultibandTileLayerRDD[SpaceTimeKey] = layer(zoom, "file:/data/MTDA/CGS_S2/CGS_S2_FAPAR/2020/01/19/S2B_20200119T110259Z_31UDQ_CGS_V102_000/S2B_20200119T110259Z_31UDQ_FAPAR_V102/10M/S2B_20200119T110259Z_31UDQ_FAPAR_10M_V102.tif")
  def mask(zoom: Int): MultibandTileLayerRDD[SpaceTimeKey] = layer(zoom, "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2020/01/19/S2B_20200119T110259Z_31UDQ_CGS_V102_000/S2B_20200119T110259Z_31UDQ_TOC_V102/S2B_20200119T110259Z_31UDQ_SHADOWMASK_10M_V102.tif")
}
