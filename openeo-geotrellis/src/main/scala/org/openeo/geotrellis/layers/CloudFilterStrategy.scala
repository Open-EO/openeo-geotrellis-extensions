package org.openeo.geotrellis.layers

import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{MultibandTile, Raster, RasterRegion}
import org.openeo.geotrellis.FFTConvolve

abstract  class CloudFilterStrategy() extends Serializable {
  def loadMasked(rasterRegion: RasterRegion): Option[MultibandTile] = ???


}


/**
 * Applies 2D convolution to extend the sen2cor sceneclassification for a more eager masking.
 * @param sclBandIndex
 */
class SCLConvolutionFilterStrategy(val sclBandIndex:Int=0) extends CloudFilterStrategy {


  override def loadMasked(rasterRegion: RasterRegion): Option[MultibandTile] = {
    val gbRegion = rasterRegion.asInstanceOf[GridBoundsRasterRegion]
    val cloudSource = gbRegion.source.asInstanceOf[BandCompositeRasterSource].sources.toList(sclBandIndex)
    val cloudRaster = new GridBoundsRasterRegion(cloudSource,gbRegion.bounds).raster

    if(cloudRaster.isDefined) {
      val maskTile = cloudRaster.get.tile.band(0)
      var allMasked = true
      var binaryMask = maskTile.map(value => {
        if (value == 2 || value == 4 || value == 5 || value == 6 || value == 7) {
          allMasked = false
          0
        } else {
          1
        }
      })
      if (! allMasked) {
        val kernel = Kernel.circle(31, 10, 15).tile
        binaryMask = FFTConvolve(binaryMask, kernel).localIf({ d: Double => d > 0.9 }, 1.0, 0.0)
        allMasked = !binaryMask.toArray().exists(_ == 0)
      }
      if (allMasked) {
        Option.empty[MultibandTile]
      } else {
        val raster: Option[Raster[MultibandTile]] = rasterRegion.raster
        if (raster.isDefined) {
          Some(raster.get.tile.mapBands((i, t) => t.localMask(binaryMask.convert(raster.get.tile.cellType), 1, -1)))
        } else {
          raster.map(r => r.tile)
        }
      }
    }else{
      rasterRegion.raster.map(r => r.tile)
    }
  }

}
