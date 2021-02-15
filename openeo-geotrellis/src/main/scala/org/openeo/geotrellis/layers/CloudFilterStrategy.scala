package org.openeo.geotrellis.layers

import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{MultibandTile, NODATA, Raster, RasterRegion, UByteUserDefinedNoDataCellType}
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

    val cloudRaster = new GridBoundsRasterRegion(cloudSource,gbRegion.bounds.buffer(100)).raster

    if(cloudRaster.isDefined) {
      val maskTile = cloudRaster.get.tile.band(0)
      var allMasked = true
      val binaryMask = maskTile.map(value => {
        if (value == 2 || value == 4 || value == 5 || value == 6 || value == 7) {
          allMasked = false
          0
        } else {
          1
        }
      })
      if (! allMasked) {

        /**
         *  0: nodata
         *  1: saturated
         *  2: dark area
         *  3 cloud shadow
         *  4 vegetatin
         *  5 no vegetation
         *  6 water
         *  7 unclassified
         *  8 cloud medium prob
         *  9 cloud high prob
         *  10 thin cirrus
         *  11 snow
         */

        //maskTile.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("mask.png")
        //binaryMask.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask1.png")
        val amplitude = 10000.0
        val kernel1 = Kernel.gaussian(17,17.0/3.0,amplitude).copy()
        val convolved = FFTConvolve(binaryMask, kernel1)
        //first dilate, with a small kernel around everything that is not valid
        allMasked = true
        val convolution1 = convolved.crop(binaryMask.cols-(256+100),binaryMask.rows-(256+100),binaryMask.cols-101,binaryMask.rows-101).localIf({ d: Double => {
          val res = d > amplitude* 0.057
          if(!res) {
            allMasked=false
          }
          res
        } }, 1.0, 0.0)


        if(!allMasked) {

          //convolution1.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv1.png")
          val binaryMask2 = maskTile.map(value => {
            if (value == 3 || value == 8 || value == 9 || value == 10 || value == 11) {
              1
            } else {
              0
            }
          })

          //binaryMask2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask2.png")
          val kernel2 = Kernel.gaussian(201,201.0/3.0,amplitude)
          val convolution2 = FFTConvolve(binaryMask2, kernel2).crop(binaryMask2.cols-(256+100),binaryMask2.rows-(256+100),binaryMask2.cols-101,binaryMask2.rows-101)
            .localIf({ d: Double => d > amplitude* 0.025 }, 1.0, 0.0)

          //convolution2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv2.png")
          val fullMask = convolution1.localOr(convolution2).convert(UByteUserDefinedNoDataCellType(127.byteValue()))

          allMasked = !fullMask.toArray().exists(_ == 0)
          if (allMasked) {
            Option.empty[MultibandTile]
          } else {
            val raster: Option[Raster[MultibandTile]] = rasterRegion.raster
            if (raster.isDefined) {
              Some(raster.get.tile.mapBands((i, t) => t.localMask(fullMask, 1, NODATA)))
            } else {
              raster.map(r => r.tile)
            }
          }
        }else{
          Option.empty[MultibandTile]
        }

      }else{
        Option.empty[MultibandTile]
      }

    }else{
      rasterRegion.raster.map(r => r.tile)
    }
  }

}
