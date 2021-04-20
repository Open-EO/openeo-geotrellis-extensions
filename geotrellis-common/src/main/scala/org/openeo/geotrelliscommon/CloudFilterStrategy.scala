package org.openeo.geotrelliscommon

import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{DoubleConstantNoDataCellType, MultibandTile, NODATA, Raster, Tile, UByteUserDefinedNoDataCellType}

trait CloudFilterStrategy extends Serializable {
  def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile]
}

trait MaskTileLoader {
  def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] // TODO: Option[MultibandTile] or even Option[Tile] instead? It's a single band after all.
  def loadData: Option[MultibandTile]
}

/**
 * Does no cloud filtering and returns the original data.
 */
object NoCloudFilterStrategy extends CloudFilterStrategy {
  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = maskTileLoader.loadData
}

/**
 * Applies 2D convolution to extend the sen2cor sceneclassification for a more eager masking.
 *
 * @param sclBandIndex
 */
class SCLConvolutionFilterStrategy(val sclBandIndex: Int = 0) extends CloudFilterStrategy {

  val amplitude = 10000.0

  private def kernel(windowSize: Int): Tile = {
    val k = Kernel.gaussian(windowSize, windowSize / 6.0, amplitude)
    k.tile.convert(DoubleConstantNoDataCellType).localDivide(k.tile.toArray().sum)
  }

  private val kernel1 = kernel(17)
  private val kernel2 = kernel(201)

  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = {
    val cloudRaster = maskTileLoader.loadMask(bufferInPixels = 100, sclBandIndex)

    if (cloudRaster.isDefined) {
      val maskTile = cloudRaster.get.tile.band(0)

      // TODO: make this more flexible
      require(maskTile.cols == 456 && maskTile.rows == 456)

      var allMasked = true
      val binaryMask = maskTile.map(value => {
        if (value == 2 || value == 4 || value == 5 || value == 6 || value == 7) {
          allMasked = false
          0
        } else {
          1
        }
      })
      if (!allMasked) {

        /**
         * 0: nodata
         * 1: saturated
         * 2: dark area
         * 3 cloud shadow
         * 4 vegetatin
         * 5 no vegetation
         * 6 water
         * 7 unclassified
         * 8 cloud medium prob
         * 9 cloud high prob
         * 10 thin cirrus
         * 11 snow
         */

        //maskTile.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("mask.png")
        //binaryMask.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask1.png")
        val convolved = FFTConvolve(binaryMask, kernel1)
        //first dilate, with a small kernel around everything that is not valid
        allMasked = true
        val convolution1 = convolved.crop(binaryMask.cols - (256 + 100), binaryMask.rows - (256 + 100), binaryMask.cols - 101, binaryMask.rows - 101).localIf({ d: Double => {
          val res = d > 0.057
          if (!res) {
            allMasked = false
          }
          res
        }
        }, 1.0, 0.0)


        if (!allMasked) {

          //convolution1.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv1.png")
          val binaryMask2 = maskTile.map(value => {
            if (value == 3 || value == 8 || value == 9 || value == 10 || value == 11) {
              1
            } else {
              0
            }
          })

          //binaryMask2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask2.png")
          val convolution2 = FFTConvolve(binaryMask2, kernel2).crop(binaryMask2.cols - (256 + 100), binaryMask2.rows - (256 + 100), binaryMask2.cols - 101, binaryMask2.rows - 101)
          val mask2 = convolution2.localIf({ d: Double => d > 0.025 }, 1.0, 0.0)
          //convolution2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv2.png")
          val fullMask = convolution1.localOr(mask2).convert(UByteUserDefinedNoDataCellType(127.byteValue()))

          allMasked = !fullMask.toArray().contains(0)

          if (allMasked) None
          else maskTileLoader.loadData.map(_.mapBands((_, tile) => tile.localMask(fullMask, 1, NODATA)))
        } else None
      } else None
    } else maskTileLoader.loadData
  }
}
