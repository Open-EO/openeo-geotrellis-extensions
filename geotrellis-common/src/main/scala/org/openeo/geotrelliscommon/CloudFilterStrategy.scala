package org.openeo.geotrelliscommon

import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{BitCellType, DoubleConstantNoDataCellType, MultibandTile, NODATA, Raster, Tile}
import org.openeo.geotrelliscommon.SCLConvolutionFilterStrategy._

import java.util

trait CloudFilterStrategy extends Serializable {
  def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile]
}

trait MaskTileLoader {
  def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] // TODO: Option[MultibandTile] or even Option[Tile] instead? It's a single band after all.
  def loadData: Option[MultibandTile]
}

class L1CCloudFilterStrategy(val bufferInMeters: Int) extends CloudFilterStrategy {
  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = maskTileLoader.loadData
}

/**
 * Does no cloud filtering and returns the original data.
 */
object NoCloudFilterStrategy extends CloudFilterStrategy {
  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = maskTileLoader.loadData
}

object SCLConvolutionFilterStrategy{

  private val defaultMask1 = util.Arrays.asList(2, 4, 5, 6, 7)
  private val defaultMask2 = util.Arrays.asList(3,8,9,10,11)

  val DEFAULT_EROSION_KERNEL = 0
  val DEFAULT_KERNEL1 = 17
  val DEFAULT_KERNEL2 = 201

  def defaultMaskingParams: util.HashMap[String, Object] = {
    val map = new util.HashMap[String,Object]()
    map.put("mask1_values",defaultMask1)
    map.put("mask2_values",defaultMask2)

    map.put("kernel1_size",DEFAULT_KERNEL1.asInstanceOf[Object])
    map.put("kernel2_size",DEFAULT_KERNEL2.asInstanceOf[Object])
    map
  }
}

/**
 * Applies 2D convolution to extend the sen2cor sceneclassification for a more eager masking.
 *
 * @param sclBandIndex
 */
class SCLConvolutionFilterStrategy(val sclBandIndex: Int = 0,val maskingParams:util.Map[String, Object] = defaultMaskingParams) extends CloudFilterStrategy {

  val amplitude = 10000.0

  private def kernel(windowSize: Int): Option[Tile] = {
    if(windowSize<=0) {
      None
    }else {
      val k = Kernel.gaussian(windowSize, windowSize / 6.0, amplitude)
      Some(k.tile.convert(DoubleConstantNoDataCellType).localDivide(k.tile.toArray().sum))
    }
  }

  private def erosion_kernel(windowSize: Int): Option[Tile] = {
    if(windowSize<=0) {
      None
    }else{
      val k = Kernel.circle(windowSize,0,windowSize/2)
      Some(k.tile)
    }
  }

  private val erosionKernel = erosion_kernel(maskingParams.getOrDefault("erosion_kernel_size",DEFAULT_EROSION_KERNEL.asInstanceOf[Object]).asInstanceOf[Int])
  private val kernel1 = kernel(maskingParams.getOrDefault("kernel1_size",DEFAULT_KERNEL1.asInstanceOf[Object]).asInstanceOf[Int])
  private val kernel2 = kernel(maskingParams.getOrDefault("kernel2_size",DEFAULT_KERNEL2.asInstanceOf[Object]).asInstanceOf[Int])

  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = {
    val bufferSize = (kernel2.get.cols/2).floor.intValue()
    val cloudRaster = maskTileLoader.loadMask(bufferInPixels = bufferSize, sclBandIndex)

    if (cloudRaster.isDefined) {
      val maskTile = cloudRaster.get.tile.band(0)

      var allMasked = true
      var nothingMasked = true
      val mask1Values = maskingParams.getOrDefault("mask1_values",defaultMask1).asInstanceOf[util.List[Int]]
      val binaryMask = maskTile.map(value => {
        if (mask1Values.contains(value)) {
          allMasked = false
          0
        } else {
          nothingMasked = false
          1
        }
      })
      if (!allMasked ) {

        /**
         * 0: nodata
         * 1: saturated
         * 2: dark area or cast shadows??
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

        val tileSize = binaryMask.cols - 2*bufferSize

        val convolution1 =
        if(!nothingMasked && kernel1.isDefined) {
          val eroded = erode(binaryMask)

          //maskTile.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("mask.png")
          //binaryMask.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask1.png")
          val convolved = FFTConvolve(eroded, kernel1.get)
          //first dilate, with a small kernel around everything that is not valid
          allMasked = true
          Some(convolved.crop(binaryMask.cols - (tileSize + bufferSize), binaryMask.rows - (tileSize + bufferSize), binaryMask.cols - (bufferSize+1), binaryMask.rows - (bufferSize+1)).localIf({ d: Double => {
            val res = d > 0.057
            if (!res) {
              allMasked = false
            }
            res
          }
          }, 1.0, 0.0))
        }else{
          if(nothingMasked){
            None
          }else{
            Some(binaryMask.crop(binaryMask.cols - (tileSize + bufferSize), binaryMask.rows - (tileSize + bufferSize), binaryMask.cols - (bufferSize+1), binaryMask.rows - (bufferSize+1))) //kernel size is 0, but there is still a basic binary mask
          }

        }


        if (!allMasked) {
          val mask2Values = maskingParams.getOrDefault("mask2_values",defaultMask2).asInstanceOf[util.List[Int]]
          //convolution1.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv1.png")
          allMasked = true
          val binaryMask2 = maskTile.map(value => {
            if (mask2Values.contains(value)) {
              1
            } else {
              allMasked = false
              0
            }
          })

          val mask2 = if(!allMasked){
            val eroded2 = erode(binaryMask2)
            //binaryMask2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("bmask2.png")
            val convolution2 = FFTConvolve(eroded2, kernel2.get).crop(binaryMask2.cols - (tileSize + bufferSize), binaryMask2.rows - (tileSize + bufferSize), binaryMask2.cols - (bufferSize+1), binaryMask2.rows - (bufferSize+1))
            convolution2.localIf({ d: Double => d > 0.025 }, 1.0, 0.0)
          } else{
            binaryMask2.crop(binaryMask2.cols - (tileSize + bufferSize), binaryMask2.rows - (tileSize + bufferSize), binaryMask2.cols - (bufferSize+1), binaryMask2.rows - (bufferSize+1))
          }
          //convolution2.convert(UByteConstantNoDataCellType).renderPng(ColorMaps.IGBP).write("conv2.png")
          //Use bit celltype because of: https://github.com/locationtech/geotrellis/issues/3488
          val fullMask = convolution1.map(_.localOr(mask2)).getOrElse(mask2).convert(BitCellType)

          allMasked = !fullMask.toArray().contains(0)

          if (allMasked) None
          else maskTileLoader.loadData.map(_.mapBands((_, tile) => tile.localMask(fullMask, 1, NODATA)))
        } else None
      } else if(nothingMasked){
        maskTileLoader.loadData
      }else None
    } else maskTileLoader.loadData
  }

  private def erode(binaryMask2: Tile) = {
    if (erosionKernel.isDefined) {
      val maskInvert = binaryMask2.localSubtract(1).localPow(2)
      val eroded = FFTConvolve(maskInvert, erosionKernel.get)
      val erodedInvert = eroded.localIf({ d: Double => d > 0.5 }, 0.0, 1.0)
      erodedInvert
    } else {
      binaryMask2
    }
  }
}
