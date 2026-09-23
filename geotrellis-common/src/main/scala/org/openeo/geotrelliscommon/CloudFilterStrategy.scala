package org.openeo.geotrelliscommon

import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{BitArrayTile, BitCellType, DoubleConstantNoDataCellType, GridBounds, MultibandTile, NODATA, Raster, ShortConstantNoDataCellType, Tile}
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

  private val erosionKernel = SCLConvolutionFilter.erosion_kernel(maskingParams.getOrDefault("erosion_kernel_size",DEFAULT_EROSION_KERNEL.asInstanceOf[Object]).asInstanceOf[Int])
  private val kernel1 = SCLConvolutionFilter.kernel(maskingParams.getOrDefault("kernel1_size",DEFAULT_KERNEL1.asInstanceOf[Object]).asInstanceOf[Int])
  private val kernel2 = SCLConvolutionFilter.kernel(maskingParams.getOrDefault("kernel2_size",DEFAULT_KERNEL2.asInstanceOf[Object]).asInstanceOf[Int])

  override def loadMasked(maskTileLoader: MaskTileLoader): Option[MultibandTile] = {
    val bufferSize = (kernel2.get.cols/2).floor.intValue()
    val cloudRaster: Option[Raster[MultibandTile]] = maskTileLoader.loadMask(bufferInPixels = bufferSize, sclBandIndex)

    if (cloudRaster.isDefined) {
      val maskTile = cloudRaster.get.tile.band(0).convert(ShortConstantNoDataCellType)

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


object SCLConvolutionFilter {
  val amplitude = 10000.0

  def kernel(windowSize: Int): Option[Tile] = {
    if (windowSize <= 0) {
      None
    } else {
      val k = Kernel.gaussian(windowSize, windowSize / 6.0, amplitude)
      Some(k.tile.convert(DoubleConstantNoDataCellType).localDivide(k.tile.toArray().sum))
    }
  }

  /**
   * The 1D Gaussian factor g such that outer(g, g) equals [[kernel]], up to rounding. Used for
   * separable convolution instead of FFTConvolve: k² multiply-adds per pixel instead of an FFT.
   */
  def kernel1D(windowSize: Int): Option[Array[Double]] = {
    if (windowSize <= 0) {
      None
    } else {
      val sigma = windowSize / 6.0
      val denom = 2.0 * sigma * sigma
      val center = windowSize / 2
      val g = Array.tabulate(windowSize) { i =>
        val d = i - center
        math.exp(-(d * d) / denom)
      }
      val sum = g.sum
      Some(g.map(_ / sum))
    }
  }

  def erosion_kernel(windowSize: Int): Option[Tile] = {
    if (windowSize <= 0) {
      None
    } else {
      val k = Kernel.circle(windowSize, 0, windowSize / 2)
      Some(k.tile)
    }
  }
}

/**
 * This class is used create a mask from SCL data.
 * @param erosion_kernal_size size of the erosion kernel
 * @param kernel1Size size of the first convolution kernel
 * @param kernel2Size size of the second convolution kernel
 * @param mask1Values SCL values to be used by the first convolution
 * @param mask2Values SCL values to be used by the second convolution
 *
 * 0: nodata
 * 1: saturated
 * 2: dark area or cast shadows??
 * 3 cloud shadow
 * 4 vegetation
 * 5 no vegetation
 * 6 water
 * 7 unclassified
 * 8 cloud medium prob
 * 9 cloud high prob
 * 10 thin cirrus
 * 11 snow
  */
class SCLConvolutionFilter(erosion_kernal_size: Int, mask1Values: util.List[Int], mask2Values: util.List[Int], kernel1Size: Int, kernel2Size: Int) extends Serializable {
  import SCLConvolutionFilter._

  private val erosionKernel = erosion_kernel(erosion_kernal_size)
  private val kernel1 = kernel(kernel1Size)
  private val kernel2 = kernel(kernel2Size)
  private val kernel1g = kernel1D(kernel1Size)
  private val kernel2g = kernel1D(kernel2Size)

  // SCL values fit comfortably in a byte; NODATA (Int.MinValue) and any other out-of-range value
  // fall through to the "not in the list" default, matching util.List#contains.
  private val TABLE_SIZE = 256
  private def lookupTable(values: util.List[Int]): Array[Boolean] = {
    val table = new Array[Boolean](TABLE_SIZE)
    val it = values.iterator()
    while (it.hasNext) {
      val v = it.next()
      if (v >= 0 && v < TABLE_SIZE) table(v) = true
    }
    table
  }
  private val mask1Table = lookupTable(mask1Values)
  private val mask2Table = lookupTable(mask2Values)
  private def inTable(table: Array[Boolean], value: Int): Boolean = value >= 0 && value < TABLE_SIZE && table(value)

  def bufferInPixels = (kernel2.get.cols/2).floor.intValue()

  def createMask(sclTile: MultibandTile): Tile =
    createMask(sclTile, GridBounds(0, 0, sclTile.cols - 1, sclTile.rows - 1))

  def createMask(sclTile: MultibandTile, targetArea: GridBounds[Int]): Tile = {
    val maskTile = sclTile.band(0).convert(ShortConstantNoDataCellType)
    val cols = maskTile.cols
    val rows = maskTile.rows
    val colMin = targetArea.colMin
    val rowMin = targetArea.rowMin
    val colMax = targetArea.colMax
    val rowMax = targetArea.rowMax
    val outCols = targetArea.width
    val outRows = targetArea.height

    var allMasked = true
    var nothingMasked = true
    val binaryMask1 = new Array[Double](cols * rows)
    var i = 0
    var r = 0
    while (r < rows) {
      var c = 0
      while (c < cols) {
        val value = maskTile.get(c, r)
        if (inTable(mask1Table, value)) {
          allMasked = false
          binaryMask1(i) = 0.0
        } else {
          nothingMasked = false
          binaryMask1(i) = 1.0
        }
        c += 1
        i += 1
      }
      r += 1
    }

    // First erosion + dilation step, restricted to the target area.
    val convolution1: Option[Array[Double]] =
      if (!nothingMasked && kernel1.isDefined) {
        val erodedArr = erode(binaryMask1, cols, rows)
        val dilated1 = SeparableConvolve.convolve(erodedArr, cols, rows, kernel1g.get, colMin, rowMin, colMax, rowMax)
        // First dilate, with a small kernel around everything that is not valid.
        allMasked = true
        var j = 0
        while (j < dilated1.length) {
          val res = dilated1(j) > 0.057
          if (res) {
            dilated1(j) = 1.0
          } else {
            dilated1(j) = 0.0
            allMasked = false
          }
          j += 1
        }
        Some(dilated1)
      } else {
        if (nothingMasked) {
          None
        } else {
          Some(sliceWindow(binaryMask1, cols, colMin, rowMin, colMax, rowMax)) //kernel size is 0, but there is still a basic binary mask
        }
      }
    if (allMasked) {
      return toBitTile(convolution1.get, outCols, outRows)
    }

    // Second erosion + dilation step, restricted to the target area.
    allMasked = true
    val binaryMask2 = new Array[Double](cols * rows)
    i = 0
    r = 0
    while (r < rows) {
      var c = 0
      while (c < cols) {
        val value = maskTile.get(c, r)
        if (inTable(mask2Table, value)) {
          binaryMask2(i) = 1.0
        } else {
          allMasked = false
          binaryMask2(i) = 0.0
        }
        c += 1
        i += 1
      }
      r += 1
    }
    val convolution2: Array[Double] = if (!allMasked) {
      val erodedArr = erode(binaryMask2, cols, rows)
      val dilated2 = SeparableConvolve.convolve(erodedArr, cols, rows, kernel2g.get, colMin, rowMin, colMax, rowMax)
      var j = 0
      while (j < dilated2.length) {
        dilated2(j) = if (dilated2(j) > 0.025) 1.0 else 0.0
        j += 1
      }
      dilated2
    } else {
      sliceWindow(binaryMask2, cols, colMin, rowMin, colMax, rowMax)
    }

    // Combine the two convolutions.
    // Use bit celltype because of: https://github.com/locationtech/geotrellis/issues/3488
    val result = convolution1 match {
      case Some(conv1) =>
        var j = 0
        while (j < conv1.length) {
          if (convolution2(j) != 0.0) conv1(j) = 1.0
          j += 1
        }
        conv1
      case None => convolution2
    }
    toBitTile(result, outCols, outRows)
  }

  private def sliceWindow(arr: Array[Double], cols: Int, colMin: Int, rowMin: Int, colMax: Int, rowMax: Int): Array[Double] = {
    val outCols = colMax - colMin + 1
    val outRows = rowMax - rowMin + 1
    val out = new Array[Double](outCols * outRows)
    var r = 0
    while (r < outRows) {
      System.arraycopy(arr, (rowMin + r) * cols + colMin, out, r * outCols, outCols)
      r += 1
    }
    out
  }

  private def toBitTile(values: Array[Double], cols: Int, rows: Int): Tile = {
    val tile = BitArrayTile.ofDim(cols, rows)
    var i = 0
    while (i < values.length) {
      tile(i) = if (values(i) != 0.0) 1 else 0
      i += 1
    }
    tile
  }

  private def erode(binaryMask: Array[Double], cols: Int, rows: Int): Array[Double] = {
    if (erosionKernel.isDefined) {
      val maskInvertTile = geotrellis.raster.DoubleArrayTile(binaryMask.map(v => (v - 1) * (v - 1)), cols, rows)
      val eroded = FFTConvolve(maskInvertTile, erosionKernel.get)
      eroded.mapDouble(d => if (d > 0.5) 0.0 else 1.0).toArrayDouble()
    } else {
      binaryMask
    }
  }
}