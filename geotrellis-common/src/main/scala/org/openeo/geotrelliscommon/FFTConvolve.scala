package org.openeo.geotrelliscommon

import geotrellis.raster.mapalgebra.focal.Kernel
import geotrellis.raster.{DoubleArrayTile, PaddedTile, Tile}
import org.jtransforms.fft.DoubleFFT_2D
import spire.syntax.cfor._

object FFTConvolve {

  private val fft_instD2D = new ThreadLocal[(Int, Int, DoubleFFT_2D)]

  private def getD2DInstance(rows: Int, columns: Int): DoubleFFT_2D = {
    val inst = fft_instD2D.get
    if (inst != null && rows == inst._1 && columns == inst._2) inst._3
    else {
      fft_instD2D.set((rows, columns, new DoubleFFT_2D(rows, columns)))
      fft_instD2D.get()._3
    }
  }

  private def tempToTile(tempArr: Array[Double], rows: Int, cols: Int): Tile = {
    val tempRet = DoubleArrayTile.ofDim(cols, rows)
    for (r <- 0 until rows; c <- 0 until cols) {
      val ind = r * 2 * cols + 2 * c
      tempRet.setDouble(c, r, tempArr(ind))
    }
    tempRet
  }

  private def denseMatrixDToTemp(tempDM: Tile): Array[Double] = {
    val tempCols = tempDM.cols
    val tempRet = new Array[Double](tempDM.rows * tempCols * 2)
    for (r <- 0 until tempDM.rows; c <- 0 until tempDM.cols) {
      val d = tempDM.getDouble(c, r)
      if (d.isNaN)
        tempRet(r * 2 * tempCols + 2 * c) = 0.0
      else
        tempRet(r * 2 * tempCols + 2 * c) = d
    }
    tempRet
  }

  def fftConvolve(tile: Tile, tile2: Tile): Tile = {
    //reformat for input
    val tempMat1: Array[Double] = denseMatrixDToTemp(tile)
    val tempMat2: Array[Double] = denseMatrixDToTemp(tile2)

    //actual action
    val fft_instance = getD2DInstance(tile.rows, tile.cols)
    fft_instance.complexForward(tempMat1) //does operation in place
    fft_instance.complexForward(tempMat2)
    //ToDo this could be optimized to use realFullForward for speed, but only if the indexes are powers of two
    cfor(0)(_ < tile.rows, _ + 1) { r =>
      cfor(0)(_ < tile.cols, _ + 1) { c =>
        val r1 = tempMat1(r * 2 * tile.cols + 2 * c)
        val c1 = tempMat1(r * 2 * tile.cols + 2 * c + 1)
        val r2 = tempMat2(r * 2 * tile.cols + 2 * c)
        val c2 = tempMat2(r * 2 * tile.cols + 2 * c + 1)
        val realPart = r1 * r2 - c1 * c2
        val complexPart = r1 * c2 + c1 * r2
        tempMat1(r * 2 * tile.cols + 2 * c) = realPart
        tempMat1(r * 2 * tile.cols + 2 * c + 1) = complexPart
      }
    }
    fft_instance.complexInverse(tempMat1, true)
    return tempToTile(tempMat1, tile.rows, tile.cols)


  }

  def apply(tile: Tile, kernel: Kernel): Tile = {

    val paddedCols = tile.cols + kernel.tile.cols - 1
    val paddedRows = tile.rows + kernel.tile.rows - 1
    val paddedKernel = PaddedTile(kernel.tile, 0, 0, paddedCols, paddedRows)
    val paddedInput = PaddedTile(tile, 0, 0, paddedCols, paddedRows)

    val paddedOutput = fftConvolve(paddedInput, paddedKernel)

    val colOffset = (paddedCols - tile.cols) / 2
    val rowOffset = (paddedRows - tile.rows) / 2
    return paddedOutput.crop(colOffset, rowOffset, colOffset + tile.cols - 1, rowOffset + tile.rows - 1)
  }
}
