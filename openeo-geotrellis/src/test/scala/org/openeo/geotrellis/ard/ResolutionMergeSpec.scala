package org.openeo.geotrellis.ard

import geotrellis.raster.FloatArrayTile
import geotrellis.raster.io.geotiff.GeoTiff
import org.apache.commons.math3.util.FastMath
import org.junit.{Ignore, Test}
import spire.implicits.cfor


class ResolutionMergeSpec {

  def distance_kernel(nk:Int): FloatArrayTile ={

    val kernel = FloatArrayTile.ofDim(nk,nk)
    val h = (nk-1)/2
    // pre-compute kernel distance
    var ki: Int = -1
    cfor(-h)(_ <= h, _ + 1)(ii => {
      ki += 1
      var kj: Int = -1
      cfor(-h)(_ <= h, _ + 1)(jj => {
        kj += 1
        kernel.setDouble(ki,kj,FastMath.sqrt(ii*ii+jj*jj))

      })
    })
    return kernel
  }

  //TODO check in geotiff
  @Ignore
  @Test
  def testSentinel2Merge(): Unit ={
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.geotiff")
    val inputTiff = GeoTiff.readMultiband(file.getPath)
    val bands = inputTiff.tile.toArrayTile()
    val kSize = 5
    val h = (kSize - 1) / 2
    val kernel = distance_kernel(kSize)
    val pansharpened = Improphe.improphe(null,bands,kernel,h,2,2,Array(2,3),Array(4,5),kSize*kSize,3)
    val tiff = GeoTiff(pansharpened,inputTiff.extent,inputTiff.crs)
    tiff.write("/tmp/sharper.tiff",true)
  }
}
