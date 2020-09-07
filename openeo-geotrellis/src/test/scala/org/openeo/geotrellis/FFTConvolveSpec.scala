package org.openeo.geotrellis

import geotrellis.raster.testkit.RasterMatchers
import geotrellis.raster.{IntArrayTile, RasterExtent}
import geotrellis.vector.Extent
import org.junit.Assert._
import org.junit.Test

class FFTConvolveSpec  extends  RasterMatchers{

  def doit(in1: Array[Int], in2: Array[Int], out: Array[Int]) = {
    val size1 = math.sqrt(in1.length).toInt
    assertEquals(size1 * size1, in1.length)

    val size2 = math.sqrt(in2.length).toInt
    assertEquals(size2 * size2, in2.length)

    val e1 = Extent(0, 0, 10 * size1, 10 * size1)
    val e2 = Extent(0, 0, 10 * size2, 10 * size2)
    val re1 = RasterExtent(e1, 10, 10, size1, size1)
    val re2 = RasterExtent(e2, 10, 10, size2, size2)

    val tile1 = IntArrayTile(in1, size1, size1)
    val tile2 = IntArrayTile(in2, size2, size2)

    val tile3 = FFTConvolve(tile1,tile2)

    assertEqual(tile3, IntArrayTile(out,size1,size1),0.01)

  }

  @Test def testSimpleConvolve(): Unit = {
    //copied from convolve test in geotrellis
    /* http://www.songho.ca/dsp/convolution/convolution2d_example.html */
    doit(Array(1, 2, 3,
      4, 5, 6,
      7, 8, 9),
      Array(-1, -2, -1,
        0, 0, 0,
        1, 2, 1),
      Array(-13, -20, -17,
        -18, -24, -18,
        13, 20, 17))
  }
}
