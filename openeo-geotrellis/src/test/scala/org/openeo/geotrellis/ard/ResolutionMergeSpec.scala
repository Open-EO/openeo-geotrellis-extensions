package org.openeo.geotrellis.ard

import geotrellis.raster.io.geotiff.GeoTiff
import org.junit.{Ignore, Test}


class ResolutionMergeSpec {

  //TODO check in geotiff
  @Ignore
  @Test
  def testSentinel2Merge(): Unit ={
    val file = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/S2-bands.geotiff")
    val inputTiff = GeoTiff.readMultiband(file.getPath)
    val bands = inputTiff.tile.toArrayTile()
    val kSize = 5

    val pansharpened = Improphe.improphe(null,bands,Array(2,3),Array(4,5),3,kSize)
    val tiff = GeoTiff(pansharpened,inputTiff.extent,inputTiff.crs)
    tiff.write("/tmp/sharper.tiff",true)
  }
}
