package org.openeo.geotrellis.icor

import java.nio.file.Paths

import scala.collection.mutable.ListBuffer

import geotrellis.layer.LayoutDefinition
import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.CRS
import geotrellis.raster.FloatConstantNoDataCellType
import geotrellis.raster.Tile
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.vector.Extent

object SRTMProvider{
  
  def findFiles(basePath:String, tileExtent: Extent, tileCRS:CRS) : List[String] = {
    val projExtent=tileExtent.reproject(tileCRS, CRS.fromName("EPSG:4326"))
    var fileList=ListBuffer[String]()
    for( x <- projExtent.xmin.floor.toInt to projExtent.xmax.floor.toInt; y <- projExtent.ymin.floor.toInt to projExtent.ymax.floor.toInt ) {
      var ifile:String=""
      ifile += (if (y<0) "S%02d".format(-y-1) else "N%02d".format(y))
      ifile += (if (x<0) "W%03d".format(-x-1) else "E%03d".format(x))
      // TODO: look into: seems like a version mismatch, oldschool addressing is required, while even version 2.3.2 should work without addressing inside the zip
      // https://gdal.org/drivers/raster/srtmhgt.html
      //ifile += ".SRTMGL1.hgt.zip"//".hgt" //".SRTMGL1.hgt.zip"
      fileList+= Paths.get(basePath,ifile.toString()+".SRTMGL1.hgt.zip",ifile.toString()+".hgt").toString()
    }
    fileList.toList
  }
  
}

class SRTMProvider(basePath:String = "/eodata/auxdata/SRTMGL1/dem") extends ElevationProvider {

  import SRTMProvider._

  def compute(key:SpaceTimeKey, targetCRS:CRS,layoutDefinition:LayoutDefinition): Tile = {

    // find the files that overlap with extent of the key
    val targetExtent= layoutDefinition.mapTransform.apply(key)
    val files = findFiles(basePath, targetExtent, targetCRS)

    // load the files (this is a lazy load according to the docs)
    val rasters=files.map(
      GDALRasterSource(_).reprojectToRegion(targetCRS,layoutDefinition.toRasterExtent()).read(targetExtent)
    ).filter(i => i.nonEmpty).map(_.get)

    // merge portions coming from the different files
    val mergedRasters=rasters.reduceLeft(
        (a,b) => a.merge(b)
    )

    // convert to km and extract the tile
    val mergedTile=mergedRasters.tile.band(0).convert(FloatConstantNoDataCellType).localMultiply(0.001)
    
//    // write reference test data - don't forget to re-comment when not needed
//    MultibandGeoTiff( MultibandTile(mergedTile), targetExtent, targetCRS).write("test_srtm_tile_%d_%d.tif".format(key.col,key.row))
//    println(mergedTile)

    mergedTile
  }

}
