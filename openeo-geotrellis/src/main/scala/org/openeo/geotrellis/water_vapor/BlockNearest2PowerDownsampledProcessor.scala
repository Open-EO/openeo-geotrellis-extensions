package org.openeo.geotrellis.water_vapor

import geotrellis.raster.MultibandTile
import geotrellis.raster.TileLayout
import org.openeo.geotrellis.icor.LookupTable
import geotrellis.raster.DoubleRawArrayTile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.Tile
import org.geotools.feature.visitor.AverageVisitor.AverageResult
import geotrellis.raster.resample.Average
import geotrellis.raster.resample.Bilinear

class BlockNearest2PowerDownsampledProcessor {
  
    def computeDoubleBlocks(mbt: MultibandTile, blockSize: Int, aot: Double, ozone: Double, nodata: Double, lut: LookupTable, f: WaterVaporCalculator): Tile = {
      // because blocks are mostly power of 2 squares, finding the nearest scale down which is still higher 
      // the assumption is that Bilinear resampling balances the error of the undersampling 
      //val alignedBlockSize=Math.pow(2.0,(Math.log10(blockSize)/Math.log10(2.0)).ceil)
      
      //val ncols=(mbt.band(0).cols.doubleValue()/alignedBlockSize.doubleValue()).round.intValue()
      //val nrows=(mbt.band(0).rows.doubleValue()/alignedBlockSize.doubleValue()).round.intValue()
      //val downsampledmbt=mbt.resample(ncols, nrows, Average).withNoData(Double)
      val downsampledmbt=mbt

      val result_tile=downsampledmbt.combineDouble(0, 1, 2, 3, 4, 5, 6) { 
        (sza, vza, raa, dem, cwv, r0, r1) => 
          f.computePixel(
            lut,
          	sza,
          	vza,
          	raa,
          	dem,
          	aot,
          	cwv,
          	r0,
          	r1,
          	ozone,
          	nodata
          )
      }
      
      // taking 2*6 blocks (6->10m to 60m reduce)
      result_tile
        .resample((mbt.band(0).cols/12).intValue(), (mbt.band(0).rows/12).intValue(), Average)
        .resample(mbt.band(0).cols, mbt.band(0).rows, NearestNeighbor)
        .crop(mbt.band(0).cols, mbt.band(0).rows)
        .withNoData(Option(nodata))

//      result_tile
//        .resample(mbt.band(0).cols, mbt.band(0).rows, Bilinear)
//        .crop(mbt.band(0).cols, mbt.band(0).rows)
//        .withNoData(Option(nodata))
  }

  def replaceNoDataWithAverage(t: Tile, nodata: Double): Tile = {
    
    var sum: Double = 0.0
    var count: Double = 0
    
    t.foreachDouble( v => {
      if ((v!=nodata)&&(!v.isNaN())) {
        sum+=v;
        count+=1;
      }
    })
    
    // TODO: fix with global average !!!
    var average=0.0;
    if (count > 0.0) average=sum/count 
    
    t.mapDouble( v => {
      if ((v==nodata)||(v.isNaN())) average else v
    })
  }
    
}