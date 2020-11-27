package org.openeo.geotrellis.water_vapor

import geotrellis.raster.MultibandTile
import geotrellis.raster.TileLayout
import org.openeo.geotrellis.icor.LookupTable
import geotrellis.raster.DoubleRawArrayTile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.Tile
import org.geotools.feature.visitor.AverageVisitor.AverageResult
import geotrellis.raster.resample.Average

class BlockProcessor {
  
    def computeDoubleBlocks(mbt: MultibandTile, blockSize: Int, aot: Double, ozone: Double, nodata: Double, lut: LookupTable, f: WaterVaporCalculator): Tile = {
      val ncols=(mbt.band(0).cols.doubleValue()/blockSize.doubleValue()).ceil.intValue()
      val nrows=(mbt.band(0).rows.doubleValue()/blockSize.doubleValue()).ceil.intValue()
      
      val tileLayout= new TileLayout(ncols,nrows,blockSize,blockSize)
      val smbt=mbt.split(tileLayout)
      
      val result_array=smbt.map( multibandTile => {
          var blockValue: Double= nodata
          multibandTile.foreachDouble(pixelArray =>{
            if ((blockValue == nodata)||(nodata.isNaN()&&(blockValue!=blockValue))) // because NaN==NaN is false  
              blockValue=f.computePixel(
                lut,
              	pixelArray(0),
              	pixelArray(1),
              	pixelArray(2),
              	pixelArray(3),
              	aot,
              	pixelArray(4),
              	pixelArray(5),
              	pixelArray(6),
              	ozone,
              	nodata
              )
          })
          blockValue
      }).toArray

      DoubleRawArrayTile(result_array,ncols,nrows)
        .resample(blockSize*ncols, blockSize*nrows, NearestNeighbor)
        .crop(mbt.band(0).cols, mbt.band(0).rows)
        .withNoData(Option(nodata))

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