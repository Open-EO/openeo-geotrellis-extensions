package org.openeo.geotrellis.water_vapor

import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, doubleNODATA, Tile}
import geotrellis.layer.SpaceTimeKey
import org.openeo.geotrellis.icor.CorrectionDescriptor
import org.apache.spark.broadcast.Broadcast
import geotrellis.raster.FloatConstantNoDataCellType
import org.openeo.geotrellis.icor.LookupTable

class CWVProvider {

  def compute(
    multibandtile: (SpaceTimeKey, MultibandTile), // where is wv/r0/r1
    szaTile: Tile, 
    vzaTile: Tile, 
    raaTile: Tile,
    elevationTile: Tile,
    aot: Double,
    ozone: Double,
    preMult: Double,
    postMult: Double,
    bcLUT: Broadcast[LookupTable],
    bandIds:java.util.List[String],
    cd: CorrectionDescriptor
  ) : Tile = {

    val wvBandId="B09"
    val r0BandId="B8A"
    val r1BandId="B11"
    
    //val bp = new FirstInBlockProcessor()
    val bp = new DoubleDownsampledBlockProcessor()
    val wvCalc = new AbdaWaterVaporCalculator()
    wvCalc.prepare(bcLUT.value,cd,wvBandId,r0BandId,r1BandId)
            
    // TODO: use reflToRad(double src, double sza, ZonedDateTime time, int bandToConvert)
    val wvTile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,wvBandId)).convert(FloatConstantNoDataCellType)*preMult
    val r0Tile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,r0BandId)).convert(FloatConstantNoDataCellType)*preMult
    val r1Tile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,r1BandId)).convert(FloatConstantNoDataCellType)*preMult
                        
    // try to get at least 1 valid value on 60m resolution
    val mbtresult : Tile = try { 
      val wvRawResultTile=bp.computeDoubleBlocks(
        MultibandTile(
          szaTile, 
          vzaTile, 
          raaTile,
          elevationTile.convert(FloatConstantNoDataCellType), 
    // AOT overriden                      aotTile.convert(FloatConstantNoDataCellType), 
          wvTile.convert(FloatConstantNoDataCellType), 
          r0Tile.convert(FloatConstantNoDataCellType), 
          r1Tile.convert(FloatConstantNoDataCellType)
        ),
        6, // on 10m base resolution looking for a value on 60m resolution
        aot,
        ozone,
        doubleNODATA,
        bcLUT.value,
        wvCalc
      )*postMult
      bp.replaceNoDataWithAverage(wvRawResultTile,doubleNODATA)
    } catch {
      case e: IllegalArgumentException => wvTile
    }
    
    mbtresult.convert(FloatConstantNoDataCellType)
   
 }
  
  
}