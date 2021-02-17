package org.openeo.geotrellis.water_vapor

import java.util.concurrent.Callable

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{FloatConstantNoDataCellType, MultibandTile, Tile, doubleNODATA}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.icor.{AtmosphericCorrection, ICorCorrectionDescriptor, LookupTable, LookupTableIO}

class CWVProvider(correctionDescriptor: ICorCorrectionDescriptor) extends Serializable {

  private val bcLUT: Broadcast[LookupTable] = {
    val lutLoader = new Callable[Broadcast[LookupTable]]() {
      override def call(): Broadcast[LookupTable] = {
        SparkContext.getOrCreate().broadcast(LookupTableIO.readLUT(correctionDescriptor.getLookupTableURL()))
      }
    }

    if( correctionDescriptor!=null) {
      AtmosphericCorrection.iCorLookupTableCache.get(correctionDescriptor.getLookupTableURL(), lutLoader)
    }else{
      null
    }
  }


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
    bandIds:java.util.List[String]
  ) : Tile = {

    val wvBandId="B09"
    val r0BandId="B8A"
    val r1BandId="B11"
    
    //val bp = new FirstInBlockProcessor()
    val bp = new DoubleDownsampledBlockProcessor()
    val wvCalc = new AbdaWaterVaporCalculator()
    wvCalc.prepare(bcLUT.value,correctionDescriptor,wvBandId,r0BandId,r1BandId)
            
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