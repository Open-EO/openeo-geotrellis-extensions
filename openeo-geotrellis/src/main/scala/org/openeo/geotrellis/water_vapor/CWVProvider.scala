package org.openeo.geotrellis.water_vapor

import java.util.concurrent.Callable

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{FloatConstantNoDataCellType, MultibandTile, Tile, doubleNODATA}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.icor.{AtmosphericCorrection, CorrectionDescriptor, ICorCorrectionDescriptor, LookupTable, LookupTableIO}

class CWVProvider() extends Serializable {

	def findIndexOf(where: java.util.List[String], what: String) : Int = {
		var idx : Int = -1
		for( i:Int <- 0 until where.size()) {
			if (where.get(i).toLowerCase().contains(what.toLowerCase())) {
				idx=i
			}
		}
		return idx
	}
  
  
  def compute(
    multibandtile: (SpaceTimeKey, MultibandTile), // where is wv/r0/r1
    szaTile: Tile, 
    vzaTile: Tile, 
    raaTile: Tile,
    elevationTile: Tile,
    aot: Double,
    ozone: Double,
    bandIds:java.util.List[String],
    correctionDescriptor: CorrectionDescriptor
  ) : Tile = {

    // water vapor can only be called from icor correction, because it uses lookup table -> this cast is safe
    val cd=correctionDescriptor.asInstanceOf[ICorCorrectionDescriptor]
    
    val wvBandId="B09"
    val r0BandId="B8A"
    val r1BandId="B11"
    
    //val bp = new FirstInBlockProcessor()
    val bp = new DoubleDownsampledBlockProcessor()
    val wvCalc = new AbdaWaterVaporCalculator()
    wvCalc.prepare(cd,wvBandId,r0BandId,r1BandId)
            
    val wvTile= multibandtile._2.band(findIndexOf(bandIds,wvBandId)).convert(FloatConstantNoDataCellType)
    val r0Tile= multibandtile._2.band(findIndexOf(bandIds,r0BandId)).convert(FloatConstantNoDataCellType)
    val r1Tile= multibandtile._2.band(findIndexOf(bandIds,r1BandId)).convert(FloatConstantNoDataCellType)
                        
    // try to get at least 1 valid value on 60m resolution
    val mbtresult : Tile = try { 
      val wvRawResultTile=bp.computeDoubleBlocks(
        MultibandTile(
          szaTile, 
          vzaTile, 
          raaTile,
          elevationTile.convert(FloatConstantNoDataCellType), 
    // AOT overriden                      aotTile.convert(FloatConstantNoDataCellType), 
          wvTile, 
          r0Tile, 
          r1Tile
        ),
        6, // on 10m base resolution looking for a value on 60m resolution
        aot,
        ozone,
        doubleNODATA,
        cd.bcLUT,
        wvCalc
      )
      bp.replaceNoDataWithAverage(wvRawResultTile,doubleNODATA)
    } catch {
      case e: IllegalArgumentException => wvTile
    }
    
    mbtresult.convert(FloatConstantNoDataCellType)
   
 }
  
  
}