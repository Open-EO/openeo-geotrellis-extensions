package org.openeo.geotrellis.water_vapor

import geotrellis.raster.MultibandTile
import geotrellis.raster.Tile
import geotrellis.layer.SpaceTimeKey
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.icor.CorrectionDescriptor
import org.openeo.geotrellis.icor.LookupTable
import geotrellis.raster.FloatConstantTile


// TODO: this class is a temporary solution, remove when water vapor calculator is refactored
class ConstantCWVProvider(constValue: Double) extends CWVProvider {

  override def compute(
    multibandtile: (SpaceTimeKey, MultibandTile),
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
    new FloatConstantTile(constValue.toFloat, multibandtile._2.band(0).cols, multibandtile._2.band(0).rows) 
 }
  
  
}