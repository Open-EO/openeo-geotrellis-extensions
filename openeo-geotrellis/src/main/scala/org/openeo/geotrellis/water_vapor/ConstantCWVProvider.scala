package org.openeo.geotrellis.water_vapor

import geotrellis.layer.SpaceTimeKey
import geotrellis.raster.{FloatConstantTile, MultibandTile, Tile}
import org.openeo.geotrellis.icor.CorrectionDescriptor


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
    bandIds:java.util.List[String],
    correctionDescriptor: CorrectionDescriptor
  ) : Tile = {
    new FloatConstantTile(constValue.toFloat, multibandtile._2.band(0).cols, multibandtile._2.band(0).rows) 
 }
  
  
}