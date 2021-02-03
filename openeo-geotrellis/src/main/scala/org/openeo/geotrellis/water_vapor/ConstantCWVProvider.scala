package org.openeo.geotrellis.water_vapor

import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, doubleNODATA, Tile}
import geotrellis.layer.SpaceTimeKey
import org.openeo.geotrellis.icor.CorrectionDescriptor
import org.apache.spark.broadcast.Broadcast
import geotrellis.raster.FloatConstantNoDataCellType
import org.openeo.geotrellis.icor.LookupTable
import geotrellis.raster.DoubleConstantTile

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
    new FloatConstantNoDataCellType(constValue, multibandtile._2.band(0).cols, multibandtile._2.band(0).cols) 
 }
  
  
}