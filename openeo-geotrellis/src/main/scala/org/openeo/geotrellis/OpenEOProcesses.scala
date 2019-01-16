package org.openeo.geotrellis

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, Metadata, MultibandTileLayerRDD, TileLayerMetadata}
import org.apache.spark.rdd.RDD


class OpenEOProcesses extends Serializable {

  val unaryProcesses: Map[String, Tile => Tile] = Map(
    "absolute" -> Abs.apply,
    //TODO "exp"
    "ln" -> Log.apply,
    //TODO "log"
    "sqrt" -> Sqrt.apply,
    "ceil" -> Ceil.apply,
    "floor" -> Floor.apply,
    //TODO: "int" integer part of a number
    "round" -> Round.apply,
    "arccos" -> Acos.apply,
    //TODO "arccos" -> Acosh.apply,
    "arcsin" -> Asin.apply,
    "arctan" -> Atan.apply,
    //TODO: arctan 2 is not unary! "arctan2" -> Atan2.apply,
    "cos" -> Cos.apply,
    "cosh" -> Cosh.apply,
    "sin" -> Sin.apply,
    "sinh" -> Sinh.apply,
    "tan" -> Tan.apply,
    "tanh" -> Tanh.apply
  )

  def applyProcess[K](datacube:MultibandTileLayerRDD[K], process:String): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]= {
    return ContextRDD(datacube.map(multibandtile => (multibandtile._1,multibandtile._2.mapBands((b,t) => unaryProcesses.get(process).get(t) ))),datacube.metadata)
  }


}
