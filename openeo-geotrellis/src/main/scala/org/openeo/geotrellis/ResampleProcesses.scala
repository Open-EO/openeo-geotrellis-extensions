package org.openeo.geotrellis

import geotrellis.layer.{EmptyBounds, KeyBounds, SpaceTimeKey}
import geotrellis.spark.MultibandTileLayerRDD
import org.openeo.geotrelliscommon.{CubeProcessProvider, OpenEOProcess}

/**
 * openEO processes that resample datacubes.
 * They are exposed to the Python driver through [[org.openeo.geotrelliscommon.CubeProcessRegistry]].
 */
object ResampleProcesses {

  @OpenEOProcess(
    id = "resample_cube_temporal",
    description = "Resamples the temporal dimension of the datacube to align with the temporal dimension of the target datacube (argument 'target') using the nearest neighbor method; ties are resolved by choosing the earlier timestamp. With argument 'valid_within' (days), each pixel gets the nearest valid value within that many days before or after the target timestamp, or no-data if there is none. The optional argument 'dimension' has to be null or the name of the (single) temporal dimension."
  )
  def resampleCubeTemporal(datacube: Object, args: java.util.Map[String, Any]): Object = {
    def arg(name: String): Option[Any] = Option(args).flatMap(a => Option(a.get(name)))

    val target = arg("target") match {
      case Some(cube: MultibandTileLayerRDD[_]) => spaceTimeCube(cube, "target")
      case Some(other) => throw new IllegalArgumentException(s"resample_cube_temporal: 'target' should be a datacube, got: ${other.getClass.getName}")
      case None => throw new IllegalArgumentException("resample_cube_temporal: missing required argument 'target'")
    }
    val validWithin = arg("valid_within") match {
      case None => None
      case Some(n: Number) => Some(n.doubleValue())
      case Some(s: String) if s.trim.toDoubleOption.isDefined => Some(s.trim.toDouble)
      case Some(other) => throw new IllegalArgumentException(s"resample_cube_temporal: 'valid_within' should be a number or null, got: $other")
    }
    arg("dimension") match {
      // a GeoTrellis datacube has a single temporal dimension, its name is only known to the Python driver
      case None | Some(_: String) =>
      case Some(other) => throw new IllegalArgumentException(s"resample_cube_temporal: 'dimension' should be a string or null, got: $other")
    }

    val data = datacube.asInstanceOf[MultibandTileLayerRDD[_]]
    if (data.metadata.bounds == EmptyBounds) {
      datacube
    } else {
      new OpenEOProcesses().resampleCubeTemporal(spaceTimeCube(data, "data"), target, validWithin)
    }
  }

  private def spaceTimeCube(cube: MultibandTileLayerRDD[_], parameter: String): MultibandTileLayerRDD[SpaceTimeKey] =
    cube.metadata.bounds match {
      case KeyBounds(_: SpaceTimeKey, _) | EmptyBounds => cube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]]
      case _ => throw new IllegalArgumentException(s"resample_cube_temporal: DimensionMismatch, '$parameter' has no temporal dimension")
    }
}

class ResampleProcessesProvider extends CubeProcessProvider {
  def getInstance(): AnyRef = ResampleProcesses
}
