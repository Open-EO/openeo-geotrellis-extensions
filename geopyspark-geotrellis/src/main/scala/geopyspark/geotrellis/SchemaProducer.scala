package geopyspark.geotrellis

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.store.avro._
import geotrellis.vector._


object SchemaProducer {
  import Constants._

  def getSchema(keyType: String): String =
    keyType match {
      case PROJECTEDEXTENT =>
        implicitly[AvroRecordCodec[(ProjectedExtent, MultibandTile)]].schema.toString
      case TEMPORALPROJECTEDEXTENT =>
        implicitly[AvroRecordCodec[(TemporalProjectedExtent, MultibandTile)]].schema.toString

      case SPATIALKEY =>
        implicitly[AvroRecordCodec[(SpatialKey, MultibandTile)]].schema.toString
      case SPACETIMEKEY =>
        implicitly[AvroRecordCodec[(SpaceTimeKey, MultibandTile)]].schema.toString
    }
}
