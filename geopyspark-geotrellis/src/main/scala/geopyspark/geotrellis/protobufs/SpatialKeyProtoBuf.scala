package geopyspark.geotrellis.protobufs

import geopyspark.util._
import geotrellis.layer._
import protos.keyMessages._


trait SpatialKeyProtoBuf {
  implicit def spatialKeyProtoBufCodec = new ProtoBufCodec[SpatialKey, ProtoSpatialKey] {
    def encode(spatialKey: SpatialKey): ProtoSpatialKey =
      ProtoSpatialKey(col = spatialKey.col, row = spatialKey.row)

    def decode(message: ProtoSpatialKey): SpatialKey =
      SpatialKey(message.col, message.row)
  }
}
