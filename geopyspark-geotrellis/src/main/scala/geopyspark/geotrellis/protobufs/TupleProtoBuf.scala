package geopyspark.geotrellis.protobufs

import com.google.protobuf.ByteString
import geopyspark.geotrellis.protobufs.Implicits._
import geopyspark.util.ProtoBufCodec
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.vector._
import protos.tupleMessages._


trait TupleProtoBuf {

  implicit def tupleProjectedExtentProtoBufCodec: ProtoBufCodec[(ProjectedExtent, MultibandTile), ProtoTuple] =
    new ProtoBufCodec[(ProjectedExtent, MultibandTile), ProtoTuple] {
    def encode(tuple: (ProjectedExtent, MultibandTile)): ProtoTuple =
      ProtoTuple(
        projectedExtent = Some(projectedExtentProtoBufCodec.encode(tuple._1)),
        tiles = Some(multibandTileProtoBufCodec.encode(tuple._2)))

    def decode(message: ProtoTuple): (ProjectedExtent, MultibandTile) =
      (projectedExtentProtoBufCodec.decode(message.projectedExtent.get),
        multibandTileProtoBufCodec.decode(message.tiles.get))
  }

  implicit def tupleProjectedExtentImageProtoBufCodec =
    new ProtoBufCodec[(ProjectedExtent, Array[Byte]), ProtoTuple] {
    def encode(tuple: (ProjectedExtent, Array[Byte])): ProtoTuple =
      ProtoTuple(
        projectedExtent = Some(projectedExtentProtoBufCodec.encode(tuple._1)),
        imageBytes = ByteString.copyFrom(tuple._2))

    def decode(message: ProtoTuple): (ProjectedExtent, Array[Byte]) =
      throw new Exception("This ProtoBuf message cannot be decoded")
  }

  implicit def tupleTemporalProjectedExtentProtoBufCodec =
    new ProtoBufCodec[(TemporalProjectedExtent, MultibandTile), ProtoTuple] {
    def encode(tuple: (TemporalProjectedExtent, MultibandTile)): ProtoTuple =
      ProtoTuple(
        temporalProjectedExtent = Some(temporalProjectedExtentProtoBufCodec.encode(tuple._1)),
        tiles = Some(multibandTileProtoBufCodec.encode(tuple._2)))

    def decode(message: ProtoTuple): (TemporalProjectedExtent, MultibandTile) =
      (temporalProjectedExtentProtoBufCodec.decode(message.temporalProjectedExtent.get),
        multibandTileProtoBufCodec.decode(message.tiles.get))
  }

  implicit def tupleTemporalProjectedExtentImageProtoBufCodec =
    new ProtoBufCodec[(TemporalProjectedExtent, Array[Byte]), ProtoTuple] {
    def encode(tuple: (TemporalProjectedExtent, Array[Byte])): ProtoTuple =
      ProtoTuple(
        temporalProjectedExtent = Some(temporalProjectedExtentProtoBufCodec.encode(tuple._1)),
        imageBytes = ByteString.copyFrom(tuple._2))

    def decode(message: ProtoTuple): (TemporalProjectedExtent, Array[Byte]) =
      throw new Exception("This ProtoBuf message cannot be decoded")
  }

  implicit def tupleSpatialKeyProtoBufCodec =
    new ProtoBufCodec[(SpatialKey, MultibandTile), ProtoTuple] {
    def encode(tuple: (SpatialKey, MultibandTile)): ProtoTuple =
      ProtoTuple(
        spatialKey = Some(spatialKeyProtoBufCodec.encode(tuple._1)),
        tiles = Some(multibandTileProtoBufCodec.encode(tuple._2)))

    def decode(message: ProtoTuple): (SpatialKey, MultibandTile) =
      (spatialKeyProtoBufCodec.decode(message.spatialKey.get),
        multibandTileProtoBufCodec.decode(message.tiles.get))
  }

  implicit def tupleSpatialKeyImageProtoBufCodec =
    new ProtoBufCodec[(SpatialKey, Array[Byte]), ProtoTuple] {
    def encode(tuple: (SpatialKey, Array[Byte])): ProtoTuple =
      ProtoTuple(
        spatialKey = Some(spatialKeyProtoBufCodec.encode(tuple._1)),
        imageBytes = ByteString.copyFrom(tuple._2))

    def decode(message: ProtoTuple): (SpatialKey, Array[Byte]) =
      throw new Exception("This ProtoBuf message cannot be decoded")
  }

  implicit def tupleSpaceTimeKeyProtoBufCodec =
    new ProtoBufCodec[(SpaceTimeKey, MultibandTile), ProtoTuple] {
    def encode(tuple: (SpaceTimeKey, MultibandTile)): ProtoTuple =
      ProtoTuple(
        spaceTimeKey = Some(spaceTimeKeyProtoBufCodec.encode(tuple._1)),
        tiles = Some(multibandTileProtoBufCodec.encode(tuple._2)))

    def decode(message: ProtoTuple): (SpaceTimeKey, MultibandTile) =
      (spaceTimeKeyProtoBufCodec.decode(message.spaceTimeKey.get),
        multibandTileProtoBufCodec.decode(message.tiles.get))
  }

  implicit def tupleSpaceTimeKeyImageProtoBufCodec =
    new ProtoBufCodec[(SpaceTimeKey, Array[Byte]), ProtoTuple] {
    def encode(tuple: (SpaceTimeKey, Array[Byte])): ProtoTuple =
      ProtoTuple(
        spaceTimeKey = Some(spaceTimeKeyProtoBufCodec.encode(tuple._1)),
        imageBytes = ByteString.copyFrom(tuple._2))

    def decode(message: ProtoTuple): (SpaceTimeKey, Array[Byte]) =
      throw new Exception("This ProtoBuf message cannot be decoded")
  }
}
