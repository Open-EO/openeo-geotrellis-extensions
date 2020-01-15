package org.openeo

import geotrellis.layer._
import geotrellis.raster.MultibandTile
import geotrellis.spark.partition.PartitionerIndex
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.index.zcurve.Z3
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters.mapAsScalaMapConverter

package object geotrellisaccumulo {

    def createGeotrellisPyramid(levels: java.util.Map[Integer,RDD[(SpaceTimeKey,MultibandTile)]  with Metadata[TileLayerMetadata[SpaceTimeKey]]]): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] ={
      val map: Map[Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = levels.asScala.toMap.map(t=>(t._1.toInt,t._2))
      new Pyramid(map)
    }

  implicit object SpaceTimeByMonthPartitioner extends  PartitionerIndex[SpaceTimeKey] {
    private def toZ(key: SpaceTimeKey): Z3 = Z3(key.col >> 4, key.row >> 4, 13*key.time.getYear + key.time.getMonthValue)

    def toIndex(key: SpaceTimeKey): BigInt = toZ(key).z

    def indexRanges(keyRange: (SpaceTimeKey, SpaceTimeKey)): Seq[(BigInt, BigInt)] =
      Z3.zranges(toZ(keyRange._1), toZ(keyRange._2))
  }
}
