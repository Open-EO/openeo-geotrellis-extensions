package org.openeo

import geotrellis.layer._
import geotrellis.raster.MultibandTile
import geotrellis.spark.pyramid.Pyramid
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters.mapAsScalaMapConverter

package object geotrellisaccumulo {

    def createGeotrellisPyramid(levels: java.util.Map[Integer,RDD[(SpaceTimeKey,MultibandTile)]  with Metadata[TileLayerMetadata[SpaceTimeKey]]]): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] ={
      val map: Map[Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = levels.asScala.toMap.map(t=>(t._1.toInt,t._2))
      return new Pyramid(map)
    }
}
