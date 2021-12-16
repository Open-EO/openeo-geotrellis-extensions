package org.openeo.geotrelliscommon

import geotrellis.layer.TileLayerMetadata
import geotrellis.raster.MultibandTile
import geotrellis.spark.ContextRDD
import org.apache.spark.rdd._



class OpenEORasterCube[K](rdd: RDD[(K, MultibandTile)], metadata: TileLayerMetadata[K],val openEOMetadata:OpenEORasterCubeMetadata) extends ContextRDD[K,MultibandTile,TileLayerMetadata[K]](rdd, metadata){


}
