package org.openeo.geotrellis.file

import java.time.ZonedDateTime

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.FloatConstantNoDataCellType
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import org.openeo.geotrellis.layers.FileLayerProvider.layerMetadata
import org.openeo.geotrellis.layers.Oscars
import org.openeo.geotrellis.layers.OscarsResponses.Feature


/**
 * A class that looks like a pyramid factory, but does not build a full datacube. Instead, it generates an RDD[SpaceTimeKey, ProductPath].
 * This RDD can then be transformed into
 */
class FileRDDFactory(oscarsCollectionId: String, oscarsLinkTitles: NonEmptyList[String],attributeValues: Map[String, Any] = Map(),correlationId: String = "") {

  protected val oscars: Oscars = Oscars()

  private def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc:SparkContext): Seq[Feature] = {
    require(zoom >= 0)

    val overlappingFeatures: Seq[Feature] = oscars.getProducts(
      collectionId = oscarsCollectionId,
      from.toLocalDate,
      to.toLocalDate,
      boundingBox,
      correlationId,
      attributeValues
    )
    return overlappingFeatures
  }


  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon],polygons_crs: CRS, zoom: Int, sc: SparkContext): ContextRDD[SpaceTimeKey,Feature,TileLayerMetadata[SpaceTimeKey]] = {
    //load product metadata from oscars
    val productMetadata: Seq[Feature] = loadRasterSourceRDD(boundingBox, from, to, zoom,sc)

    //construct layer metadata
    //hardcoded celltype of float: assuming we will generate floats in further processing
    //use a floating layout scheme, so we will process data in original utm projection and 10m resolution
    val metadata: TileLayerMetadata[SpaceTimeKey] = layerMetadata(boundingBox, from, to, 0, FloatConstantNoDataCellType,FloatingLayoutScheme(256))

    //construct Spatial Keys that we want to load
    val requiredKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons).map{_.reproject(polygons_crs,metadata.crs)}.clipToGrid(metadata.layout).groupByKey()
    val productsRDD = sc.parallelize(productMetadata)
    val spatialRDD: RDD[(SpaceTimeKey, Feature)] = requiredKeys.keys.cartesian(productsRDD).map{ case (key,value) => (SpaceTimeKey(key.col,key.row,value.nominalDate),value)}

    /**
     * Note that keys in spatialRDD are not necessarily unique, because one date can have multiple Sentinel-1 products
     * Further possible improvements:
     * already filter out spatialkeys that do not match the feature extent
     */
    return new ContextRDD(spatialRDD.partitionBy(SpacePartitioner(metadata.bounds)),metadata)
  }
}
