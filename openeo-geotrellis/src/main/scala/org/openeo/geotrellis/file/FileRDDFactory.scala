package org.openeo.geotrellis.file

import java.net.URL
import java.time.ZonedDateTime
import java.util

import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.FloatConstantNoDataCellType
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.FileLayerProvider.layerMetadata
import org.openeo.geotrellis.layers.OpenSearch
import org.openeo.geotrellis.layers.OpenSearchResponses.Feature
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner

import scala.collection.JavaConverters._

/**
 * A class that looks like a pyramid factory, but does not build a full datacube. Instead, it generates an RDD[SpaceTimeKey, ProductPath].
 * This RDD can then be transformed into
 */
class FileRDDFactory(openSearchCollectionId: String, openSearchLinkTitles: util.List[String],attributeValues: util.Map[String, Any] = util.Collections.emptyMap(),correlationId: String = "") {

  protected val openSearch: OpenSearch = OpenSearch(new URL("http://oscars-01.vgt.vito.be:8080"))

  private def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc:SparkContext): Seq[Feature] = {
    require(zoom >= 0)

    val overlappingFeatures: Seq[Feature] = openSearch.getProducts(
      openSearchCollectionId,
      from.toLocalDate,
      to.toLocalDate,
      boundingBox,
      correlationId,
      attributeValues.asScala.toMap
    )
    return overlappingFeatures
  }


  def readMultibandTileLayer(polygons:ProjectedPolygons, from_date: String, to_date: String, zoom: Int): ContextRDD[SpaceTimeKey,Feature,TileLayerMetadata[SpaceTimeKey]] = {
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bbox = polygons.polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons.crs)
    //load product metadata from OpenSearch
    val productMetadata: Seq[Feature] = loadRasterSourceRDD(boundingBox, from, to, zoom,sc)

    //construct layer metadata
    //hardcoded celltype of float: assuming we will generate floats in further processing
    //use a floating layout scheme, so we will process data in original utm projection and 10m resolution
    val metadata: TileLayerMetadata[SpaceTimeKey] = layerMetadata(boundingBox, from, to, 0, FloatConstantNoDataCellType,FloatingLayoutScheme(256))

    //construct Spatial Keys that we want to load
    val requiredKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons.polygons).map{_.reproject(polygons.crs,metadata.crs)}.clipToGrid(metadata.layout).groupByKey()
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
