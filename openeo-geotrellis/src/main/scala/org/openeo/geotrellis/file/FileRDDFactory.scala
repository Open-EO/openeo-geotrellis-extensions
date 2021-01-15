package org.openeo.geotrellis.file

import java.net.URL
import java.time.ZonedDateTime
import java.util

import geotrellis.layer.{FloatingLayoutScheme, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{CellSize, FloatConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.FileLayerProvider.layerMetadata
import org.openeo.geotrellis.layers.OpenSearch
import org.openeo.geotrellis.layers.OpenSearchResponses.{Feature, Link}
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner

import scala.collection.JavaConverters._

/**
 * A class that looks like a pyramid factory, but does not build a full datacube. Instead, it generates an RDD[SpaceTimeKey, ProductPath].
 * This RDD can then be transformed into
 */
class FileRDDFactory(openSearch: OpenSearch, openSearchCollectionId: String, openSearchLinkTitles: util.List[String], attributeValues: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String = "") {

  private val maxSpatialResolution = CellSize(10, 10)

  /**
   * Lookup OpenSearch Features
   */
  private def getFeatures(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc: SparkContext): Seq[Feature] = {
    require(zoom >= 0)

    val overlappingFeatures: Seq[Feature] = openSearch.getProducts(
      openSearchCollectionId,
      from.toLocalDate,
      to.toLocalDate,
      boundingBox,
      attributeValues = attributeValues.asScala.toMap,
      correlationId = correlationId
    )
    return overlappingFeatures
  }


  def loadSpatialFeatureRDD(polygons: ProjectedPolygons, from_date: String, to_date: String, zoom: Int, tileSize: Int = 256): ContextRDD[SpaceTimeKey, Feature, TileLayerMetadata[SpaceTimeKey]] = {
    val sc = SparkContext.getOrCreate()

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bbox = polygons.polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons.crs)
    //load product metadata from OpenSearch
    val productMetadata: Seq[Feature] = getFeatures(boundingBox, from, to, zoom,sc)

    //construct layer metadata
    //hardcoded celltype of float: assuming we will generate floats in further processing
    //use a floating layout scheme, so we will process data in original utm projection and 10m resolution
    val metadata: TileLayerMetadata[SpaceTimeKey] = layerMetadata(boundingBox, from, to, 0, FloatConstantNoDataCellType, FloatingLayoutScheme(tileSize), maxSpatialResolution)

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

  /**
   * Variant of `loadSpatialFeatureRDD` that allows working with the data in JavaRDD format in PySpark context:
   * (e.g. oscars response is JSON-serialized)
   */
  def loadSpatialFeatureJsonRDD(polygons: ProjectedPolygons, from_date: String, to_date: String, zoom: Int, tileSize: Int = 256): (JavaRDD[String], TileLayerMetadata[SpaceTimeKey]) = {
    import org.openeo.geotrellis.file.FileRDDFactory.{jsonObject, toJson}
    val crdd = loadSpatialFeatureRDD(polygons, from_date, to_date, zoom, tileSize)
    val jrdd = crdd.map { case (key, feature) => jsonObject(
      "key" -> toJson(key),
      "key_extent" -> toJson(crdd.metadata.mapTransform.keyToExtent(key)),
      "feature" -> jsonObject(
        "id" -> toJson(feature.id),
        "bbox" -> toJson(feature.bbox),
        "nominalDate" -> toJson(feature.nominalDate.toLocalDate.toString),
        "links" -> toJson(feature.links)
      ),
      "metadata" -> jsonObject(
        "extent" -> toJson(crdd.metadata.extent),
        "crs_epsg" -> crdd.metadata.crs.epsgCode.getOrElse(0).toString
      )
    )}.toJavaRDD()

    return (jrdd, crdd.metadata)
  }
}

object FileRDDFactory {

  def oscars(openSearchCollectionId: String, openSearchLinkTitles: util.List[String], attributeValues: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String = ""): FileRDDFactory = {
    val openSearch: OpenSearch = OpenSearch.oscars(new URL("http://oscars-01.vgt.vito.be:8080"))
    new FileRDDFactory(openSearch, openSearchCollectionId, openSearchLinkTitles, attributeValues, correlationId = correlationId)
  }

  def creo(openSearchCollectionId: String, openSearchLinkTitles: util.List[String], attributeValues: util.Map[String, Any] = util.Collections.emptyMap(), correlationId: String = ""): FileRDDFactory = {
    val openSearch: OpenSearch = OpenSearch.creo()
    new FileRDDFactory(openSearch, openSearchCollectionId, openSearchLinkTitles, attributeValues, correlationId = correlationId)
  }

  /*
   * Poor man's JSON serialization
   * TODO: can we reuse some more standard/general JSON-serialization functionality?
   */

  def toJson(s: String): String =
    "\"" + s + "\""

  def toJson(s: Option[String]): String =
    s.map(toJson).getOrElse("null")

  def toJson(k: SpaceTimeKey): String =
    s"""{"col": ${k.col}, "row": ${k.row}, "instant": ${k.instant} }"""

  def toJson(e: Extent): String =
    s"""{"xmin": ${e.xmin}, "ymin": ${e.ymin}, "xmax": ${e.xmax}, "ymax": ${e.ymax}}"""

  def toJson(l: Link): String =
    s"""{"href": {"file": ${toJson(l.href.toString)}}, "title": ${toJson(l.title)}}"""

  def toJson(a: Array[Link]): String =
    "[" + a.map(toJson).mkString(",") + "]"

  /**
   * Helper to build JSON object (values should already be JSON-serialized)
   */
  def jsonObject(items: (String, String)*): String =
    "{" + items.map { case (k, v) => toJson(k) + ": " + v }.toList.mkString(",") + "}"
}