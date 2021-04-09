package org.openeo.geotrellis.geotiff

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneId
import java.time.{LocalDate, ZonedDateTime}
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.{CellType, InterpretAsTargetCellType, MultibandTile, RasterRegion, RasterSource}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.vector._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.reflect.ClassTag
import scala.util.matching.Regex

object PyramidFactory {
  def from_disk(glob_pattern: String, date_regex: String): PyramidFactory =
    from_disk(glob_pattern, date_regex, interpret_as_cell_type = null, lat_lon = false)

  def from_disk(glob_pattern: String, date_regex: String, interpret_as_cell_type: String,
                lat_lon: Boolean): PyramidFactory =
    new PyramidFactory({
      val path = { // default to file: scheme
        val path = new Path(glob_pattern)
        val uri = path.toUri

        uri.getScheme match {
          case null => new Path("file", uri.getAuthority, uri.getPath)
          case _ => path
        }
      }

      HdfsUtils.listFiles(path, new Configuration)
        .map(path => (GeoTiffRasterSource(path.toString, parseTargetCellType(interpret_as_cell_type)),
          deriveDate(path.toString, date_regex.r)))
    }, date_regex.r, lat_lon)

  def from_s3(s3_uri: String, key_regex: String, date_regex: String, recursive: Boolean,
              interpret_as_cell_type: String): PyramidFactory =
    from_s3(s3_uri, key_regex, date_regex, recursive, interpret_as_cell_type, lat_lon = false)

  def from_s3(s3_uri: String, key_regex: String = ".*", date_regex: String, recursive: Boolean = false,
              interpret_as_cell_type: String = null, lat_lon: Boolean): PyramidFactory =
    new PyramidFactory({
      // adapted from geotrellis.spark.io.s3.geotiff.S3GeoTiffInput.list
      val s3Uri = new AmazonS3URI(s3_uri)
      val keyPattern = key_regex.r

      val requestBuilder = ListObjectsRequest.builder()
        .bucket(s3Uri.getBucket)
        .prefix(s3Uri.getKey)

      val request = (if (recursive) requestBuilder else requestBuilder.delimiter("/")).build()

      S3ClientProducer.get()
        .listObjects(request)
        .contents()
        .asScala
        .map(_.key())
        .flatMap(key => key match {
          case keyPattern(_*) => Some(new AmazonS3URI(s"s3://${s3Uri.getBucket}/${key}"))
          case _ => None
        })
        .map(uri =>
          (GeoTiffRasterSource(uri.toString, parseTargetCellType(interpret_as_cell_type)),
            deriveDate(uri.getKey, date_regex.r))
        ).toSeq
    }, date_regex.r, lat_lon)

  private def deriveDate(filename: String, date: Regex): ZonedDateTime = {
    filename match {
      case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), MIDNIGHT, ZoneId.of("UTC"))
    }
  }

  private def parseTargetCellType(targetCellType: String): Option[InterpretAsTargetCellType] =
    Option(targetCellType)
      .map(CellType.fromName)
      .map(InterpretAsTargetCellType.apply)
}

class PyramidFactory private (rasterSources: => Seq[(RasterSource, ZonedDateTime)], date: Regex, latLng: Boolean) {
  import PyramidFactory._

  private val targetCrs = if (latLng) LatLng else WebMercator

  private lazy val reprojectedRasterSources =
    rasterSources.map { case (rasterSource, date) => (rasterSource.reproject(targetCrs), date) }

  private lazy val maxZoom = reprojectedRasterSources.headOption match {
    case Some((rasterSource, _)) => ZoomedLayoutScheme(targetCrs).zoom(rasterSource.extent.center.getX, rasterSource.extent.center.getY, rasterSource.cellSize)
    case None => throw new IllegalStateException("no raster sources found")
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = if (bbox != null) ProjectedExtent(bbox, CRS.fromName(bbox_srs)) else ProjectedExtent(LatLng.worldExtent, LatLng)
    val from = if (from_date != null) ZonedDateTime.parse(from_date) else null
    val to = if (to_date != null) ZonedDateTime.parse(to_date) else null

    pyramid(projectedExtent, from, to).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime)(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom)
    Pyramid(layers.toMap)
  }

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)

    val overlappingRasterSources = reprojectedRasterSources
      .filter { case (rasterSource, date) =>
        // FIXME: this means the driver will (partially) read the geotiff instead of the executor - on the other hand, e.g. an AccumuloRDDReader will interpret a LayerQuery both in the driver (to determine Accumulo ranges) and the executors
        // FIXME: what's the advantage of an AttributeStore.query over comparing extents?
        val overlaps = rasterSource.extent intersects reprojectedBoundingBox.extent

        // FIXME: this is also done in the driver while a RasterSource carries a URI so an executor can do it himself
        val withinDateRange =
          if (from == null && to == null) true
          else if (from == null) !(date isAfter to)
          else if (to == null) !(date isBefore from)
          else !(date isBefore from) && !(date isAfter to)

        overlaps && withinDateRange
      }

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(zoom).layout

    lazy val bounds = {
      val spatialBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox.extent)
      val KeyBounds(SpatialKey(minCol, minRow), SpatialKey(maxCol, maxRow)) = KeyBounds(spatialBounds)
      KeyBounds(SpaceTimeKey(minCol, minRow, from), SpaceTimeKey(maxCol, maxRow, to))
    }

    val (rasterSources, _) = overlappingRasterSources.unzip
    rasterSourceRDD(rasterSources, layout, bounds)
  }

  private def rasterSourceRDD(
    rasterSources: Seq[RasterSource],
    layout: LayoutDefinition,
    bounds: => KeyBounds[SpaceTimeKey]
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val date = this.date
    val keyExtractor = TemporalKeyExtractor.fromPath { case GeoTiffPath(value) => deriveDate(value, date) }

    val sources = sc.parallelize(rasterSources).cache()
    val summary = RasterSummary.fromRDD(sources, keyExtractor.getMetadata)
    val layerMetadata = summary.toTileLayerMetadata(layout, keyExtractor.getKey)

    // FIXME: supply our own RasterSummary? (use bounds)
    tiledLayerRDD(
      sources,
      layout,
      keyExtractor,
      layerMetadata,
      SpacePartitioner(layerMetadata.bounds)
    )
  }

  // adapted from geotrellis.spark.RasterSourceRDD.tiledLayerRDD
  private def tiledLayerRDD[K: SpatialComponent: Boundable: ClassTag, M: Boundable](
    sources: RDD[RasterSource],
    layout: LayoutDefinition,
    keyExtractor: KeyExtractor.Aux[K, M],
    layerMetadata: TileLayerMetadata[K],
    partitioner: Partitioner
  ): MultibandTileLayerRDD[K] = {
    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => K = { sk => keyExtractor.getKey(m, sk) }
        rs.tileToLayout(layout, tileKeyTransform)
      }

    val rasterRegionRDD: RDD[(K, RasterRegion)] =
      tiledLayoutSourceRDD.flatMap { _.keyedRasterRegions() }

    val tiledRDD: RDD[(K, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner)
        .mapValues { overlappingRasterRegions =>
          overlappingRasterRegions
            .flatMap(rasterRegion => rasterRegion.raster.map(_.tile.interpretAs(rasterRegion.cellType)))
            .reduce(_ merge _)
        }

    ContextRDD(tiledRDD, layerMetadata)
  }

  def datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: optimize
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(polygons.polygons.toTraversable.extent, polygons.crs)
    val from = if (from_date != null) ZonedDateTime.parse(from_date) else null
    val to = if (to_date != null) ZonedDateTime.parse(to_date) else null

    Seq(0 -> layer(boundingBox, from, to))
  }
}
