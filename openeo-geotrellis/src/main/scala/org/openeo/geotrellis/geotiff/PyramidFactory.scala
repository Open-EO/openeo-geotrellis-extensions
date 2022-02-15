package org.openeo.geotrellis.geotiff

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource}
import geotrellis.raster.{CellSize, CellType, InterpretAsTargetCellType, MultibandTile, RasterRegion, RasterSource}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.store.s3.AmazonS3URI
import geotrellis.vector.Extent.toPolygon
import geotrellis.vector._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.locationtech.proj4j.proj.TransverseMercatorProjection
import org.openeo.geotrellis.layers.{BandCompositeRasterSource, FileLayerProvider}
import org.openeo.geotrellis.{ProjectedPolygons, bucketRegion, s3Client}
import org.openeo.geotrelliscommon.{DataCubeParameters, DatacubeSupport, OpenEORasterCube, OpenEORasterCubeMetadata, SpaceTimeByMonthPartitioner}
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request

import java.time.LocalTime.MIDNIGHT
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
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
          deriveDate(date_regex.r)(path.toString)))
    }, deriveDate(date_regex.r), lat_lon)

  def from_disk(timestamped_paths: util.Map[String, String]): PyramidFactory = {
    val sc = SparkContext.getOrCreate()

    val timestampedPaths = timestamped_paths.asScala
      .mapValues { timestamp => ZonedDateTime.parse(timestamp) }
      .toMap

    val broadcastedTimestampedPaths = sc.broadcast(timestampedPaths)

    new PyramidFactory(
      rasterSources = timestampedPaths
        .map { case (path, timestamp) => GeoTiffRasterSource(path) -> timestamp }
        .toSeq,
      extractDateFromPath = broadcastedTimestampedPaths.value, latLng = false)
  }

  def from_s3(s3_uri: String, key_regex: String, date_regex: String, recursive: Boolean,
              interpret_as_cell_type: String): PyramidFactory =
    from_s3(s3_uri, key_regex, date_regex, recursive, interpret_as_cell_type, lat_lon = false)

  def from_s3(s3_uri: String, key_regex: String = ".*", date_regex: String, recursive: Boolean = false,
              interpret_as_cell_type: String = null, lat_lon: Boolean): PyramidFactory =
    new PyramidFactory({
      val s3Uri = new AmazonS3URI(s3_uri)
      val keyPattern = key_regex.r

      val listObjectsRequest = {
        val requestBuilder = ListObjectsV2Request.builder()
          .bucket(s3Uri.getBucket)
          .prefix(s3Uri.getKey)

        (if (recursive) requestBuilder else requestBuilder.delimiter("/")).build()
      }

      s3Client(bucketRegion(s3Uri.getBucket))
        .listObjectsV2Paginator(listObjectsRequest)
        .contents()
        .asScala
        .map(_.key())
        .flatMap(key => key match {
          case keyPattern(_*) => Some(new AmazonS3URI(s"s3://${s3Uri.getBucket}/$key"))
          case _ => None
        })
        .map(uri =>
          (GeoTiffRasterSource(uri.toString, parseTargetCellType(interpret_as_cell_type)),
            deriveDate(date_regex.r)(uri.getKey))
        ).toSeq
    }, deriveDate(date_regex.r), lat_lon)

  private def deriveDate(date: Regex)(path: String): ZonedDateTime = {
    path match {
      case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), MIDNIGHT, ZoneId.of("UTC"))
    }
  }

  private def parseTargetCellType(targetCellType: String): Option[InterpretAsTargetCellType] =
    Option(targetCellType)
      .map(CellType.fromName)
      .map(InterpretAsTargetCellType.apply)
}

class PyramidFactory private (rasterSources: => Seq[(RasterSource, ZonedDateTime)],
                              extractDateFromPath: String => ZonedDateTime, latLng: Boolean) {
  private val targetCrs = if (latLng) LatLng else WebMercator

  private lazy val reprojectedRasterSources =
    rasterSources.map { case (rasterSource, date) => (rasterSource.reproject(targetCrs), date) }

  private lazy val maxZoom = reprojectedRasterSources.headOption match {
    case Some((rasterSource, _)) => ZoomedLayoutScheme(targetCrs).zoom(rasterSource.extent.center.getX, rasterSource.extent.center.getY, rasterSource.cellSize)
    case None => throw new IllegalStateException("no raster sources found")
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = if (bbox != null) ProjectedExtent(bbox, CRS.fromName(bbox_srs)) else ProjectedExtent(targetCrs.worldExtent, targetCrs)
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

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, parameters: DataCubeParameters = new DataCubeParameters)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)
    layer(reprojectedRasterSources, reprojectedBoundingBox, from, to, parameters, zoom = zoom)
  }

  private def layer(rasterSources: Seq[(RasterSource, ZonedDateTime)], boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, params: DataCubeParameters, zoom: Int = maxZoom)(implicit sc: SparkContext): OpenEORasterCube[SpaceTimeKey] = {
    val overlappingRasterSources = rasterSources
      .filter { case (rasterSource, date) =>
        // FIXME: this means the driver will (partially) read the geotiff instead of the executor - on the other hand, e.g. an AccumuloRDDReader will interpret a LayerQuery both in the driver (to determine Accumulo ranges) and the executors
        // FIXME: what's the advantage of an AttributeStore.query over comparing extents?
        val overlaps = rasterSource.extent intersects boundingBox.extent

        // FIXME: this is also done in the driver while a RasterSource carries a URI so an executor can do it himself
        val withinDateRange =
          if (from == null && to == null) true
          else if (from == null) !(date isAfter to)
          else if (to == null) !(date isBefore from)
          else !(date isBefore from) && !(date isAfter to)

        overlaps && withinDateRange
      }
      .map { case (rasterSource, date) =>
        new BandCompositeRasterSource(NonEmptyList.of(rasterSource), boundingBox.crs, Predef.Map("date"->date.toString))
      }

    val resultRDD = rasterSourceRDD(overlappingRasterSources,boundingBox,from,to, params)
    new OpenEORasterCube[SpaceTimeKey](resultRDD,resultRDD.metadata, OpenEORasterCubeMetadata())
  }

  private def rasterSourceRDD(
    rasterSources: Seq[RasterSource],
    boundingBox: ProjectedExtent,
    from: ZonedDateTime, to: ZonedDateTime,
    params: DataCubeParameters,
    zoom:Int = maxZoom
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val extractDateFromPath = this.extractDateFromPath
    val keyExtractor = TemporalKeyExtractor.fromPath { case GeoTiffPath(value) => extractDateFromPath(value) }

    val sources = sc.parallelize(rasterSources).cache()
    val summary = RasterSummary.fromRDD(sources, keyExtractor.getMetadata)

    val scheme = if(params.layoutScheme=="ZoomedLayoutScheme"){
      ZoomedLayoutScheme(targetCrs)
    }else{
      FloatingLayoutScheme(params.tileSize)
    }

    val layerMetadata = DatacubeSupport.layerMetadata(boundingBox,from,to,zoom,summary.cellType,scheme,summary.cellSize,params.globalExtent)
    val sourceRDD = FileLayerProvider.rasterSourceRDD(rasterSources,layerMetadata,summary.cellSize,"Geotiff collection")
    FileLayerProvider.readMultibandTileLayer(sourceRDD,layerMetadata,Array(MultiPolygon(toPolygon(boundingBox.extent))),boundingBox.crs,sc,datacubeParams = Some(params))

  }


  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val boundingBox = ProjectedExtent(polygons.polygons.toTraversable.extent, polygons.crs)
    val from = if (from_date != null) ZonedDateTime.parse(from_date) else null
    val to = if (to_date != null) ZonedDateTime.parse(to_date) else null

    if (latLng) // TODO: drop the workaround for what look like negative SpatialKeys?
      Seq(0 -> layer(boundingBox, from, to))
    else {
      Seq(0 -> layer(rasterSources, boundingBox, from, to, dataCubeParameters))
    }

  }

  def datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    datacube_seq(polygons, from_date, to_date,null,"",new DataCubeParameters)
  }


}
