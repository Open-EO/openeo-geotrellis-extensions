package org.openeo.geotrellis.geotiff

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}

import geotrellis.layer._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.{MultibandTile, RasterSource}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag
import scala.util.matching.Regex

object PyramidFactory {
  def from_disk(glob_pattern: String, date_regex: String): PyramidFactory = {
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
        .map(path => (GeoTiffRasterSource(path.toString), deriveDate(path.toString, date_regex.r)))
    })
  }

  def from_s3(s3_uri: String, key_regex: String = ".*", date_regex: String): PyramidFactory = {
    new PyramidFactory({
      // adapted from geotrellis.spark.io.s3.geotiff.S3GeoTiffInput.list
      val s3Uri = new AmazonS3URI(s3_uri)
      val keyPattern = key_regex.r



      val request = ListObjectsRequest.builder()
        .bucket(s3Uri.getBucket)
        .prefix(s3Uri.getKey)
        .delimiter("/")
        .build()

      S3ClientProducer.get.apply()
        .listObjects(request)
        .contents()
        .asScala
        .map(_.key())
        .flatMap(key => key match {
          case keyPattern(_*) => Some(new AmazonS3URI(s"s3://${s3Uri.getBucket}/${key}"))
          case _ => None
        })
        .map(uri => (GeoTiffRasterSource(uri.toString), deriveDate(uri.getKey, date_regex.r))).toSeq
    })
  }

  private def deriveDate(filename: String, date: Regex): ZonedDateTime = {
    filename match {
      case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), MIDNIGHT, UTC)
    }
  }

  private def partition[T: ClassTag](
    chunks: Traversable[T],
    maxPartitionSize: Long
  )(chunkSize: T => Long = { c: T => 1l }): Array[Array[T]] = {
    if (chunks.isEmpty) {
      Array[Array[T]]()
    } else {
      val partition = ArrayBuilder.make[T]
      partition.sizeHintBounded(128, chunks)
      var partitionSize: Long = 0l
      var partitionCount: Long = 0l
      val partitions = ArrayBuilder.make[Array[T]]

      def finalizePartition() {
        val res = partition.result
        if (res.nonEmpty) partitions += res
        partition.clear()
        partitionSize = 0l
        partitionCount = 0l
      }

      def addToPartition(chunk: T) {
        partition += chunk
        partitionSize += chunkSize(chunk)
        partitionCount += 1
      }

      for (chunk <- chunks) {
        if ((partitionCount == 0) || (partitionSize + chunkSize(chunk)) < maxPartitionSize)
          addToPartition(chunk)
        else {
          finalizePartition()
          addToPartition(chunk)
        }
      }

      finalizePartition()
      partitions.result
    }
  }
}

class PyramidFactory private (rasterSources: => Seq[(RasterSource, ZonedDateTime)]) {
  import PyramidFactory._

  private val targetCrs = WebMercator

  private lazy val reprojectedRasterSources =
    rasterSources.map { case (rasterSource, date) => (rasterSource.reproject(targetCrs), date) }

  private lazy val maxZoom = reprojectedRasterSources.headOption match {
    case Some((rasterSource, _)) => ZoomedLayoutScheme(targetCrs).zoom(rasterSource.extent.center.getX, rasterSource.extent.center.getY, rasterSource.cellSize)
    case None => throw new IllegalStateException("no raster sources found")
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

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
        val withinDateRange = !(date isBefore from) && !(date isAfter to)

        overlaps && withinDateRange
      }

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(zoom).layout

    rasterSourceRDD(overlappingRasterSources, layout)
  }

  // adapted from geotrellis.contrib.vlm.spark.RasterSourceRDD.apply
  private def rasterSourceRDD(
    sources: Seq[(RasterSource, ZonedDateTime)],
    layout: LayoutDefinition,
    partitionBytes: Long = RasterSourceRDD.DEFAULT_PARTITION_BYTES
  )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val cellTypes = sources.map { case (source, _) => source.cellType }.toSet
    require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = sources.map { case (source, _) => source.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val Some((lowerTimestamp, upperTimestamp)) = sources
      .map { case (_, timestamp) => timestamp }
      .foldLeft(z = None: Option[(ZonedDateTime, ZonedDateTime)]) { case (acc, timestamp) =>
        acc match {
          case None => Some((timestamp, timestamp))
          case Some((lower, upper)) => Some((
            if (timestamp isBefore lower) timestamp else lower,
            if (timestamp isAfter upper) timestamp else upper
          ))
        }
      }

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { case (source, _) => source.extent }.reduce { _ combine _ }

    val spatialKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val layerKeyBounds = KeyBounds(
      SpaceTimeKey(spatialKeyBounds.minKey, TemporalKey(lowerTimestamp)),
      SpaceTimeKey(spatialKeyBounds.maxKey, TemporalKey(upperTimestamp))
    )

    val layerMetadata =
      TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, layerKeyBounds)

    val sourcesRDD: RDD[((RasterSource, ZonedDateTime), Array[SpatialKey])] =
      sc.parallelize(sources).flatMap { case (source, timestamp) =>
        val keys: Traversable[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              layout.mapTransform.keysForGeometry(intersection.toPolygon)
            case None =>
              Seq.empty[SpatialKey]
          }
        val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
        partition(keys, partitionBytes)( _ => tileSize).map { res => ((source, timestamp), res) }
      }

    val repartitioned = sourcesRDD

    val result: RDD[(SpaceTimeKey, MultibandTile)] =
      repartitioned.flatMap { case ((source, timestamp), keys) =>
        // might need to e.g. anonymously subclass GeoTiffReprojectRasterSource to be able to pass a custom S3 client
        // (see org.openeo.geotrellis.geotiff.PyramidFactoryTest.anonymousInnerClass)
        val tileSource = source.tileToLayout(layout)
        tileSource.readAll(keys.toIterator).map { case (SpatialKey(col, row), tile) =>
          (SpaceTimeKey(col = col, row = row, timestamp), tile)
        }
      }

    ContextRDD(result, layerMetadata)
  }
}
