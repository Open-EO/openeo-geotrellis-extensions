package org.openeo.geotrellis.layers

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.layer._
import geotrellis.proj4.LatLng
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource}
import geotrellis.raster.{MultibandTile, RasterRegion, RasterSource}
import geotrellis.spark._
import geotrellis.spark.partition.SpatialPartitioner
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.util._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit.HOURS
import scala.reflect.ClassTag
import scala.util.matching.Regex

object GlobalNetCdfFileLayerProvider {
  private val crs = LatLng
  private val layoutScheme = ZoomedLayoutScheme(crs, 256)

  private case class CacheKey(dataGlob: String, bandName: String, dateRegex: Regex)

  // TODO: maybe it's cleaner to cache the Paths instead and only use the dataGlob as the cache key
  private val rasterSourcesCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, HOURS)
    .build(new CacheLoader[CacheKey, Array[(ZonedDateTime, RasterSource)]] {
      override def load(cacheKey: CacheKey): Array[(ZonedDateTime, RasterSource)] = {
        val paths = HdfsUtils.listFiles(new Path(s"file:${cacheKey.dataGlob}"), new Configuration)

        val datedPaths = paths
          .map(path => deriveDate(path.toUri.getPath, cacheKey.dateRegex) -> path.toUri.getPath)
          .groupBy { case (date, _) => date }
          .mapValues(_.map { case (_, path) => path }.max) // take RTxs into account

        datedPaths
          .toArray
          .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }
          .map { case (date, path) => date -> GDALRasterSource(s"""NETCDF:"$path":${cacheKey.bandName}""" ) }
      }
    })

  private def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
  }
}

// TODO: find a better name?
class GlobalNetCdfFileLayerProvider(dataGlob: String, bandName: String, dateRegex: Regex) extends LayerProvider {
  import GlobalNetCdfFileLayerProvider._

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null,
                                      zoom: Int = Int.MaxValue, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val rasterSources = query(from, to)

    if (rasterSources.isEmpty) throw new IllegalArgumentException("no fitting raster sources found")

    val sources = sc.parallelize(rasterSources)

    val date = dateRegex
    val keyExtractor = TemporalKeyExtractor.fromPath { case GDALPath(value) => deriveDate(value, date) }

    val maxZoom = layoutScheme.levelFor(rasterSources.head.extent, rasterSources.head.cellSize).zoom
    val layout = layoutScheme.levelForZoom(zoom min maxZoom).layout

    implicit val _sc: SparkContext = sc // TODO: clean up

    tiledLayerRDD(
      sources,
      layout,
      keyExtractor,
      boundingBox.reproject(crs)
    )
  }

  private def queryAll(): Array[(ZonedDateTime, RasterSource)] =
    rasterSourcesCache.get(CacheKey(dataGlob, bandName, dateRegex))

  private def query(from: ZonedDateTime, to: ZonedDateTime): Array[RasterSource] = {
    queryAll()
      .dropWhile { case (date, _) => date isBefore from }
      .takeWhile  { case (date, _) => !(date isAfter to) }
      .map { case (_, rasterSource) => rasterSource }
  }

  private def tiledLayerRDD[K: SpatialComponent: Boundable: ClassTag, M: Boundable](
   sources: RDD[RasterSource],
   layout: LayoutDefinition,
   keyExtractor: KeyExtractor.Aux[K, M],
   boundingBox: Extent,
   rasterSummary: Option[RasterSummary[M]] = None,
   partitioner: Option[Partitioner] = None
 )(implicit sc: SparkContext): MultibandTileLayerRDD[K] = {
    val summary = rasterSummary.getOrElse(RasterSummary.fromRDD(sources, keyExtractor.getMetadata))
    val layerMetadata = summary.toTileLayerMetadata(layout, keyExtractor.getKey)

    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => K = { sk => keyExtractor.getKey(m, sk) }
        rs.tileToLayout(layout, tileKeyTransform)
      }

    val rasterRegionRDD: RDD[(K, RasterRegion)] =
      tiledLayoutSourceRDD.flatMap { tiledLayoutSource =>
        tiledLayoutSource
          .keyedRasterRegions()
          .filter { case (key, _) => key.getComponent[SpatialKey].extent(layout) intersects boundingBox }
      }

    // The number of partitions estimated by RasterSummary can sometimes be much
    // lower than what the user set. Therefore, we assume that the larger value
    // is the optimal number of partitions to use.
    val partitionCount =
    math.max(rasterRegionRDD.getNumPartitions, summary.estimatePartitionsNumber)

    val tiledRDD: RDD[(K, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner.getOrElse(SpatialPartitioner[K](partitionCount)))
        .mapValues { iter =>
          MultibandTile( // TODO: use our version? (see org.openeo.geotrellis.geotiff.PyramidFactory.tiledLayerRDD)
            iter.flatMap { _.raster.toSeq.flatMap { _.tile.bands } }
          )
        }

    ContextRDD(tiledRDD, layerMetadata)
  }

  override def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null,
                             zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerRDD[SpaceTimeKey] =
    readMultibandTileLayer(from, to, boundingBox, zoom, sc).withContext(_.mapValues(_.band(0))) // TODO: clean up

  override def readMetadata(zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    val datedRasterSources = queryAll()

    val (minDate, _) = datedRasterSources.head
    val (maxDate, newestRasterSource) = datedRasterSources.last

    val maxZoom = layoutScheme.levelFor(newestRasterSource.extent, newestRasterSource.cellSize).zoom
    val layout = layoutScheme.levelForZoom(zoom min maxZoom).layout

    TileLayerMetadata(
      cellType = newestRasterSource.cellType, // intentional: 2014 has empty NetCDFs
      layout,
      extent = newestRasterSource.extent,
      crs = newestRasterSource.crs,
      bounds = KeyBounds(
        SpaceTimeKey(0, 0, minDate),
        SpaceTimeKey(layout.layoutCols - 1, layout.layoutRows - 1, maxDate)
      )
    )
  }

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) = loadMetadata(sc).get

  override def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])] = {
    val datedRasterSources = queryAll()

    datedRasterSources
      .lastOption // intentional: 2014 has empty NetCDFs
      .map { case (_, rasterSource) =>
        val dates = datedRasterSources.map { case (date, _) => date }
        (ProjectedExtent(rasterSource.extent, rasterSource.crs), dates)
      }
  }
}
