package org.openeo.geotrellis.layers

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit.HOURS

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.layer.{Boundable, KeyBounds, KeyExtractor, LayoutDefinition, SpaceTimeKey, SpatialKey, TemporalKeyExtractor, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{MultibandTile, RasterRegion, RasterSource, SourceName, SourcePath}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.util._
import geotrellis.vector._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.openeo.geotrellis.ProjectedPolygons

import scala.util.matching.Regex

object AbstractGlobFileLayerProvider {
  private val crs = LatLng
  private val layoutScheme = ZoomedLayoutScheme(crs, 256)

  protected def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
  }

  private val pathsCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, HOURS)
    .build(new CacheLoader[String, List[Path]] {
      override def load(dataGlob: String): List[Path] =
        HdfsUtils.listFiles(new Path(s"file:$dataGlob"), new Configuration)
    })
}

abstract class AbstractGlobFileLayerProvider extends LayerProvider {
  import AbstractGlobFileLayerProvider._

  lazy val maxZoom: Int = {
    val (_, newestRasterSource) = queryAll().last
    layoutScheme.levelFor(newestRasterSource.extent, newestRasterSource.cellSize).zoom
  }

  protected def dataGlob: String
  protected def dateRegex: Regex
  protected def queryAll(): Array[(ZonedDateTime, RasterSource)]

  protected val crs: CRS = AbstractGlobFileLayerProvider.crs
  protected val deriveDate: (String, Regex) => ZonedDateTime = AbstractGlobFileLayerProvider.deriveDate

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null,
                                      zoom: Int = Int.MaxValue, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val projectedPolygons = ProjectedPolygons(Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs)
    readMultibandTileLayer(from, to, projectedPolygons, zoom, sc)
  }

  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, projectedPolygons: ProjectedPolygons, zoom: Int,
                             sc: SparkContext)
  : MultibandTileLayerRDD[SpaceTimeKey] = {
    val rasterSources = query(from, to)

    if (rasterSources.isEmpty) throw new IllegalArgumentException("no fitting raster sources found")

    val sources = sc.parallelize(rasterSources)

    val dateRegex = this.dateRegex
    val parseTime: SourceName => ZonedDateTime =
      sourceName => AbstractGlobFileLayerProvider.deriveDate(sourceName.asInstanceOf[SourcePath].value, dateRegex)
    val keyExtractor = TemporalKeyExtractor.fromPath(parseTime)

    val layout = layoutScheme.levelForZoom(zoom min maxZoom).layout
    val reprojectedPolygons = projectedPolygons.polygons.map(_.reproject(projectedPolygons.crs, crs))

    implicit val _sc: SparkContext = sc // TODO: clean up

    tiledLayerRDD(
      sources,
      layout,
      keyExtractor,
      reprojectedPolygons,
      from,
      to
    )
  }

  private def query(from: ZonedDateTime, to: ZonedDateTime): Array[RasterSource] = {
    queryAll()
      .dropWhile { case (date, _) => date isBefore from }
      .takeWhile  { case (date, _) => !(date isAfter to) }
      .map { case (_, rasterSource) => rasterSource }
  }

  protected def paths: List[Path] = pathsCache.get(dataGlob)

  private def tiledLayerRDD[ M: Boundable](
                                                                                     sources: RDD[RasterSource],
                                                                                     layout: LayoutDefinition,
                                                                                     keyExtractor: KeyExtractor.Aux[SpaceTimeKey, M],
                                                                                     polygons: Seq[MultiPolygon],
                                                                                     from:ZonedDateTime,
                                                                                     to:ZonedDateTime,
                                                                                     rasterSummary: Option[RasterSummary[M]] = None,
                                                                                     partitioner: Option[Partitioner] = None
                                                                                   )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {

    val polygonsExtent = polygons.extent // TODO: can be done on Spark too

    val gridBounds = layout.mapTransform.extentToBounds(polygonsExtent)
    val cellType = sources.take(1).head.cellType
    val layerMetadata = TileLayerMetadata(
      cellType,
      layout,
      polygonsExtent,
      crs,
      KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
    )



    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => SpaceTimeKey = { sk => keyExtractor.getKey(m, sk) }
        rs.tileToLayout(layout, tileKeyTransform)
      }


    val requiredKeys = sc.parallelize(polygons).clipToGrid(layout).groupByKey()

    val rasterRegionRDD: RDD[(SpaceTimeKey, RasterRegion)] =
      tiledLayoutSourceRDD.flatMap { tiledLayoutSource =>
        tiledLayoutSource
          .keyedRasterRegions()
          .filter { case (key, _) =>
            val keyExtent = key.getComponent[SpatialKey].extent(layout)
            keyExtent intersects polygonsExtent
          }
      }

    val spatiallyKeyedRasterRegionRDD: RDD[(SpatialKey, (SpaceTimeKey, RasterRegion))] = rasterRegionRDD
      .map { case keyedRasterRegion @ (key, _) => (key.getComponent[SpatialKey], keyedRasterRegion ) }

    val filteredRdd = spatiallyKeyedRasterRegionRDD.join(requiredKeys)
      .mapValues { case (keyedRasterRegion, _) => keyedRasterRegion }
      .values


    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      filteredRdd
        .groupByKey(partitioner.getOrElse(SpacePartitioner(layerMetadata.bounds)))
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
