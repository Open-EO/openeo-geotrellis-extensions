package org.openeo.geotrelliss3

import java.nio.file.{Files, Paths}
import java.time._

import geotrellis.layer._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.{MultibandTile, Tile, UByteUserDefinedNoDataCellType, isNoData}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.LayerId
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.math.max

object CreoPyramidFactory {
  private val crs = WebMercator
  private val layoutScheme = ZoomedLayoutScheme(crs, 256)
  private val layerName = "S3"
  private val maxZoom = 14

  private implicit val dateOrdering: Ordering[ZonedDateTime] = new Ordering[ZonedDateTime] {
    override def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.withZoneSameInstant(ZoneId.of("UTC")) compareTo b.withZoneSameInstant(ZoneId.of("UTC"))
  }
}

class CreoPyramidFactory(productPaths: Seq[String], bands: Seq[String]) extends Serializable {

  import CreoPyramidFactory._

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

  private def listProducts(productPath: String) = {
    val keyPattern = raw".*\.jp2".r

    Files.walk(Paths.get(productPath)).iterator().asScala
      .map(p => p.toString)
      .flatMap(key => key match {
        case keyPattern(_*) => Some(key)
        case _ => None
      })
      .toSeq
  }

  private def extent(productPaths: Seq[String]) = {
    productPaths
      .map(key => GDALRasterSource(key).reproject(crs))
      .map(_.extent)
      .reduceOption((e1, e2) => e1 combine e2)
      .getOrElse(Extent(0, 0, 0, 0))
  }

  private def tileLayerMetadata(layout: LayoutDefinition, extent: Extent, from: ZonedDateTime, to: ZonedDateTime): TileLayerMetadata[SpaceTimeKey] = {
    val gridBounds = layout.mapTransform.extentToBounds(extent)

    TileLayerMetadata(
      UByteUserDefinedNoDataCellType(255.asInstanceOf[Byte]),
      layout,
      extent,
      crs,
      KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
    )
  }

  private def mapToSingleTile(tiles: Iterable[Tile]): Option[Tile] = {
    val intCombine = (t1: Int, t2: Int) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)
    val doubleCombine = (t1: Double, t2: Double) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)

    tiles.map(_.toArrayTile()).reduceOption[Tile](_.dualCombine(_)(intCombine)(doubleCombine))
  }

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val sc: SparkContext = SparkContext.getOrCreate()

    val reprojectedBoundingBox = boundingBox.reproject(crs)

    val layerId = LayerId(layerName, zoom)
    val LayoutLevel(_, layout) = layoutScheme.levelForZoom(layerId.zoom)

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = dates.flatMap(date =>
      layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon())
        .map(key => SpaceTimeKey(key, date)))

    val productKeys = productPaths.flatMap(listProducts)

    def extractDate(key: String): ZonedDateTime = {
      val date = raw"\/(\d{4})\/(\d{2})\/(\d{2})\/".r.unanchored
      key match {
        case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      }
    }

    val bandFileMaps = bands.map(b =>
      productKeys.filter(_.contains(b))
        .map(pk => extractDate(pk) -> pk)
        .groupBy(_._1)
        .map { case (k, v) => (k, v.map(_._2)) }
    )

    val tiles = sc.parallelize(overlappingKeys)
      .map(key => (key, bandFileMaps
        .flatMap(_.get(key.time))
        .map(_.map(path => GDALRasterSource(path).reproject(crs).tileToLayout(layout))
          .flatMap(_.rasterRegionForKey(key.spatialKey).flatMap(_.raster))
          .map(_.tile.band(0)))
        .flatMap(mapToSingleTile(_))))
      .flatMapValues(v => if (v.isEmpty) None else Some(MultibandTile(v)))

    val metadata = tileLayerMetadata(layout, extent(productKeys.filter(_.contains(bands.head))), from, to)

    ContextRDD(tiles, metadata)
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom)
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

}
