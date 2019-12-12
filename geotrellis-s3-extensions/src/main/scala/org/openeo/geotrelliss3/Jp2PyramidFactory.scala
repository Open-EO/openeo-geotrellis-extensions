package org.openeo.geotrelliss3

import java.lang.System.getenv
import java.net.URI
import java.time._

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.gdal.config.GDALOptionsConfig.registerOption
import geotrellis.raster.{MultibandTile, Tile, UByteUserDefinedNoDataCellType, isNoData}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.LayerId
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.math.max

object Jp2PyramidFactory {
  private val crs = WebMercator
  private val layoutScheme = ZoomedLayoutScheme(crs, 256)
  private val layerName = "S3"
  private val maxZoom = 14
  private val catalog = Catalog("Sentinel-2", "Level-2A")

  sealed trait Band
  case object B02 extends Band
  case object B03 extends Band
  case object B04 extends Band
  case object B08 extends Band

  private val allBands: Seq[Band] = Seq(B02, B03, B04, B08)

  private implicit val dateOrdering: Ordering[ZonedDateTime] = new Ordering[ZonedDateTime] {
    override def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.withZoneSameInstant(ZoneId.of("UTC")) compareTo b.withZoneSameInstant(ZoneId.of("UTC"))
  }

  private def getS3Client(endpoint: String, region: String): S3Client = {
    S3Client.builder()
      .endpointOverride(new URI(endpoint))
      .region(Region.of(region))
      .build()
  }
}

class Jp2PyramidFactory(endpoint: String, region: String) extends Serializable {

  import Jp2PyramidFactory._

  registerOption("AWS_S3_ENDPOINT", URI.create(endpoint).getAuthority)
  registerOption("AWS_SECRET_ACCESS_KEY", getenv("AWS_SECRET_ACCESS_KEY"))
  registerOption("AWS_ACCESS_KEY_ID", getenv("AWS_ACCESS_KEY_ID"))

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

  private def getS3Client: S3Client = Jp2PyramidFactory.getS3Client(endpoint, region)

  private def listProducts(bucket: String, key: String) = {
    val request = ListObjectsRequest.builder().bucket(bucket).prefix(key).build()

    val keyPattern = raw".*S2._MSIL2A_\d{8}T.*B\d{2}_10m\.jp2$$".r

    getS3Client
      .listObjects(request)
      .contents()
      .asScala
      .map(_.key())
      .flatMap(key => key match {
        case keyPattern(_*) => Some(key)
        case _ => None
      })
      .toSeq
  }

  private val uri = ((bucket: String, key: String) => s"/vsis3/$bucket/$key").tupled

  private def extent(productKeys: Seq[(String, String)]) = {
    productKeys
      .map(key => GDALRasterSource(uri(key)).reproject(crs))
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
    tiles.map(_.toArrayTile()).reduceOption[Tile](_.combine(_)((t1, t2) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)))
  }

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, bands: Seq[Band] = allBands): MultibandTileLayerRDD[SpaceTimeKey] = {
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

    val catalogEntries = catalog.query(from, to, polygon = boundingBox.reproject(LatLng).toPolygon())

    val productKeys = catalogEntries.flatMap(e => listProducts(e.getS3bucket, e.getS3Key).map((e.getS3bucket, _)))

    def extractDate(key: String): ZonedDateTime = {
      val date = raw"S2._MSIL2A_(\d{4})(\d{2})(\d{2})T".r.unanchored
      key match {
        case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      }
    }

    val bandFileMaps = bands.map(b =>
      productKeys.filter(pk => pk._2.contains(s"${b.toString}_10m.jp2"))
        .map(pk => extractDate(pk._2) -> pk)
        .groupBy(_._1)
        .map { case (k, v) => (k, v.map(_._2)) }
    )

    val tiles = sc.parallelize(overlappingKeys)
      .map(key => (key, bandFileMaps
        .flatMap(_.get(key.time))
        .map(_.map(s3Key => GDALRasterSource(uri(s3Key)).reproject(crs).tileToLayout(layout))
          .flatMap(_.rasterRegionForKey(key.spatialKey).flatMap(_.raster))
          .map(_.tile.band(0)))
        .flatMap(mapToSingleTile(_))))
      .flatMapValues(v => if (v.isEmpty) None else Some(MultibandTile(v)))

    val metadata = tileLayerMetadata(layout, extent(productKeys), from, to)

    ContextRDD(tiles, metadata)
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[Band] = allBands): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, bands)
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[Band] =
      if (band_indices == null) allBands
      else band_indices.asScala.map(allBands(_)).toSeq

    pyramid(projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

}
