package org.openeo.geotrelliss3

import java.net.URI
import java.time.{LocalDate, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{MultibandTile, Raster, UByteUserDefinedNoDataCellType}
import geotrellis.spark.io.hadoop.geotiff.InMemoryGeoTiffAttributeStore
import geotrellis.spark.io.s3.geotiff.{S3GeoTiffLayerReader, S3IMGeoTiffAttributeStore}
import geotrellis.spark.io.s3.{AmazonS3Client, S3Client}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{LayoutDefinition, LayoutLevel, ZoomedLayoutScheme}
import geotrellis.spark.{ContextRDD, KeyBounds, LayerId, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

object PyramidFactory {
  private val crs = LatLng
  private val layoutScheme = ZoomedLayoutScheme(crs, 256)
  private val layerName = "S3"
  private val maxZoom = 14

  private def getS3Client(endpoint: String, region: String): S3Client = {
    val s3builder: AmazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))

    AmazonS3Client(s3builder)
  }

  private implicit val dateOrdering: Ordering[ZonedDateTime] = new Ordering[ZonedDateTime] {
    override def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.withZoneSameInstant(ZoneId.of("UTC")) compareTo b.withZoneSameInstant(ZoneId.of("UTC"))
  }

  implicit class WithExtent(attributeStore: InMemoryGeoTiffAttributeStore) {
    lazy val projectedExtent: Option[ProjectedExtent] = {
      attributeStore.metadataList.foldLeft(None: Option[ProjectedExtent]) { (fullExtent, md) =>
        val geoTiffExtent = ProjectedExtent(md.projectedExtent.reproject(crs), crs)

        fullExtent match {
          case None => Some(geoTiffExtent)
          case Some(incompleteExtent) => Some(ProjectedExtent(incompleteExtent.extent combine geoTiffExtent.extent, crs))
        }
      }
    }
  }
}

class PyramidFactory(endpoint: String, region: String, bucketName: String) {
  import PyramidFactory._

  private val s3Uri = URI.create(s"s3://$bucketName")

  private def getS3Client: S3Client = PyramidFactory.getS3Client(endpoint, region)

  private def listBlobKeys(s3Uri: URI, keyPattern: Regex): Seq[String] = {
    val uri = new AmazonS3URI(s3Uri)

    val request = (new ListObjectsRequest)
      .withBucketName(uri.getBucket)
      .withPrefix(uri.getKey)

    getS3Client
      .listKeys(request)
      .flatMap(key => key match {
        case keyPattern(_*) => Some(key)
        case _ => None
      })
  }

  private def dates: Seq[ZonedDateTime] = {
    val date = raw"S2._MSIL1C_(\d{4})(\d{2})(\d{2})T".r.unanchored
    def extractDate(geoTiff: String): ZonedDateTime =
      geoTiff match { case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC) }

    val allGeoTiffs = listBlobKeys(s3Uri, dataKeyPattern(None))

    allGeoTiffs
      .map(extractDate)
      .sorted
  }

  private def dataKeyPattern(date: Option[ZonedDateTime]): Regex = {
    val datePattern = date match {
      case Some(date) => f"${date.getYear}${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
      case _ => raw"\d{8}"
    }

    raw".*S2._MSIL1C_${datePattern}T.*\.tiff$$".r
  }

  private lazy val attributeStores: RDD[(ZonedDateTime, InMemoryGeoTiffAttributeStore)] = {
    val sc = SparkContext.getOrCreate()

    val keyPatternsPerDay = dates.map(date => (date, dataKeyPattern(Some(date))))
    val keyPatterns: RDD[(ZonedDateTime, Regex)] = sc.parallelize(keyPatternsPerDay, keyPatternsPerDay.size)

    val s3Uri = this.s3Uri

    val endpoint = this.endpoint
    val region = this.region

    keyPatterns
      .mapValues(pattern =>
        S3IMGeoTiffAttributeStore(
          layerName,
          s3Uri,
          pattern.regex,
          recursive = true,
          () => PyramidFactory.getS3Client(endpoint, region)
        )
      )
  }

  private def layer(zoom: Int, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): MultibandTileLayerRDD[SpaceTimeKey] = {
    val reprojectedBoundingBox = boundingBox.reproject(crs)

    val layerId = LayerId(layerName, zoom)
    val LayoutLevel(_, layout) = layoutScheme.levelForZoom(layerId.zoom)

    val endpoint = this.endpoint
    val region = this.region

    val getS3Client = () => PyramidFactory.getS3Client(endpoint, region)

    val tiles: RDD[(SpaceTimeKey, MultibandTile)] = attributeStores.cache()
      .filterByRange(from, to)
      /*.filter { case (_, attributeStore) => attributeStore.projectedExtent match {
        case Some(ProjectedExtent(extent, _)) => extent intersects reprojectedBoundingBox
        case None => false
      }}*/
      .flatMap { case (date, attributeStore) =>
        val reader = S3GeoTiffLayerReader(attributeStore, layoutScheme, getS3Client = getS3Client)

        try {
          for {
            SpatialKey(col, row) <- layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon())
            key = SpaceTimeKey(col, row, date)
            Raster(tile, _) <- try {
              Some(reader.read[MultibandTile](layerId)(col, row))
            } catch {
              case e: UnsupportedOperationException if e.getMessage == "empty.reduceLeft" => None
            }
          } yield key -> tile
        } finally {
          reader.shutdown
        }
    }

    val projectedExtent: Option[ProjectedExtent] = attributeStores
      .map { case (_, attributeStore) => attributeStore.projectedExtent }
      .fold(None)((e1, e2) => (e1, e2) match {
        case (None, None) => None
        case (e @ Some(_), None) => e
        case (None, e @ Some(_)) => e
        case (Some(e1), Some(e2)) => Some(ProjectedExtent(e1.extent combine e2.extent, e1.crs))
      })

    val metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(layout, projectedExtent.get, from, to)

    ContextRDD(tiles, metadata)
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(zoom, boundingBox, from, to)
    Pyramid(layers.toMap)
  }

  private def tileLayerMetadata(layout: LayoutDefinition, projectedExtent: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): TileLayerMetadata[SpaceTimeKey] = {
    val gridBounds = layout.mapTransform.extentToBounds(projectedExtent.extent)

    TileLayerMetadata(
      UByteUserDefinedNoDataCellType(255.asInstanceOf[Byte]),
      layout,
      projectedExtent.extent,
      projectedExtent.crs,
      KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
    )
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
