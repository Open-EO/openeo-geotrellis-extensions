package org.openeo.geotrellissentinelhub

import com.google.common.cache.{CacheBuilder, CacheLoader}
import geotrellis.layer.{KeyBounds, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.proj4j.proj.TransverseMercatorProjection
import org.openeo.geotrelliscommon.{DataCubeParameters, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner}
import org.openeo.geotrellissentinelhub.SampleType.{SampleType, UINT16}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS
import scala.collection.JavaConverters._

object PyramidFactory {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[PyramidFactory])

  private val maxKeysPerPartition = 20

  // TODO: invalidate key on 401 Unauthorized
  private val authTokenCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1800L, SECONDS)
    .build(new CacheLoader[(String, String), String] {
      override def load(credentials: (String, String)): String = credentials match {
        case (clientId, clientSecret) =>
          withRetries(5, clientId) { new AuthApi().authenticate(clientId, clientSecret).access_token }
      }
    })

  // TODO: put this in a central place
  private implicit object ZonedDateTimeOrdering extends Ordering[ZonedDateTime] {
    override def compare(x: ZonedDateTime, y: ZonedDateTime): Int = x compareTo y
  }

  // convenience method for Python client
  // TODO: change name? A 429 response makes it rate-limited anyway.
  def rateLimited(endpoint: String, collectionId: String, datasetId: String, clientId: String, clientSecret: String,
                  processingOptions: util.Map[String, Any], sampleType: SampleType, maxSpatialResolution: CellSize = CellSize(10,10)): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      clientId, clientSecret, processingOptions, sampleType, new RlGuardAdapter, maxSpatialResolution)

  @deprecated("include a collectionId")
  def rateLimited(endpoint: String, datasetId: String, clientId: String, clientSecret: String,
                  processingOptions: util.Map[String, Any], sampleType: SampleType, maxSpatialResolution: CellSize = CellSize(10,10)): PyramidFactory =
    rateLimited(endpoint, collectionId = null, datasetId, clientId, clientSecret, processingOptions, sampleType, maxSpatialResolution)
}

class PyramidFactory(collectionId: String, datasetId: String, @transient catalogApi: CatalogApi, processApi: ProcessApi,
                     clientId: String, clientSecret: String,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16,
                     rateLimitingGuard: RateLimitingGuard = NoRateLimitingGuard,
                     maxSpatialResolution: CellSize) extends Serializable {
  import PyramidFactory._

  private val maxZoom = 14

  private def accessToken: String = authTokenCache.get((clientId, clientSecret))

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom,
                    bandNames: Seq[String], metadataProperties: util.Map[String, Any],
                    dates: Seq[ZonedDateTime])(implicit sc: SparkContext):
  MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val overlappingKeys = for {
      date <- dates
      spatialKey <- layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon())
    } yield SpaceTimeKey(spatialKey, date)

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox)

      TileLayerMetadata(
        cellType = sampleType.cellType,
        layout = layout,
        extent = reprojectedBoundingBox,
        crs = targetCrs,
        KeyBounds(
          SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from),
          SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to)
        )
      )
    }

    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)

    val tilesRdd = sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
      .map { key =>
        val width = layout.tileLayout.tileCols
        val height = layout.tileLayout.tileRows

        awaitRateLimitingGuardDelay(bandNames, width, height)

        val tile = processApi.getTile(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
          key.temporalKey, width, height, bandNames, sampleType, metadataProperties, processingOptions, accessToken)

        (key, tile)
      }
      .filter(_._2.bands.exists(b => !b.isNoDataTile))
      .partitionBy(partitioner)

    ContextRDD(tilesRdd, metadata)
  }

  private def dates(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                    metadataProperties: util.Map[String, Any]): Seq[ZonedDateTime] = {
    def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

    def atEndOfDay(to: ZonedDateTime): ZonedDateTime = {
      val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
      to.toLocalDate.atTime(endOfDay).toZonedDateTime
    }

    if (collectionId == null)
      sequentialDates(from)
        .takeWhile(date => !(date isAfter to))
    else
      catalogApi
        .dateTimes(collectionId, boundingBox, from, atEndOfDay(to), accessToken, mapDataFilters(metadataProperties))
        .map(_.toLocalDate.atStartOfDay(UTC))
        .distinct // ProcessApi::getTile takes care of [day, day + 1] interval and mosaicking therein
        .sorted
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
              metadataProperties: util.Map[String, Any])(implicit sc: SparkContext):
  Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val dates = this.dates(boundingBox, from, to, metadataProperties)

    val layers = for (zoom <- maxZoom to 0 by -1)
      yield zoom -> layer(boundingBox, from, to, zoom, bandNames, metadataProperties, dates)

    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_names: util.List[String],
                  metadata_properties: util.Map[String, Any]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to, band_names.asScala, metadata_properties).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, Any]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = datacube_seq(polygons, polygons_crs, from_date, to_date,
    band_names, metadata_properties, new DataCubeParameters)

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, Any],
                   dataCubeParameters: DataCubeParameters):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: use ProjectedPolygons type
    // TODO: reduce code duplication with pyramid_seq()

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = {
      implicit val sc: SparkContext = SparkContext.getOrCreate()

      val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)

      val from = ZonedDateTime.parse(from_date)
      val to = ZonedDateTime.parse(to_date)

      // TODO: call into AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

      val layout = this.layout(FloatingLayoutScheme(256), boundingBox)

      val metadata: TileLayerMetadata[SpaceTimeKey] = {
        val gridBounds = layout.mapTransform.extentToBounds(boundingBox.extent)

        TileLayerMetadata(
          cellType = sampleType.cellType,
          layout,
          extent = boundingBox.extent,
          crs = boundingBox.crs,
          bounds = KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
        )
      }

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        val overlappingKeys = for {
          date <- dates(boundingBox, from, to, metadata_properties)
          spatialKey <- layout.mapTransform.keysForGeometry(GeometryCollection(polygons))
        } yield SpaceTimeKey(spatialKey, date)

        val maskClouds = dataCubeParameters.maskingStrategyParameters.get("method") == "mask_scl_dilation" // TODO: what's up with this warning in Idea?

        def loadMasked(key: SpaceTimeKey): Option[MultibandTile] = {
          def getTile(bandNames: Seq[String], projectedExtent: ProjectedExtent, width: Int, height: Int): MultibandTile = {
            awaitRateLimitingGuardDelay(bandNames, width, height)

            processApi.getTile(datasetId, projectedExtent, key.temporalKey, width, height, bandNames,
              sampleType, metadata_properties, processingOptions, accessToken)
          }

          val keyExtent = key.spatialKey.extent(layout)

          def dataTile: MultibandTile = getTile(band_names.asScala, ProjectedExtent(keyExtent, boundingBox.crs),
            width = layout.tileLayout.tileCols, height = layout.tileLayout.tileRows)

          if (maskClouds) {
            val cloudFilterStrategy = {
              val sclBandIndex = band_names.asScala.indexWhere { bandName =>
                bandName.contains("SCENECLASSIFICATION") || bandName.contains("SCL")
              }

              if (sclBandIndex >= 0) new SCLConvolutionFilterStrategy(sclBandIndex)
              else NoCloudFilterStrategy
            }

            cloudFilterStrategy.loadMasked(new MaskTileLoader {
              override def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] = Some {
                val bufferedWidth = layout.tileLayout.tileCols + 2 * bufferInPixels
                val bufferedHeight = layout.tileLayout.tileRows + 2 * bufferInPixels
                val bufferedExtent = keyExtent.expandBy(bufferInPixels * layout.cellwidth, bufferInPixels * layout.cellheight)

                val maskTile = getTile(Seq(band_names.get(sclBandIndex)), ProjectedExtent(bufferedExtent, boundingBox.crs), bufferedWidth, bufferedHeight)
                Raster(maskTile, bufferedExtent)
              }

              override def loadData: Option[MultibandTile] = Some(dataTile)
            })
          } else Some(dataTile)
        }

        val tilesRdd = for {
          key <- sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
          tile <- loadMasked(key)
          if !tile.bands.forall(_.isNoDataTile)
        } yield (key, tile)

        val partitioner = SpacePartitioner(metadata.bounds)
        assert(partitioner.index == SpaceTimeByMonthPartitioner)

        tilesRdd.partitionBy(partitioner)
      }

      ContextRDD(tilesRdd, metadata)
    }

    Seq(0 -> cube)
  }

  private def awaitRateLimitingGuardDelay(bandNames: Seq[String], width: Int, height: Int): Unit = {
    val delay = rateLimitingGuard.delay(
      batchProcessing = false,
      width, height,
      bandNames.count(_ != "dataMask"),
      outputFormat = "tiff32",
      nDataSamples = bandNames.size,
      s1Orthorectification = false
    )

    logger.debug(s"$rateLimitingGuard says to wait $delay")
    TimeUnit.MILLISECONDS.sleep(delay.toMillis)
  }

  private def layout(layoutScheme: FloatingLayoutScheme, boundingBox: ProjectedExtent): LayoutDefinition = {
    //Giving the layout a deterministic extent simplifies merging of data with spatial partitioner
    val layoutExtent =
      if (boundingBox.crs.proj4jCrs.getProjection.getName == "utm") {
        //for utm, we return an extent that goes beyound the utm zone bounds, to avoid negative spatial keys
        if (boundingBox.crs.proj4jCrs.getProjection.asInstanceOf[TransverseMercatorProjection].getSouthernHemisphere)
        //official extent: Extent(166021.4431, 1116915.0440, 833978.5569, 10000000.0000) -> round to 10m + extend
          Extent(0.0, 1000000.0, 833970.0 + 100000.0, 10000000.0000 + 100000.0)
        else {
          //official extent: Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825) -> round to 10m + extend
          Extent(0.0, -1000000.0000, 833970.0 + 100000.0, 9329000.0 + 100000.0)
        }
      } else {
        boundingBox.extent
      }

    layoutScheme.levelFor(layoutExtent, maxSpatialResolution).layout
  }
}
