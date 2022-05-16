package org.openeo.geotrellissentinelhub

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, DatacubeSupport, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner}
import org.openeo.geotrellissentinelhub.SampleType.{SampleType, UINT16}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.JavaConverters._

object PyramidFactory {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PyramidFactory])


  private val maxKeysPerPartition = 20

  // convenience methods for Python client
  def withGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                              clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                              maxSpatialResolution: CellSize, softErrors: Boolean): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      new MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
      processingOptions, sampleType, new RlGuardAdapter, maxSpatialResolution, softErrors)

  def withoutGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                                 clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                                 maxSpatialResolution: CellSize, softErrors: Boolean): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      new MemoizedRlGuardAdapterCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
      processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution, softErrors = softErrors)
}

class PyramidFactory(collectionId: String, datasetId: String, catalogApi: CatalogApi, processApi: ProcessApi,
                     authorizer: Authorizer,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16,
                     rateLimitingGuard: RateLimitingGuard = NoRateLimitingGuard,
                     maxSpatialResolution: CellSize = CellSize(10,10), softErrors: Boolean = false) extends Serializable {
  import PyramidFactory._

  @transient private val _catalogApi = if (collectionId == null) new MadeToMeasureCatalogApi else catalogApi

  private val maxZoom = 14

  private def authorized[R](fn: String => R): R = authorizer.authorized(fn)

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom,
                    bandNames: Seq[String], metadataProperties: util.Map[String, util.Map[String, Any]],
                    features: collection.Map[String, Feature[Geometry, ZonedDateTime]])(implicit sc: SparkContext):
  MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)

    val scheme = ZoomedLayoutScheme(targetCrs)
    val metadata: TileLayerMetadata[SpaceTimeKey] =
      DatacubeSupport.layerMetadata(reprojectedBoundingBox,from,to,zoom,sampleType.cellType,scheme,maxSpatialResolution)
    val layout = metadata.layout

    val reprojectedGeometry = reprojectedBoundingBox.extent.toPolygon()

    val overlappingKeys = {
      val intersectingFeatureKeys = for {
        (_, Feature(geom, datetime)) <- features.toSet
        date = datetime.toLocalDate.atStartOfDay(UTC)
        reprojectedGeom = geom.reproject(LatLng, reprojectedBoundingBox.crs)
        SpatialKey(col, row) <- layout.mapTransform.keysForGeometry(reprojectedGeom intersection reprojectedGeometry)
      } yield SpaceTimeKey(col, row, date)

      intersectingFeatureKeys
        .toSeq
    }

    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)

    val tracker = BatchJobMetadataTracker.tracker("")
    tracker.registerDoubleCounter(BatchJobMetadataTracker.SH_PU)

    val tilesRdd = sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
      .map { key =>
        val width = layout.tileLayout.tileCols
        val height = layout.tileLayout.tileRows

        awaitRateLimitingGuardDelay(bandNames, width, height)

        val (tile, processingUnitsSpent) = authorized { accessToken =>
          processApi.getTile(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
            key.temporalKey, width, height, bandNames, sampleType, Criteria.toDataFilters(metadataProperties),
            processingOptions, accessToken)
        }
        tracker.add(BatchJobMetadataTracker.SH_PU, processingUnitsSpent)

        (key, tile)
      }
      .filter(_._2.bands.exists(b => !b.isNoDataTile))
      .partitionBy(partitioner)

    ContextRDD(tilesRdd, metadata)
  }

  private def atEndOfDay(to: ZonedDateTime): ZonedDateTime = {
    val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
    to.toLocalDate.atTime(endOfDay).toZonedDateTime
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
              metadataProperties: util.Map[String, util.Map[String, Any]])(implicit sc: SparkContext):
  Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val (polygon, polygonCrs) = (boundingBox.extent.toPolygon(), boundingBox.crs)

    val features = authorized { accessToken =>
      _catalogApi.search(collectionId, polygon, polygonCrs,
        from, atEndOfDay(to), accessToken, Criteria.toQueryProperties(metadataProperties))
    }

    val layers = for (zoom <- maxZoom to 0 by -1)
      yield zoom -> layer(boundingBox, from, to, zoom, bandNames, metadataProperties, features)

    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_names: util.List[String],
                  metadata_properties: util.Map[String, util.Map[String, Any]]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to, band_names.asScala, metadata_properties).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = datacube_seq(polygons, polygons_crs, from_date, to_date,
    band_names, metadata_properties, new DataCubeParameters)

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]],
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

      val scheme = FloatingLayoutScheme(256)

      val multiple_polygons_flag = polygons.length > 1
      val metadata = DatacubeSupport.layerMetadata(
        boundingBox, from, to, 0, sampleType.cellType, scheme, maxSpatialResolution,
        dataCubeParameters.globalExtent, multiple_polygons_flag
      )
      val layout = metadata.layout

      val tracker = BatchJobMetadataTracker.tracker("")
      tracker.registerDoubleCounter(BatchJobMetadataTracker.SH_PU)

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        //noinspection ComparingUnrelatedTypes
        val maskClouds = dataCubeParameters.maskingStrategyParameters.get("method") == "mask_scl_dilation"

        def loadMasked(spatialKey: SpatialKey, dateTime: ZonedDateTime): Option[MultibandTile] = try {
          def getTile(bandNames: Seq[String], projectedExtent: ProjectedExtent, width: Int, height: Int): MultibandTile = {
            awaitRateLimitingGuardDelay(bandNames, width, height)

            val (tile, processingUnitsSpent) = authorized { accessToken =>
              processApi.getTile(datasetId, projectedExtent, dateTime, width, height, bandNames,
                sampleType, Criteria.toDataFilters(metadata_properties), processingOptions, accessToken)
            }
            tracker.add(BatchJobMetadataTracker.SH_PU, processingUnitsSpent)
            tile
          }

          val keyExtent = spatialKey.extent(layout)

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
        } catch {
          case e @ SentinelHubException(_, _, responseBody) if softErrors =>
            logger.warn(s"ignoring soft error $responseBody", e)
            None
        }

        val tilesRdd =
          if (datasetId == "dem") {
            val overlappingKeys = layout.mapTransform.keysForGeometry(GeometryCollection(polygons)).toSeq

            val keysRdd = sc.parallelize(overlappingKeys)

            val tilesRdd = keysRdd
              .flatMap { spatialKey =>
                // "dem" data doesn't have a time dimension so the actual timestamp doesn't matter
                loadMasked(spatialKey, ZonedDateTime.of(1981, 4, 24, 2, 0, 0, 0, UTC))
                  .map(tile => spatialKey -> tile)
              }
              .flatMap { case (spatialKey, tile) =>
                if (!tile.bands.forall(_.isNoDataTile))
                  sequentialDays(from, to)
                    .map(day => SpaceTimeKey(spatialKey.col, spatialKey.row, day.toLocalDate.atStartOfDay(UTC)) -> tile)
                else
                  Stream.empty
              }

            DatacubeSupport.applyDataMask(Some(dataCubeParameters), tilesRdd)
          } else {
            val overlappingKeys = {
              val multiPolygon = simplify(polygons)

              val features = authorized { accessToken =>
                _catalogApi.search(collectionId, multiPolygon, polygons_crs,
                  from, atEndOfDay(to), accessToken, Criteria.toQueryProperties(metadata_properties))
              }

              val intersectingFeatureKeys = for {
                (_, Feature(geom, datetime)) <- features.toSet
                date = datetime.toLocalDate.atStartOfDay(UTC)
                reprojectedGeom = geom.reproject(LatLng, boundingBox.crs)
                SpatialKey(col, row) <- layout.mapTransform.keysForGeometry(reprojectedGeom intersection multiPolygon)
              } yield SpaceTimeKey(col, row, date)

              intersectingFeatureKeys
                .toSeq
            }

            val requiredKeysRdd = sc.parallelize(overlappingKeys)
            val partitioner = DatacubeSupport.createPartitioner(Some(dataCubeParameters), requiredKeysRdd, metadata)
            logger.info(s"Created Sentinelhub datacube with ${overlappingKeys.size} keys and metadata ${metadata} and ${partitioner.get}")

            var keysRdd = requiredKeysRdd.map((_,Option.empty)).partitionBy(partitioner.get)

            keysRdd = DatacubeSupport.applyDataMask(Some(dataCubeParameters),keysRdd)

            val tilesRdd: RDD[(SpaceTimeKey,MultibandTile)] = keysRdd
              .mapPartitions(_.map { case (spaceTimeKey, _) => (spaceTimeKey, loadMasked(spaceTimeKey.spatialKey, spaceTimeKey.time)) }, preservesPartitioning = true)
              .filter {case (_: SpaceTimeKey, tile: Option[MultibandTile]) => tile.isDefined && !tile.get.bands.forall(_.isNoDataTile) }
              .mapValues(t => t.get)

            tilesRdd
          }

        tilesRdd.name = s"Sentinelhub-$collectionId"
        tilesRdd
      }

      val cRDD = ContextRDD(tilesRdd, metadata)
      cRDD.name = tilesRdd.name
      cRDD
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

    if (logger.isDebugEnabled) logger.debug(s"$rateLimitingGuard says to wait $delay")
    else if (!delay.isZero) logger.info(s"$rateLimitingGuard says to wait $delay")

    MILLISECONDS.sleep(delay.toMillis)
  }
}
