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
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrelliscommon.BatchJobMetadataTracker.{SH_FAILED_TILE_REQUESTS, SH_PU}
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
  @deprecated("syncer endpoint is no longer online")
  def withGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                              clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                              maxSpatialResolution: CellSize, maxSoftErrorsRatio: Double): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint, respectRetryAfterHeader = false),
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
      processingOptions, sampleType, new RlGuardAdapter, maxSpatialResolution, maxSoftErrorsRatio)

  def withoutGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String,
                                 clientId: String, clientSecret: String,
                                 zookeeperConnectionString: String, zookeeperAccessTokenPath: String,
                                 processingOptions: util.Map[String, Any], sampleType: SampleType,
                                 maxSpatialResolution: CellSize, maxSoftErrorsRatio: Double): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint),
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(zookeeperConnectionString,
        zookeeperAccessTokenPath, clientId, clientSecret),
      processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution, maxSoftErrorsRatio = maxSoftErrorsRatio)

  def withoutGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                                 clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                                 maxSpatialResolution: CellSize, maxSoftErrorsRatio: Double): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint),
      new MemoizedCuratorCachedAccessTokenWithAuthApiFallbackAuthorizer(clientId, clientSecret),
      processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution, maxSoftErrorsRatio = maxSoftErrorsRatio)
}

class PyramidFactory(collectionId: String, datasetId: String, catalogApi: CatalogApi, processApi: ProcessApi,
                     authorizer: Authorizer,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16,
                     rateLimitingGuard: RateLimitingGuard = NoRateLimitingGuard,
                     maxSpatialResolution: CellSize = CellSize(10,10), maxSoftErrorsRatio: Double = 0.0) extends Serializable {
  import PyramidFactory._

  require(maxSoftErrorsRatio >= 0.0 && maxSoftErrorsRatio <= 1.0,
    s"maxSoftErrorsRatio $maxSoftErrorsRatio out of range [0.0, 1.0]")

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
    tracker.registerDoubleCounter(SH_PU)
    tracker.registerCounter(SH_FAILED_TILE_REQUESTS)

    val numRequests = overlappingKeys.size
    val tilesRdd = sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
      .flatMap { key =>
        val width = layout.tileLayout.tileCols
        val height = layout.tileLayout.tileRows

        awaitRateLimitingGuardDelay(bandNames, width, height)

        try {
          val (tile, processingUnitsSpent) = authorized { accessToken =>
            processApi.getTile(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
              key.temporalKey, width, height, bandNames, sampleType, Criteria.toDataFilters(metadataProperties),
              processingOptions, accessToken)
          }
          tracker.add(SH_PU, processingUnitsSpent)

          Some(key -> tile)
        } catch {
          case e @ SentinelHubException(_, _, _, responseBody) =>
            tracker.add(SH_FAILED_TILE_REQUESTS, 1)

            val trackedMetadata = tracker.asDict()
            val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

            val errorsRatio = numFailedRequests.toDouble / numRequests
            if (errorsRatio <= maxSoftErrorsRatio) {
              logger.warn(s"ignoring soft error $responseBody;" +
                s" error/request ratio [$numFailedRequests/$numRequests] $errorsRatio <= $maxSoftErrorsRatio", e)
              None
            } else {
              logger.warn(s"propagating hard error $responseBody;" +
                s" error/request ratio [$numFailedRequests/$numRequests] $errorsRatio > $maxSoftErrorsRatio", e)
              throw e
            }
        }
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

      val scheme = FloatingLayoutScheme(dataCubeParameters.tileSize)

      val multiple_polygons_flag = polygons.length > 1
      var metadata = DatacubeSupport.layerMetadata(
        boundingBox, from, to, 0, sampleType.cellType, scheme, maxSpatialResolution,
        dataCubeParameters.globalExtent, multiple_polygons_flag
      )
      val layout = metadata.layout
      logger.info(s"Creating Sentinelhub datacube ${collectionId} with metadata ${metadata}")

      val tracker = BatchJobMetadataTracker.tracker("")
      tracker.registerDoubleCounter(SH_PU)
      tracker.registerCounter(SH_FAILED_TILE_REQUESTS)

      val maskingStrategyParameters = dataCubeParameters.maskingStrategyParameters

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        //noinspection ComparingUnrelatedTypes
        val maskClouds = maskingStrategyParameters.get("method") == "mask_scl_dilation"

        def loadMasked(spatialKey: SpatialKey, dateTime: ZonedDateTime, numRequests: Long): Option[MultibandTile] = try {
          def getTile(bandNames: Seq[String], projectedExtent: ProjectedExtent, width: Int, height: Int): MultibandTile = {
            awaitRateLimitingGuardDelay(bandNames, width, height)

            val (tile, processingUnitsSpent) = authorized { accessToken =>
              processApi.getTile(datasetId, projectedExtent, dateTime, width, height, bandNames,
                sampleType, Criteria.toDataFilters(metadata_properties), processingOptions, accessToken)
            }
            tracker.add(SH_PU, processingUnitsSpent)
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

              if (sclBandIndex >= 0) new SCLConvolutionFilterStrategy(sclBandIndex,maskingStrategyParameters)
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
          case e @ SentinelHubException(_, _, _, responseBody) =>
            tracker.add(SH_FAILED_TILE_REQUESTS, 1)

            val trackedMetadata = tracker.asDict()
            val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

            val errorsRatio = numFailedRequests.toDouble / numRequests
            if (errorsRatio <= maxSoftErrorsRatio) {
              logger.warn(s"ignoring soft error $responseBody;" +
                s" error/request ratio [$numFailedRequests/$numRequests] $errorsRatio <= $maxSoftErrorsRatio", e)
              None
            } else {
              logger.warn(s"propagating hard error $responseBody;" +
                s" error/request ratio [$numFailedRequests/$numRequests] $errorsRatio > $maxSoftErrorsRatio", e)
              throw e
            }
        }

        val tilesRdd =
          if (datasetId == "dem") {
            val overlappingKeys = layout.mapTransform.keysForGeometry(GeometryCollection(polygons)).toSeq

            val numRequests = overlappingKeys.size
            val keysRdd = sc.parallelize(overlappingKeys, math.max(1, overlappingKeys.size / 10))

            val tilesRdd = keysRdd
              .flatMap { spatialKey =>
                // "dem" data doesn't have a time dimension so the actual timestamp doesn't matter
                loadMasked(spatialKey, ZonedDateTime.of(1981, 4, 24, 2, 0, 0, 0, UTC), numRequests)
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
              val multiPolygon:Geometry = if(polygons.length <=2000){
                simplify(polygons)
              }else{
                val polygonsRDD: RDD[Polygon] = sc.parallelize(polygons,math.max(1,polygons.length/100))
                // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
                var requiredSpatialKeys = polygonsRDD.clipToGrid(metadata.layout)
                val transform = metadata.mapTransform
                val tilebounds = dissolve(requiredSpatialKeys.map(_._1).distinct().map(key=>transform.keyToExtent(key).toPolygon()).collect())
                if(tilebounds.getNumGeometries > 500) {
                  //shub catalog can not handle huge amount of polygons, so just use bbox
                  boundingBox.extent.toPolygon()
                } else{
                  tilebounds
                }
              }

              val features = authorized { accessToken =>
                _catalogApi.search(collectionId, multiPolygon, polygons_crs,
                  from, atEndOfDay(to), accessToken, Criteria.toQueryProperties(metadata_properties))
              }

            tracker.addInputProducts(collectionId,features.keys.toList.asJava)

            val polygonsRDD = sc.parallelize(features.values.toSeq,math.max(1,features.size/10)).map(_.reproject(LatLng, boundingBox.crs)).map(f => Feature(f intersection multiPolygon, f.data.toLocalDate.atStartOfDay(UTC)))
            var requiredSpatialKeysForFeatures: RDD[(SpatialKey, Iterable[Feature[Geometry, ZonedDateTime]])] = polygonsRDD.clipToGrid(metadata.layout).groupByKey()

            val spatialKeyCount = requiredSpatialKeysForFeatures.map(_._1).countApproxDistinct()
            logger.info(s"Sentinelhub datacube requires approximately ${spatialKeyCount} spatial keys.")

            val retiledMetadata: Option[TileLayerMetadata[SpaceTimeKey]] = None//DatacubeSupport.optimizeChunkSize(metadata, polygons, Option(dataCubeParameters), spatialKeyCount)
            metadata = retiledMetadata.getOrElse(metadata)

            if (retiledMetadata.isDefined) {
              requiredSpatialKeysForFeatures = polygonsRDD.clipToGrid(metadata.layout).groupByKey()
            }

            val requiredKeysRdd = requiredSpatialKeysForFeatures.flatMap(tuple=> tuple._2.map(feature => SpaceTimeKey(tuple._1.col,tuple._1.row,feature.data)))

            val partitioner = DatacubeSupport.createPartitioner(Some(dataCubeParameters), requiredKeysRdd, metadata)
            val approxRequests = requiredKeysRdd.countApproxDistinct()
            logger.info(s"Created Sentinelhub datacube ${collectionId} with $approxRequests keys and metadata ${metadata} and ${partitioner.get}")

            var keysRdd = requiredKeysRdd.map((_,Option.empty)).partitionBy(partitioner.get)

            keysRdd = DatacubeSupport.applyDataMask(Some(dataCubeParameters), keysRdd)

            val tilesRdd: RDD[(SpaceTimeKey,MultibandTile)] = keysRdd
              .mapPartitions(_.map { case (spaceTimeKey, _) => (spaceTimeKey, loadMasked(spaceTimeKey.spatialKey, spaceTimeKey.time, approxRequests)) }, preservesPartitioning = true)
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
