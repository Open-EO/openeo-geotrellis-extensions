package org.openeo.geotrellissentinelhub

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.densify.Densifier
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrelliscommon.BatchJobMetadataTracker.{ProductIdAndUrl, SH_FAILED_TILE_REQUESTS, SH_PU}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, DatacubeSupport, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, ScopedMetadataTracker, SpaceTimeByMonthPartitioner}
import org.openeo.geotrellissentinelhub.SampleType.{SampleType, UINT16}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._

object PyramidFactory {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PyramidFactory])


  private val maxKeysPerPartition = 20

  // convenience methods for Python client
  // Terrascope setup with Zookeeper cache and default Auth API endpoint
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

  // CDSE setup with user's Keycloak access token
  def withFixedAccessToken(endpoint: String, collectionId: String, datasetId: String,
                           accessToken: String,
                           processingOptions: util.Map[String, Any], sampleType: SampleType,
                           maxSpatialResolution: CellSize, maxSoftErrorsRatio: Double): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), new FixedAccessTokenAuthorizer(accessToken),
      processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution, maxSoftErrorsRatio = maxSoftErrorsRatio)

  // CDSE setup with workaround for Keycloak access token not working yet
  def withCustomAuthApi(endpoint: String, collectionId: String, datasetId: String,
                        authApiUrl: String, clientId: String, clientSecret: String,
                        processingOptions: util.Map[String, Any], sampleType: SampleType,
                        maxSpatialResolution: CellSize, maxSoftErrorsRatio: Double): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      new MemoizedAuthApiAccessTokenAuthorizer(clientId, clientSecret, authApiUrl),
      processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution, maxSoftErrorsRatio = maxSoftErrorsRatio)

  private def byTileId(productId: String, tileIdCriteria: util.Map[String, Any]): Boolean = {
    lazy val actualTileId = Sentinel2L2a.extractTileId(productId)

    val matchesSingleTileId = tileIdCriteria.get("eq") match {
      case tileId: String => actualTileId contains tileId
      case _ => true
    }

    val matchesMultipleTileIds = tileIdCriteria.get("in") match {
      case tileIds: util.List[String] => tileIds.asScala.exists(actualTileId.contains)
      case _ => true
    }

    matchesSingleTileId && matchesMultipleTileIds
  }
}

class PyramidFactory(collectionId: String, datasetId: String, catalogApi: CatalogApi, processApi: ProcessApi,
                     authorizer: Authorizer,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16,
                     maxSpatialResolution: CellSize = CellSize(10,10), maxSoftErrorsRatio: Double = 0.0) extends Serializable {
  import PyramidFactory._

  require(maxSoftErrorsRatio >= 0.0 && maxSoftErrorsRatio <= 1.0,
    s"maxSoftErrorsRatio $maxSoftErrorsRatio out of range [0.0, 1.0]")

  @transient private val _catalogApi: CatalogApi = if (collectionId == null) new MadeToMeasureCatalogApi else catalogApi

  private val maxZoom = 14

  private def authorized[R](fn: String => R): R = authorizer.authorized(fn)

  private def sentinelHubException[R](tracker: BatchJobMetadataTracker, numRequests: Long): PartialFunction[Throwable, Option[R]] = {
    case e @ SentinelHubException(_, _, _, responseBody) =>
      tracker.add(SH_FAILED_TILE_REQUESTS, 1)

      // failed requests might not get tracked (think /result) but there's at least one because we're handling it!
      val numFailedRequests = tracker.asDict().get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long] max 1

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

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom,
                    bandNames: Seq[String], metadataProperties: util.Map[String, util.Map[String, Any]],
                    features: collection.Map[String, Feature[Geometry, FeatureData]],
                    correlationId: String)(implicit sc: SparkContext):
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
      val intersectingFeaturesByDay = features.values
        .map(feature => feature.mapData(_.dateTime.toLocalDate.atStartOfDay(UTC)))
        .groupBy(_.data)

      val simplifiedIntersectingFeaturesByDay = intersectingFeaturesByDay
        .map { case (date, features) =>
          val multiPolygons = features
            .map(_.geom)
            .flatMap {
              case polygon: Polygon => Some(MultiPolygon(polygon))
              case multiPolygon: MultiPolygon => Some(multiPolygon)
              case _ => None
            }

          Feature(simplify(multiPolygons), date)
        }

      val intersectingFeatureKeys = for {
        Feature(geom, date) <- simplifiedIntersectingFeaturesByDay
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

    val scopedMetadataTracker = ScopedMetadataTracker(scope = correlationId)

    val numRequests = overlappingKeys.size
    val tilesRdd = sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
      .flatMap { key =>
        val width = layout.tileLayout.tileCols
        val height = layout.tileLayout.tileRows

        try {
          val (multibandTile, processingUnitsSpent) = authorized { accessToken =>
            processApi.getTile(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
              key.temporalKey, width, height, bandNames, sampleType, Criteria.toDataFilters(metadataProperties),
              processingOptions, accessToken)
          }
          tracker.add(SH_PU, processingUnitsSpent)
          scopedMetadataTracker.addSentinelHubProcessingUnits(processingUnitsSpent)

          Some(key -> multibandTile)
        } catch sentinelHubException(tracker, numRequests)
      }
      .filter { case (_, multibandTile) => multibandTile.bands.exists(tile => !tile.isNoDataTile) }
      .partitionBy(partitioner)

    ContextRDD(tilesRdd, metadata)
  }

  private def atEndOfDay(to: ZonedDateTime): ZonedDateTime = {
    val endOfDay = OffsetTime.of(LocalTime.MAX, UTC)
    to.toLocalDate.atTime(endOfDay).toZonedDateTime
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
              metadataProperties: util.Map[String, util.Map[String, Any]], correlationId: String)(implicit sc: SparkContext):
  Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val (polygon, polygonCrs) = (boundingBox.extent.toPolygon(), boundingBox.crs)

    // TODO: apply the "dem" optimization of datacube_seq to pyramid_seq as well
    val features = authorized { accessToken =>
      _catalogApi.search(collectionId, polygon, polygonCrs,
        from, to, accessToken, Criteria.toQueryProperties(metadataProperties, collectionId))
    }

    val layers = for (zoom <- maxZoom to 0 by -1)
      yield zoom -> layer(boundingBox, from, to, zoom, bandNames, metadataProperties, features, correlationId)

    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_datetime: String, until_datetime: String,
                  band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = pyramid_seq(bbox, bbox_srs, from_datetime, until_datetime, band_names,
    metadata_properties, correlationId = "")

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_datetime: String, until_datetime: String,
                  band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]],
                  correlationId: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_datetime, ISO_OFFSET_DATE_TIME)
    val until = ZonedDateTime.parse(until_datetime, ISO_OFFSET_DATE_TIME)
    val to = if (from isEqual until) atEndOfDay(until) else until minusNanos 1

    pyramid(projectedExtent, from, to, band_names.asScala, metadata_properties, correlationId).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_datetime: String, until_datetime: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = datacube_seq(polygons, polygons_crs, from_datetime, until_datetime,
    band_names, metadata_properties, new DataCubeParameters)

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_datetime: String, until_datetime: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]],
                   dataCubeParameters: DataCubeParameters):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = datacube_seq(polygons, polygons_crs, from_datetime, until_datetime,
    band_names, metadata_properties, dataCubeParameters, correlationId = "")

  def datacube_seq(polygons: Array[MultiPolygon], polygons_crs: CRS, from_datetime: String, until_datetime: String,
                   band_names: util.List[String], metadata_properties: util.Map[String, util.Map[String, Any]],
                   dataCubeParameters: DataCubeParameters, correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    // TODO: use ProjectedPolygons type
    // TODO: reduce code duplication with pyramid_seq()
    if (dataCubeParameters.timeDimensionFilter.isDefined) {
      throw new IllegalArgumentException("OpenEO does not support timeDimensionFilter yet for SentinelHub layers. (Probably used in filter_labels)")
    }

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = {
      implicit val sc: SparkContext = SparkContext.getOrCreate()

      val boundingBox = ProjectedExtent(polygons.toSeq.extent, polygons_crs)

      val from = ZonedDateTime.parse(from_datetime, ISO_OFFSET_DATE_TIME)
      val until = ZonedDateTime.parse(until_datetime, ISO_OFFSET_DATE_TIME)
      val to = if (from isEqual until) atEndOfDay(until) else until minusNanos 1

      // TODO: call into AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

      val scheme = FloatingLayoutScheme(dataCubeParameters.tileSize)

      val multiple_polygons_flag = polygons.length > 1
      val metadata = DatacubeSupport.layerMetadata(
        boundingBox, from, to, 0, sampleType.cellType, scheme, maxSpatialResolution, // TODO: derive from and to from catalog features instead? (see FileLayerProvider)
        dataCubeParameters.globalExtent, multiple_polygons_flag
      )
      val layout = metadata.layout
      logger.info(s"Creating Sentinelhub datacube ${collectionId} with metadata ${metadata}")

      val tracker = BatchJobMetadataTracker.tracker("")
      tracker.registerDoubleCounter(SH_PU)
      tracker.registerCounter(SH_FAILED_TILE_REQUESTS)

      val scopedMetadataTracker = ScopedMetadataTracker(scope = correlationId)

      val maskingStrategyParameters = dataCubeParameters.maskingStrategyParameters

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        //noinspection ComparingUnrelatedTypes
        val maskClouds = maskingStrategyParameters.get("method") == "mask_scl_dilation"

        def loadMasked(spatialKey: SpatialKey, dateTime: ZonedDateTime, numRequests: Long): Option[MultibandTile] = try {
          def getTile(bandNames: Seq[String], projectedExtent: ProjectedExtent, width: Int, height: Int): MultibandTile = {
            val (tile, processingUnitsSpent) = authorized { accessToken =>
              processApi.getTile(datasetId, projectedExtent, dateTime, width, height, bandNames,
                sampleType, Criteria.toDataFilters(metadata_properties), processingOptions, accessToken)
            }
            tracker.add(SH_PU, processingUnitsSpent)
            scopedMetadataTracker.addSentinelHubProcessingUnits(processingUnitsSpent)
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
        } catch sentinelHubException(tracker, numRequests)

        val tilesRdd =
          if (datasetId == "dem") {
            // optimization to avoid an unnecessary cartesian product of spatial keys and timestamps
            val overlappingSpatialKeys = layout.mapTransform.keysForGeometry(GeometryCollection(polygons)).toSeq

            val numRequests = overlappingSpatialKeys.size
            val spatialKeysRdd = sc.parallelize(overlappingSpatialKeys, math.max(1, overlappingSpatialKeys.size / 10))

            val tilesRdd = spatialKeysRdd
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

            DatacubeSupport.applyDataMask(Some(dataCubeParameters), tilesRdd, metadata, pixelwiseMasking = true)
          } else {
            val multiPolygon: Geometry = if (polygons.length <= 2000) {
              val simplified = simplify(polygons)
              logger.debug(s"simplified ${polygons.map(_.getNumGeometries).sum} polygons to ${simplified.getNumGeometries}")
              simplified
            } else {
              val polygonsRDD = sc.parallelize(polygons, math.max(1, polygons.length / 100))
              // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
              val requiredSpatialKeys = polygonsRDD.clipToGrid(metadata.layout)
              val transform = metadata.mapTransform
              val tilebounds = dissolve(requiredSpatialKeys.map(_._1).distinct().map(key => transform.keyToExtent(key).toPolygon()).collect())
              if (tilebounds.getNumGeometries > 500) {
                logger.debug(s"shub catalog can not handle huge amount of polygons, so just use bbox ${boundingBox}")
                boundingBox.extent.toPolygon()
              } else {
                tilebounds
              }
            }

            val features = Option(metadata_properties.get("tileId"))
              .foldLeft(authorized { accessToken =>
                _catalogApi.search(collectionId, multiPolygon, polygons_crs,
                  from, to, accessToken, Criteria.toQueryProperties(metadata_properties, collectionId))
              }) { (acc, tileIdCriteria) =>
                acc.filterKeys(byTileId(_, tileIdCriteria))
              }

            tracker.addInputProductsWithUrls(
              collectionId,
              features.map {
                case (id, Feature(_, FeatureData(_, selfUrl))) => new ProductIdAndUrl(id, selfUrl.orNull)
              }.toList.asJava
            )

            // In test over England, there where up to 0.003 deviations on long line segments due to curvature
            // change between CRS. Here we convert that distance to the value in the polygon specific CRS.
            //TODO we introduced densifier below, which may solve this problem without requiring this buffer
            val multiPolygonBuffered = {
              val centroid = multiPolygon.getCentroid.reproject(polygons_crs, LatLng)
              val derivationSegmentLatLng = LineString(centroid, Point(centroid.x, centroid.y + 0.006))
              val derivationSegment = derivationSegmentLatLng.reproject(LatLng, polygons_crs)
              val maxDerivationEstimate = derivationSegment.getLength
              multiPolygon.buffer(maxDerivationEstimate)
            }

            val targetExtentInLatLon = dataCubeParameters.globalExtent.getOrElse(boundingBox).reproject(LatLng)
            val featuresRDD = sc.parallelize(features.values.toSeq, 1 max (features.size / 10))
            val featureIntersections = featuresRDD.flatMap { feature =>

              //feature geometry in lat lon may be invalid in target CRS, so we apply a first clipping
              val clippedFeature = feature.geom.intersection(targetExtentInLatLon).buffer(1.0).intersection(feature.geom)
              //correct reprojection requires densified geometry
              if(clippedFeature.isEmpty) {
                None
              }else{
                val densifiedFeature = Densifier.densify(clippedFeature, 0.1)
                val reprojectedFeature = densifiedFeature.reproject(LatLng, boundingBox.crs)
                val intersection = reprojectedFeature intersection multiPolygonBuffered

                if (intersection.isEmpty) {
                  if (logger.isDebugEnabled) {
                    logger.debug(s"shub returned a Feature that does not intersect with our requested polygons: ${feature.geom.toGeoJson()}")
                  }
                  None
                } else
                  Some(Feature(intersection, feature.data.dateTime.toLocalDate.atStartOfDay(UTC)))
              }

            }

            if (featureIntersections.isEmpty()) {
              if(dataCubeParameters.allowEmptyCube) {
                return Seq( 0-> ContextRDD(sc.emptyRDD[(SpaceTimeKey, MultibandTile)],metadata))
              }else{
                throw NoSuchFeaturesException(message =
                  s"""no features found for criteria:
                     |collection ID "$collectionId"
                     |${polygons.length} polygon(s)
                     |[$from_datetime, $until_datetime)
                     |metadata properties $metadata_properties""".stripMargin)
              }

            }

            // this optimization consists of dissolving geometries of overlapping features and will therefore lose
            // per-feature information (currently there is none).
            val featureIntersectionsByDay = featureIntersections.groupBy(_.data)
            val simplifiedFeatureIntersectionsByDay = featureIntersectionsByDay.map { case (date, features) =>
              val multiPolygons = features
                .map(_.geom)
                .flatMap {
                  case polygon: Polygon => Some(MultiPolygon(polygon))
                  case multiPolygon: MultiPolygon => Some(multiPolygon)
                  case _ => None
                }

              Feature(simplify(multiPolygons), date)
            }

            val requiredSpatialKeysForFeatures = simplifiedFeatureIntersectionsByDay.clipToGrid(metadata.layout)

            if (logger.isInfoEnabled) {
              val spatialKeyCount = requiredSpatialKeysForFeatures.map(_._1).countApproxDistinct()
              logger.info(s"Sentinelhub datacube requires approximately ${spatialKeyCount} spatial keys.")
            }

            val requiredKeysRdd = requiredSpatialKeysForFeatures.map { case (SpatialKey(col, row), Feature(_, date)) => SpaceTimeKey(col, row, date)}.filter(k=> k.col>=0&&k.row>=0)

            val partitioner = DatacubeSupport.createPartitioner(Some(dataCubeParameters), requiredKeysRdd, metadata)
            val approxRequests = requiredKeysRdd.countApproxDistinct()
            logger.info(s"Created Sentinelhub datacube ${collectionId} with $approxRequests keys and metadata ${metadata} and ${partitioner.get}")

            val keysRdd = requiredKeysRdd.map((_, None)).partitionBy(partitioner.get)

            var keysRddWithData = keysRdd
              .mapPartitions(_.map { case (spaceTimeKey, _) => (spaceTimeKey, loadMasked(spaceTimeKey.spatialKey, spaceTimeKey.time, approxRequests)) }, preservesPartitioning = true)
              .flatMapValues(_.toList)
            keysRddWithData = DatacubeSupport.applyDataMask(Some(dataCubeParameters), keysRddWithData, metadata, pixelwiseMasking = true)

            val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = keysRddWithData
              .filter(tile => !tile._2.bands.forall(_.isNoDataTile))
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
}
