package org.openeo.geotrellissentinelhub

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.{CellSize, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.{DataCubeParameters, DatacubeSupport, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceOnlyPartitioner, SparseSpaceTimePartitioner}
import org.openeo.geotrellissentinelhub.SampleType.{SampleType, UINT16}
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZoneOffset.UTC
import java.time.{LocalTime, OffsetTime, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.annotation.meta.param
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object PyramidFactory {
  private val logger: Logger = LoggerFactory.getLogger(classOf[PyramidFactory])

  private val maxKeysPerPartition = 20

  // convenience methods for Python client
  def withGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                              clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                              maxSpatialResolution: CellSize): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      clientId, clientSecret, processingOptions, sampleType, new RlGuardAdapter, maxSpatialResolution)

  def withoutGuardedRateLimiting(endpoint: String, collectionId: String, datasetId: String, clientId: String,
                                 clientSecret: String, processingOptions: util.Map[String, Any], sampleType: SampleType,
                                 maxSpatialResolution: CellSize): PyramidFactory =
    new PyramidFactory(collectionId, datasetId, new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint),
      clientId, clientSecret, processingOptions, sampleType, maxSpatialResolution = maxSpatialResolution)

  @deprecated("call withGuardedRateLimiting instead")
  def rateLimited(endpoint: String, collectionId: String, datasetId: String, clientId: String, clientSecret: String,
                  processingOptions: util.Map[String, Any], sampleType: SampleType,
                  maxSpatialResolution: CellSize): PyramidFactory =
    withGuardedRateLimiting(endpoint, collectionId, datasetId, clientId, clientSecret, processingOptions, sampleType,
      maxSpatialResolution)

  @deprecated("include a maxSpatialResolution")
  def rateLimited(endpoint: String, collectionId: String, datasetId: String, clientId: String, clientSecret: String,
                  processingOptions: util.Map[String, Any], sampleType: SampleType): PyramidFactory =
    rateLimited(endpoint, collectionId, datasetId, clientId, clientSecret, processingOptions, sampleType, CellSize(10,10))

  @deprecated("include a collectionId")
  def rateLimited(endpoint: String, datasetId: String, clientId: String, clientSecret: String,
                  processingOptions: util.Map[String, Any], sampleType: SampleType): PyramidFactory =
    rateLimited(endpoint, collectionId = null, datasetId, clientId, clientSecret, processingOptions, sampleType)
}

class PyramidFactory(collectionId: String, datasetId: String, @(transient @param) catalogApi: CatalogApi,
                     processApi: ProcessApi, clientId: String, clientSecret: String,
                     processingOptions: util.Map[String, Any] = util.Collections.emptyMap[String, Any],
                     sampleType: SampleType = UINT16,
                     rateLimitingGuard: RateLimitingGuard = NoRateLimitingGuard,
                     maxSpatialResolution: CellSize = CellSize(10,10)) extends Serializable {
  import PyramidFactory._

  private val maxZoom = 14

  private def authorized[R](fn: String => R): R =
    org.openeo.geotrellissentinelhub.authorized[R](clientId, clientSecret)(fn)

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom,
                    bandNames: Seq[String], metadataProperties: util.Map[String, Any],
                    dates: Seq[ZonedDateTime])(implicit sc: SparkContext):
  MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)

    val scheme = ZoomedLayoutScheme(targetCrs)
    val metadata: TileLayerMetadata[SpaceTimeKey] = DatacubeSupport.layerMetadata(ProjectedExtent(reprojectedBoundingBox,targetCrs),from,to,zoom,sampleType.cellType,scheme,maxSpatialResolution)
    val layout = metadata.layout

    val overlappingKeys = for {
      date <- dates
      spatialKey <- layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon())
    } yield SpaceTimeKey(spatialKey, date)


    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)

    val tilesRdd = sc.parallelize(overlappingKeys, numSlices = (overlappingKeys.size / maxKeysPerPartition) max 1)
      .map { key =>
        val width = layout.tileLayout.tileCols
        val height = layout.tileLayout.tileRows

        awaitRateLimitingGuardDelay(bandNames, width, height)

        val tile = authorized { accessToken =>
          processApi.getTile(datasetId, ProjectedExtent(key.spatialKey.extent(layout), targetCrs),
            key.temporalKey, width, height, bandNames, sampleType, metadataProperties, processingOptions, accessToken)
        }

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

  private def sequentialDates(from: ZonedDateTime, to: ZonedDateTime): Stream[ZonedDateTime] = {
    def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

    sequentialDates(from)
      .takeWhile(date => !(date isAfter to))
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bandNames: Seq[String],
              metadataProperties: util.Map[String, Any])(implicit sc: SparkContext):
  Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val dates =
      if (collectionId != null) authorized { accessToken =>
        catalogApi
          .dateTimes(collectionId, boundingBox.extent.toPolygon(), boundingBox.crs, from, atEndOfDay(to), accessToken,
            toQueryProperties(dataFilters = metadataProperties))
          .map(_.toLocalDate.atStartOfDay(UTC))
          .distinct // ProcessApi::getTile takes care of [day, day + 1] interval and mosaicking therein
          .sorted
      } else sequentialDates(from, to)

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

      val scheme = FloatingLayoutScheme(256)

      val metadata = DatacubeSupport.layerMetadata(boundingBox,from,to,0,sampleType.cellType,scheme,maxSpatialResolution,dataCubeParameters.globalExtent)
      val layout = metadata.layout

      val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = {
        val overlappingKeys =
          if (collectionId != null) {
            val multiPolygon = multiPolygonFromPolygonExteriors(polygons)

            val features = authorized { accessToken =>
              catalogApi.search(collectionId, multiPolygon, polygons_crs,
                from, atEndOfDay(to), accessToken, toQueryProperties(dataFilters = metadata_properties))
            }

            val intersectingFeatureKeys = for {
              (_, Feature(geom, datetime)) <- features.toSet
              date = datetime.toLocalDate.atStartOfDay(UTC)
              reprojectedGeom = geom.reproject(LatLng, boundingBox.crs)
              SpatialKey(col, row) <- layout.mapTransform.keysForGeometry(reprojectedGeom intersection multiPolygon)
            } yield SpaceTimeKey(col, row, date)

            intersectingFeatureKeys
              .toSeq
          } else for {
            date <- sequentialDates(from, to)
            spatialKey <- layout.mapTransform.keysForGeometry(GeometryCollection(polygons))
          } yield SpaceTimeKey(spatialKey, date)

        //noinspection ComparingUnrelatedTypes
        val maskClouds = dataCubeParameters.maskingStrategyParameters.get("method") == "mask_scl_dilation"

        def loadMasked(key: SpaceTimeKey): Option[MultibandTile] = {
          def getTile(bandNames: Seq[String], projectedExtent: ProjectedExtent, width: Int, height: Int): MultibandTile = {
            awaitRateLimitingGuardDelay(bandNames, width, height)

            authorized { accessToken =>
              processApi.getTile(datasetId, projectedExtent, key.temporalKey, width, height, bandNames,
                sampleType, metadata_properties, processingOptions, accessToken)
            }
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

        logger.info(s"Created Sentinelhub datacube with ${overlappingKeys.size} keys and metadata ${metadata}")

        val reduction: Int = dataCubeParameters.partitionerIndexReduction
        val partitionerIndex: PartitionerIndex[SpaceTimeKey] = {
          if( dataCubeParameters.partitionerTemporalResolution!= "ByDay") {
            val indices = overlappingKeys.map(SparseSpaceOnlyPartitioner.toIndex(_,indexReduction = reduction)).distinct.sorted.toArray
            new SparseSpaceOnlyPartitioner(indices,reduction)
          }else{
            val indices = overlappingKeys.map(SparseSpaceTimePartitioner.toIndex(_,indexReduction = reduction)).distinct.sorted.toArray
            new SparseSpaceTimePartitioner(indices,reduction)
          }
        }
        val partitioner = SpacePartitioner(metadata.bounds)(SpaceTimeKey.Boundable,ClassTag(classOf[SpaceTimeKey]), partitionerIndex)

        val tilesRdd: RDD[(SpaceTimeKey,MultibandTile)] = sc.parallelize(overlappingKeys.map((_,Option.empty)))
          .partitionBy(partitioner)
          .map(key => (key._1,loadMasked(key._1)))
          .filter{case (key:SpaceTimeKey,tile:Option[MultibandTile])=> tile.isDefined && !tile.get.bands.forall(_.isNoDataTile) }
          .mapValues(t => t.get)

        tilesRdd.name = s"Sentinelhub-${collectionId}"

        tilesRdd
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
    MILLISECONDS.sleep(delay.toMillis)
  }

}
