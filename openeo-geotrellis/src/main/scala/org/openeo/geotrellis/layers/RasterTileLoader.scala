package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{LayoutDefinition, LayoutTileSource, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, MultibandTile, NoNoData, PaddedTile, Raster, RasterExtent, RasterRegion, RasterSource, SourceName, Tile}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withGeometryClipToGridMethods}
import geotrellis.vector.{MultiPolygon, Polygon, ReprojectMutliPolygon}
import org.apache.spark.SparkContext
import org.apache.spark.metrics.source
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrellis.layers.FileLayerProvider.{applySpatialMask, createPartitioner, megapixelPerSecondMeter}
import org.openeo.geotrellis.layers.raster_source.{GDALCloudRasterSource, IndexedRasterSource, ValueOffsetRasterSource}
import org.openeo.geotrellis.{EmptyMultibandTile, sortableSourceName}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, ByKeyPartitioner, CloudFilterStrategy, DataCubeParameters, DatacubeSupport, L1CCloudFilterStrategy, MaskTileLoader, NoCloudFilterStrategy, time}
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.{Logger, LoggerFactory}
import spire.implicits.coordinateSpaceOps

import java.io.IOException
import scala.collection.immutable
import scala.collection.parallel.CollectionsHaveToParArray
import scala.jdk.CollectionConverters.{IterableHasAsJava, IteratorHasAsScala}

object RasterTileLoader extends RasterTileLoader {
}


case class RasterTileLoader() {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[RasterTileLoader])
  private val PIXEL_COUNTER = "InputPixels"


  def readMultibandTileLayer(rasterSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey], polygons: Array[MultiPolygon], polygons_crs: CRS, sc: SparkContext, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, useSparsePartitioner: Boolean = true, datacubeParams: Option[DataCubeParameters] = None): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val polygonsRDD = sc.parallelize(polygons).map {
      _.reproject(polygons_crs, metadata.crs)
    }
    // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
    val requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] = polygonsRDD.clipToGrid(metadata.layout).groupByKey()
    val retainNoDataTiles = datacubeParams.exists(_.retainNoDataTiles)
    tileSourcesToDataCube(rasterSources, metadata, requiredSpatialKeys, sc, retainNoDataTiles, cloudFilterStrategy, useSparsePartitioner, datacubeParams)
  }

  def loadRasterRegionsToTiles(
                                regions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
                                metadata: TileLayerMetadata[SpaceTimeKey],
                                maskStrategy: Option[CloudFilterStrategy],
                                partitioner: Option[SpacePartitioner[SpaceTimeKey]],
                                datacubeParams: Option[DataCubeParameters],
                                sources: Seq[(RasterSource, Feature)],
                                openSearchLinkTitlesWithBandId: Seq[(String, Int)],
                                softErrors: Boolean
                              ): MultibandTileLayerRDD[SpaceTimeKey] = {
    val theMaskStrategy: CloudFilterStrategy = maskStrategy.getOrElse(NoCloudFilterStrategy)
    val retainNoDataTiles = datacubeParams.exists(_.retainNoDataTiles)
    val size = openSearchLinkTitlesWithBandId.size * metadata.layout.size
    logger.debug(s"Size: $size")
    if (!datacubeParams.exists(_.loadPerProduct) || theMaskStrategy != NoCloudFilterStrategy) {
      logger.debug("Load per product: false")
      rasterRegionsToTiles(regions, metadata, retainNoDataTiles, theMaskStrategy, partitioner, datacubeParams)
    } else {
      logger.debug("Load per product: true")
      rasterRegionsToTilesLoadPerProductStrategy(regions, metadata, retainNoDataTiles, NoCloudFilterStrategy, partitioner, datacubeParams, openSearchLinkTitlesWithBandId.size, sources, softErrors)
    }
  }

  private def tileSourcesToDataCube(rasterSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey], requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])], sc: SparkContext, retainNoDataTiles: Boolean, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, useSparsePartitioner: Boolean = true, datacubeParams: Option[DataCubeParameters] = None, inputFeatures: Option[Seq[Feature]] = None): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val localSpatialKeys = applySpatialMask(datacubeParams, requiredSpatialKeys, metadata)

    var spatialKeyCount = localSpatialKeys.countApproxDistinct()

    // Remove all source files that do not intersect with the 'interior' of the requested extent.
    // Note: A normal intersect would also include sources that exactly border the requested extent.
    val filteredSources: RDD[LayoutTileSource[SpaceTimeKey]] = rasterSources.filter({ tiledLayoutSource =>
      tiledLayoutSource.source.extent.interiorIntersects(tiledLayoutSource.layout.extent)
    })

    val partitioner = createPartitioner(datacubeParams, localSpatialKeys, filteredSources, metadata)

    //use spatialkeycount as heuristic to choose code path

    var requestedRasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] =
      if (spatialKeyCount < 1000) {
        val keys = sc.broadcast(requiredSpatialKeys.map(_._1).collect())
        filteredSources
          .flatMap { tiledLayoutSource => {
            val spaceTimeKeys: Array[SpaceTimeKey] = keys.value.map(tiledLayoutSource.tileKeyTransform(_))
            spaceTimeKeys
              .map(key => (key, tiledLayoutSource.rasterRegionForKey(key))).filter(_._2.isDefined).map(t => (t._1, t._2.get))
              .filter({ case (key, rasterRegion) => metadata.extent.interiorIntersects(key.spatialKey.extent(metadata.layout)) })
              .map { case (key, rasterRegion) => (key, (rasterRegion, tiledLayoutSource.source.name)) }
          }
          }
      } else {
        // Convert RasterSources to RasterRegions.
        val rasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] =
          filteredSources
            .flatMap { tiledLayoutSource =>
              tiledLayoutSource.keyedRasterRegions()
                //this filter step reduces the 'Shuffle Write' size of this stage, so it already
                .filter({ case (key, rasterRegion) => metadata.extent.interiorIntersects(key.spatialKey.extent(metadata.layout)) })
                .map { case (key, rasterRegion) => (key, (rasterRegion, tiledLayoutSource.source.name)) }
            }

        // Only use the regions that correspond with a requested spatial key.

        rasterRegions
          .map { tuple => (tuple._1.spatialKey, tuple) }
          //for sparse keys, this takes a silly amount of time and memory. Just broadcasting spatialkeys and filtering on that may be a lot easier...
          //stage boundary, first stage of data loading ends here!
          .join[Null](requiredSpatialKeys.map(t => (t._1, null))).map { t => t._2._1 }

      }

    requestedRasterRegions.name = rasterSources.name
    rasterRegionsToTiles(requestedRasterRegions, metadata, retainNoDataTiles, cloudFilterStrategy, partitioner, datacubeParams)
  }

  private def rasterRegionsToTiles(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
                                   metadata: TileLayerMetadata[SpaceTimeKey],
                                   retainNoDataTiles: Boolean,
                                   cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy,
                                   partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = None,
                                   datacubeParams: Option[DataCubeParameters] = None,
                                  ) = {
    val partitioner = partitionerOption.getOrElse(SpacePartitioner(metadata.bounds))
    logger.info(s"Cube partitioner index: ${partitioner.index}")
    val totalChunksAcc: LongAccumulator = rasterRegionRDD.sparkContext.longAccumulator("ChunkCount_" + rasterRegionRDD.name)
    val tracker = BatchJobMetadataTracker.tracker("")
    tracker.registerCounter(PIXEL_COUNTER)
    val loadingTimeAcc = rasterRegionRDD.sparkContext.doubleAccumulator("SecondsPerChunk_" + rasterRegionRDD.name)
    val crs = metadata.crs
    val layout = metadata.layout
    var tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner)
        .mapPartitions(partitionIterator => {
          val loadedRDD = {
            var totalPixelsPartition = 0
            val startTime = System.currentTimeMillis()

            val (loadedPartitions, partitionPixels) = loadPartition(partitionIterator, cloudFilterStrategy, totalChunksAcc, tracker, crs, layout)
            totalPixelsPartition += partitionPixels

            val durationMillis = System.currentTimeMillis() - startTime
            if (totalPixelsPartition > 0) {
              val secondsPerChunk = (durationMillis / 1000.0) / (totalPixelsPartition / (256 * 256))
              loadingTimeAcc.add(secondsPerChunk)
            }
            loadedPartitions
          }
          val withEmptyTiles = if (retainNoDataTiles) {
            loadedRDD.map { case (key, tile) =>
              if (tile.get.bands.forall(_.isNoDataTile)) {
                (key, Some(new EmptyMultibandTile(tile.get.cols, tile.get.rows, tile.get.cellType, tile.get.bandCount)))
              } else {
                (key, tile)
              }
            }
          } else {
            loadedRDD
          }
          withEmptyTiles.filter { case (_, tile) => tile.isDefined && (retainNoDataTiles || !tile.get.bands.forall(_.isNoDataTile)) }
            .map(t => (t._1, t._2.get)).iterator
        }, preservesPartitioning = true)
    tiledRDD = DatacubeSupport.applyDataMask(datacubeParams, tiledRDD, metadata, pixelwiseMasking = true)

    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD
  }


  private def rasterRegionsToTilesLoadPerProductStrategy(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
                                                         metadata: TileLayerMetadata[SpaceTimeKey],
                                                         retainNoDataTiles: Boolean,
                                                         cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy,
                                                         partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = None,
                                                         datacubeParams: Option[DataCubeParameters] = None,
                                                         expectedBandCount: Int = -1,
                                                         sources: Seq[(RasterSource, Feature)],
                                                         softErrors: Boolean,
                                                        ): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    if (cloudFilterStrategy != NoCloudFilterStrategy) {
      throw new IllegalArgumentException("load_collection: mask_l1c or mask_scl_dilation are not supported by the 'load per product' strategy. Consider using 'to_scl_dilation_mask'.")
    }

    val partitioner = partitionerOption.getOrElse(SpacePartitioner(metadata.bounds))

    logger.info(s"Cube partitioner index: ${partitioner.index}")
    val totalChunksAcc: LongAccumulator = rasterRegionRDD.sparkContext.longAccumulator("ChunkCount_" + rasterRegionRDD.name)
    val tracker = BatchJobMetadataTracker.tracker("")
    tracker.registerCounter(PIXEL_COUNTER)
    val loadingTimeAcc = rasterRegionRDD.sparkContext.doubleAccumulator("SecondsPerChunk_" + rasterRegionRDD.name)
    val crs = metadata.crs
    val layout = metadata.layout

    /**
     * Determine unique sources, to be used for partitioning.
     * Avoid the use of the rdd to simply compute source names, because this triggers a lot of computation which is then repeated later on, even touching the rasters in some cases.
     */
    val allSources: Array[SourceName] = sources.flatMap(t => {
      t._1 match {
        case multibandCompositeRasterSource: MultibandCompositeRasterSource =>
          //decompose into individual bands
          //TODO do something like line below, but make sure that band order is maintained! For now we just return the composite source.
          //source1.sourcesListWithBandIds.map(s => (s._1.name, (s._2,key_region_sourcename._1,GridBoundsRasterRegion(s._1, bounds))))
          Seq(t._1.name)
        case bandCompositeRasterSource: BandCompositeRasterSource =>
          //decompose into individual bands
          bandCompositeRasterSource.sources.map(s => s.name).toList
        case rasterSource =>
          Seq(rasterSource.name)
      }
    }).distinct.toArray

    rasterRegionRDD.sparkContext.setCallSite("load_collection: group by input product")
    val parallelRead = datacubeParams.forall(!_.loadPerProduct)
    val byBandSource: RDD[(SourceName, (Seq[Int], SpaceTimeKey, RasterRegion))] = rasterRegionRDD.flatMap(key_region_sourcename => {
      val key: SpaceTimeKey = key_region_sourcename._1
      val region_sourcename: (RasterRegion, SourceName) = key_region_sourcename._2
      val gridBoundsRasterRegion = region_sourcename._1.asInstanceOf[GridBoundsRasterRegion]
      val source = gridBoundsRasterRegion.source
      val bounds = gridBoundsRasterRegion.bounds
      val result: Seq[(SourceName, (Seq[Int], SpaceTimeKey, RasterRegion))] =
        source match {
          case multibandCompositeRasterSource: MultibandCompositeRasterSource =>
            Seq((multibandCompositeRasterSource.name, (Seq(0), key, gridBoundsRasterRegion)))

          case bandCompositeRasterSource: BandCompositeRasterSource =>
            implicit def order[A <: SourceName]: cats.Order[A] = new cats.Order[A] {
              override def compare(x: A, y: A): Int = {
                // use the same order as allSources
                allSources.indexOf(x) - allSources.indexOf(y)
              }
            }

            val map: Map[SourceName, NonEmptyList[RasterSource]] = bandCompositeRasterSource.sources.groupBy(_.name)
            val nameToRegion: Map[SourceName, GridBoundsRasterRegion] = map.map(t => (t._1, GridBoundsRasterRegion(new BandCompositeRasterSource(t._2, bandCompositeRasterSource.crs, bandCompositeRasterSource.attributes, bandCompositeRasterSource.predefinedExtent, parallelRead = parallelRead, softErrors = softErrors, readFullTile = true), bounds)))
            nameToRegion.toList.sortWith {
              case (a: (SourceName, GridBoundsRasterRegion), b: (SourceName, GridBoundsRasterRegion)) => allSources.indexOf(a._1) < allSources.indexOf(b._1)
            }.zipWithIndex.map(t => (t._1._1, (Seq(t._2), key, t._1._2))).toList.toSeq

          case otherSource =>
            Seq((otherSource.name, (Seq(0), key, gridBoundsRasterRegion)))
        }
      result
    })


    val theCellType = metadata.cellType
    rasterRegionRDD.sparkContext.setCallSite("load_collection: read by input product")
    val partitionedBySource = byBandSource.groupByKey(new ByKeyPartitioner(allSources))
    var tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] = partitionedBySource.mapPartitions((partition: Iterator[(SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])]) => {

      val ((loadedPartition: Iterator[(SpaceTimeKey, (Int, MultibandTile, SourceName))], partitionPixels), duration) = time {
        loadPartitionBySource(partition, cloudFilterStrategy, totalChunksAcc, tracker, crs, layout, theCellType)
      }

      if (partitionPixels > 0) {
        val durationSeconds = duration.toMillis / 1000.0
        val secondsPerChunk = durationSeconds / (partitionPixels / (256 * 256))
        loadingTimeAcc.add(secondsPerChunk)
        val megapixelPerSecond = (partitionPixels / (1024.0 * 1024)) / durationSeconds
        logger.info(s"totalPixelsPartition=$partitionPixels durationSeconds=$durationSeconds megapixelPerSecond=$megapixelPerSecond")
        megapixelPerSecondMeter.set(megapixelPerSecond)
      }
      loadedPartition

    }, preservesPartitioning = true).groupByKey(partitioner).mapValues((tiles: Iterable[(Int, MultibandTile, SourceName)]) => {
      var mergedBands: Map[Int, Option[MultibandTile]] = tiles.groupBy(_._1)
        .map(t => (t._1, t._2.toList.sortBy(x => sortableSourceName(x._3))))
        .view.mapValues(x => x.map(_._2).reduceOption(_ merge _))
        .flatMap { case (index, multiband) => {
          if (multiband.isDefined && multiband.get.bandCount > 1) {
            if (index != 0) {
              throw new NotImplementedError("load_collection: read by input product: no support for reading from multiple multiband assets")
            } else {
              val bandsWithIndex: immutable.Seq[(Tile, Int)] = multiband.get.bands.zipWithIndex
              bandsWithIndex.map(t => (t._2, Some(MultibandTile(t._1))))
            }
          } else {
            Seq[(Int, Option[MultibandTile])]((index, multiband))
          }
        }
        }.toMap
      for (x <- 0 until expectedBandCount) {
        if (!mergedBands.contains(x)) {
          logger.warn("Band " + x + " is missing in the input data. Filling with empty tile.")
          val someTile = mergedBands.head._2.get
          mergedBands = mergedBands + (x -> Some(someTile.prototype(someTile.cols, someTile.rows)))
        }
      }
      MultibandTile(mergedBands.toSeq.sortBy(_._1).flatMap(_._2.get.bands))
    })
    val withEmptyTiles = tiledRDD.mapValues {
      case tile if retainNoDataTiles && tile.bands.forall(_.isNoDataTile) =>
        new EmptyMultibandTile(tile.cols, tile.rows, tile.cellType, tile.bandCount)
      case tile =>
        tile
    }
    tiledRDD = withEmptyTiles.filter { case (_, tile) => retainNoDataTiles || !tile.bands.forall(_.isNoDataTile) }

    rasterRegionRDD.sparkContext.setCallSite("load_collection: apply mask pixel wise")
    tiledRDD = DatacubeSupport.applyDataMask(datacubeParams, tiledRDD, metadata, pixelwiseMasking = true)
    rasterRegionRDD.sparkContext.clearCallSite()
    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD

  }


  private def loadPartition(partitionIterator: Iterator[(SpaceTimeKey, Iterable[(RasterRegion, SourceName)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs: CRS, layout: LayoutDefinition) = {
    var totalPixelsPartition = 0
    val loadedPartitions = partitionIterator.toParArray.map(tuple => {
      val allRegions = tuple._2.toSeq

      val tilesForRegion = allRegions
        .flatMap { case (rasterRegion, sourceName: SourceName) =>
          val result: Option[(MultibandTile, SourceName)] = cloudFilterStrategy match {
            case l1cFilterStrategy: L1CCloudFilterStrategy =>
              if (L1CFunctions.isRegionFullyClouded(rasterRegion, crs, layout, l1cFilterStrategy.bufferInMeters)) {
                // Do not read the tile data at all.
                Option.empty
              } else {
                // Simply mask out the clouds.
                cloudFilterStrategy.loadMasked(maskTileLoader = new MaskTileLoader {
                  override def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] = Option.empty

                  override def loadData: Option[MultibandTile] = {
                    val tile: Option[MultibandTile] = rasterRegion.raster.map(_.tile)
                    if (tile.isDefined) {
                      val compositeRasterSource = rasterRegion.asInstanceOf[GridBoundsRasterRegion].source.asInstanceOf[BandCompositeRasterSource]
                      val cloudRasterSource = (compositeRasterSource.sources.head match {
                        case rsOffset: ValueOffsetRasterSource => rsOffset.rasterSource
                        case indexedRasterSource: IndexedRasterSource => indexedRasterSource.rasterSource
                        case rs => rs
                      }).asInstanceOf[GDALCloudRasterSource]

                      val cloudPolygons: Seq[Polygon] = cloudRasterSource.getMergedPolygons(l1cFilterStrategy.bufferInMeters)
                      val cloudPolygon = MultiPolygon(cloudPolygons) reproject(cloudRasterSource.crs, crs)
                      val cloudTile = Rasterizer.rasterizeWithValue(cloudPolygon, RasterExtent(rasterRegion.extent, tile.get.cols, tile.get.rows), 1)
                      val cloudMultibandTile = MultibandTile(List.fill(tile.get.bandCount)(cloudTile))
                      val maskedTile = tile.get.localMask(cloudMultibandTile, 1, 0).convert(tile.get.cellType)
                      Some(maskedTile)
                    } else Option.empty
                  }
                }).map((_, sourceName))
              }
            case _ =>
              cloudFilterStrategy.loadMasked(new MaskTileLoader {
                override def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] = {
                  val gridBoundsRasterRegion = rasterRegion.asInstanceOf[GridBoundsRasterRegion]
                  val bufferedGridBounds = gridBoundsRasterRegion.bounds.buffer(bufferInPixels, bufferInPixels, clamp = false)

                  val maskOption = gridBoundsRasterRegion.source.read(bufferedGridBounds, Seq(sclBandIndex))

                  maskOption.map { mask =>
                    val expectedTileSize = gridBoundsRasterRegion.cols + 2 * bufferInPixels

                    if (mask.cols == expectedTileSize && mask.rows == expectedTileSize) mask // an optimization really
                    else { // raster can be smaller than requested extent
                      val emptyBufferedRaster: Raster[MultibandTile] = {
                        val bufferedExtent = gridBoundsRasterRegion.source.gridExtent.extentFor(bufferedGridBounds, clamp = false)

                        // warning: convoluted way of creating a NODATA tile
                        val arbitraryNoDataCellType = FloatConstantNoDataCellType
                        val emptyBufferedTile =
                          FloatConstantTile(arbitraryNoDataCellType.noDataValue, cols = expectedTileSize, rows = expectedTileSize, arbitraryNoDataCellType)
                            .toArrayTile() // TODO: not materializing messes up the NODATA value
                            .convert(mask.cellType)

                        Raster(MultibandTile(emptyBufferedTile), bufferedExtent)
                      }

                      emptyBufferedRaster merge mask
                    }
                  }
                }

                override def loadData: Option[MultibandTile] = {
                  for {
                    Raster(tile, _) <- rasterRegion.raster
                  } yield {
                    tile.cellType match {
                      case originalCellType: NoNoData =>
                        val noDataCellType =
                          if (originalCellType.isFloatingPoint) originalCellType.withDefaultNoData()
                          else originalCellType withNoData Some(0)

                        logger.debug(s"converting tile cell type from $originalCellType to $noDataCellType with NODATA")
                        tile convert noDataCellType
                      case _ => tile
                    }
                  }
                }
              }).map((_, sourceName))
          }
          if (result.isDefined) {
            val mbTile = result.get._1
            val totalPixels = mbTile.rows * mbTile.cols * mbTile.bandCount
            totalPixelsPartition += totalPixels
            totalChunksAcc.add(totalPixels / (256 * 256))
            tracker.add(PIXEL_COUNTER, totalPixels)
          }
          result
        }
        .sortWith { case ((leftMultibandTile, leftSourcePath), (rightMultibandTile, rightSourcePath)) =>
          if (leftMultibandTile.band(0).isInstanceOf[PaddedTile] && !rightMultibandTile.band(0).isInstanceOf[PaddedTile]) true
          else if (!leftMultibandTile.band(0).isInstanceOf[PaddedTile] && rightMultibandTile.band(0).isInstanceOf[PaddedTile]) false
          else {
            sortableSourceName(leftSourcePath) < sortableSourceName(rightSourcePath)
          }
        }
        .map { case (multibandTile, _) => multibandTile }
        .reduceOption(_ merge _)
      (tuple._1, tilesForRegion)
    })
    (loadedPartitions, totalPixelsPartition)
  }


  private def loadPartitionBySource(partition: Iterator[(SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs: CRS, layout: LayoutDefinition, cellType: CellType) = {
    var totalPixelsPartition = 0
    val tiles: Iterator[(SpaceTimeKey, (Int, MultibandTile, SourceName))] = partition.flatMap((tuple: (SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])) => {
      val keys = tuple._2.map(_._2).asJavaCollection
      val source = tuple._2.head._3.asInstanceOf[GridBoundsRasterRegion].source
      val bounds = tuple._2.map(_._3.asInstanceOf[GridBoundsRasterRegion].bounds).toSeq
      val intersections: Seq[Option[GridBounds[Long]]] = bounds.map(_.intersection(source.dimensions)).toSeq
      //TODO this assumes that the index is actually the index of this band in the eventual multiband tile, not the index to read from the source
      val theIndex = tuple._2.flatMap(_._1).head

      val allRasters =
        try {
          source.readBounds(bounds).map(_.mapTile {
            _ convert cellType
          }).toSeq
        } catch {
          case e: Exception => throw new IOException(s"load_collection/load_stac: error while reading from: ${source.name.toString}. Detailed error: ${e.getMessage}")
      }

      val totalPixels = allRasters.map(tile => tile.cols * tile.rows * tile.tile.bandCount).sum
      val paddedRasters = allRasters.zipWithIndex.flatMap { case (raster, index) => {
        val intersection = intersections(index)
        val theBounds = bounds(index)
        //apply padding, as done in GridBoundsRasterRegion
        if (intersection.isEmpty) {
          None
        }
        else if (raster.tile.cols == theBounds.width && raster.tile.rows == theBounds.height)
          Some(raster)
        else {
          val colOffset = math.abs(theBounds.colMin - intersection.get.colMin)
          val rowOffset = math.abs(theBounds.rowMin - intersection.get.rowMin)
          require(colOffset <= Int.MaxValue && rowOffset <= Int.MaxValue, "Computed offsets are outside of RasterBounds")
          Some(raster.mapTile {
            //GridBounds(16,0,79,58)
            //coloffset = 16 , rowOffset = 0
            // band = 64 x 59
            //theBounds = 64x64
            //require((chunk.cols (64) + colOffset (16)  <= cols (64)) && (chunk.rows + rowOffset <= rows),
            // chunk at GridBounds(16,0,79,58) exceeds tile boundary at (64, 64)
            _.mapBands { (_, band) => PaddedTile(band, colOffset.toInt, rowOffset.toInt, theBounds.width.toInt, theBounds.height.toInt) }
          })
        }
      }
      }

      totalPixelsPartition += totalPixels
      totalChunksAcc.add(totalPixels / (256 * 256))
      tracker.add(PIXEL_COUNTER, totalPixels)
      keys.iterator().asScala.zip(paddedRasters.map(b => (theIndex, b.tile, tuple._1)).iterator)

    })
    (tiles.toVector.iterator, totalPixelsPartition) // materialize to actually read partition elements and take time
  }

}
