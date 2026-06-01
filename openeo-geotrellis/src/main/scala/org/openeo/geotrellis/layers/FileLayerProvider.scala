package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import com.azavea.gdal.GDALWarp
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, NoNoData, PaddedTile, Raster, RasterExtent, RasterMetadata, RasterRegion, RasterSource, ShortConstantNoDataCellType, SourceName, SourcePath, TargetCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.clip.ClipToGrid
import geotrellis.spark.clip.ClipToGrid.clipFeatureToExtent
import geotrellis.spark.join.VectorJoin
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector
import geotrellis.vector.Extent.toPolygon
import geotrellis.vector._
import _root_.io.opentelemetry.api._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{LongAccumulator, SizeEstimator}
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrellis.OpenEOProcessScriptBuilder.AnyProcess
import org.openeo.geotrellis._
import org.openeo.geotrellis.file.{AbstractPyramidFactory, FixedFeaturesOpenSearchClient}
import org.openeo.geotrellis.layers.provider._
import org.openeo.geotrellis.layers.raster_source.{GDALCloudRasterSource, IndexedRasterSource, NoDataRasterSource, ValueOffsetRasterSource}
import org.openeo.geotrelliscommon.DatacubeSupport.prepareMask
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, CloudFilterStrategy, ConfigurableSpatialPartitioner, DataCubeParameters, DatacubeSupport, L1CCloudFilterStrategy, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceTimePartitioner, autoUtmEpsg}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{IOException, Serializable}
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.time._
import java.time.temporal.ChronoUnit.DAYS
import java.util.concurrent.TimeUnit
import scala.collection.parallel.CollectionsHaveToParArray
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.matching.Regex

/**
 * This class fixes a bug in the original LayoutTileSource
 */
private class LayoutTileSourceFixed[K: SpatialComponent](
                                                          override val source: RasterSource,
                                                          override val layout: LayoutDefinition,
                                                          override val tileKeyTransform: SpatialKey => K
                                                        ) extends LayoutTileSource[K](source, layout, tileKeyTransform) with Serializable {

  override def sourceColOffset: Long = ((source.extent.xmin - layout.extent.xmin) / layout.cellwidth).round

  override def sourceRowOffset: Long = ((layout.extent.ymax - source.extent.ymax) / layout.cellheight).round

}

final case class RasterRegionContext(
  requiredKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])],
  regions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
  metadata: TileLayerMetadata[SpaceTimeKey],
  maskStrategy: Option[CloudFilterStrategy],
  partitioner: Option[SpacePartitioner[SpaceTimeKey]],
  sources: Seq[(RasterSource, Feature)]
) {
  def unpersist(): Unit =
    requiredKeys.unpersist(false)

  // Py4J-friendly getters.
  def hasMaskStrategy: Boolean = maskStrategy.isDefined
  def getMaskStrategy: CloudFilterStrategy = maskStrategy.orNull
  def hasPartitioner: Boolean = partitioner.isDefined
  def getPartitioner: SpacePartitioner[SpaceTimeKey] = partitioner.orNull
  def getSourcesAsJavaList: java.util.List[(RasterSource, Feature)] = {
    sources.asJava
  }
}


object FileLayerProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[FileLayerProvider])


  private lazy val openTelemetry: OpenTelemetry = GlobalOpenTelemetry.get()
  private[layers] lazy val megapixelPerSecondMeter = openTelemetry.meterBuilder("load_collection_read").build().gaugeBuilder("openeo_megapixel_per_second").build()

  {
    try {
      val gdaldatasetcachesize = Integer.valueOf(System.getenv().getOrDefault("GDAL_DATASET_CACHE_SIZE", "32"))
      GDALWarp.init(gdaldatasetcachesize)
    } catch {
      case e: java.lang.UnsatisfiedLinkError =>
        // Error message probably looks like this:
        // "java.lang.UnsatisfiedLinkError: C:\Users\...\gdalwarp_bindings.dll: Can't find dependent libraries"
        // Ignore GDAL init error so that tests that don't require it will be ok.
        // Tests that require it will still crash when it is not installed.
        logger.warn("GDAL library not found: " + e.getMessage)
    }
  }

  def vsis3ToS3(path: String): String = {
    val vsis3Prefix = "/vsis3/eodata/"
    if (path.toLowerCase().startsWith(vsis3Prefix)) {
      "S3://eodata/" + path.substring(vsis3Prefix.length)
    } else {
      path
    }
  }

  // important: make sure to implement object equality for CacheKey's members
  private case class CacheKey(openSearch: OpenSearchClient, openSearchCollectionId: String, rootPath: Path,
                              pathDateExtractor: PathDateExtractor)

  def apply(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
            maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
            bandIndices: Seq[Int] = Seq(), correlationId: String = "", experimental: Boolean = false,
            maxSoftErrorsRatio: Double = 0.0): FileLayerProvider = new FileLayerProvider(
    openSearch, openSearchCollectionId, openSearchLinkTitles, rootPath, maxSpatialResolution, pathDateExtractor,
    attributeValues, layoutScheme, bandIndices, correlationId, experimental, maxSoftErrorsRatio,
    disambiguateConstructors = null
  )

  private def extractDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) =>
      ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
  }

  private def fetchExtentFromOpenSearch(openSearch: OpenSearchClient, collectionId: String): ProjectedExtent = {
    val collection = openSearch.getCollections()
      .find(_.id == collectionId)
      .getOrElse(throw new IllegalArgumentException(s"unknown OpenSearch collection $collectionId"))

    ProjectedExtent(collection.bbox.reproject(LatLng, WebMercator), WebMercator)
  }

  def rasterSourceRDD(rasterSources: Seq[RasterSource], metadata: TileLayerMetadata[SpaceTimeKey], maxSpatialResolution: CellSize, collection: String)(implicit sc: SparkContext): RDD[LayoutTileSource[SpaceTimeKey]] = {

    val keyExtractor = new TemporalKeyExtractor {
      def getMetadata(rs: RasterMetadata): ZonedDateTime = ZonedDateTime.parse(rs.attributes("date")).truncatedTo(DAYS)
    }
    val sources = sc.parallelize(rasterSources, rasterSources.size)

    val noResampling = metadata.crs.proj4jCrs.getProjection.getName == "utm" && math.abs(metadata.layout.cellSize.resolution - maxSpatialResolution.resolution) < 0.0000001 * metadata.layout.cellSize.resolution
    sc.setJobDescription("Load tiles: " + collection + ", rs: " + noResampling)
    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => SpaceTimeKey = { sk => keyExtractor.getKey(m, sk) }
        //The first form 'rs.tileToLayout' will check if rastersources are aligned, requiring reading of metadata, which has a serious performance impact!
        try {
          if (noResampling)
            LayoutTileSource(rs, metadata.layout, tileKeyTransform)
          else
            rs.tileToLayout(metadata.layout, tileKeyTransform)
        } catch {
          case e: IllegalArgumentException => {
            logger.error(s"Error tiling rastersource ${rs.name} to layout: ${metadata.layout}, ${rs.gridExtent}, ${rs.cellSize}")
            throw e
          }
        }
      }

    tiledLayoutSourceRDD
  }

  private def checkLatLon(extent: Extent): Boolean = {
    if (extent.xmin < -360 || extent.xmax > 360 || extent.ymin < -92 || extent.ymax > 92) {
      false
    } else {
      true
    }
  }

  /**
   * TODO: use generics to have one function for SpatialKey and SpacetimeKey
   * @param datacubeParams
   * @param requiredSpacetimeKeys
   * @param metadata
   * @return
   */
  def applySpaceTimeMask(datacubeParams: Option[DataCubeParameters], requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])], metadata: TileLayerMetadata[SpaceTimeKey]): RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = {
    if (datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject = datacubeParams.get.maskingCube.get
      requiredSpacetimeKeys.sparkContext.setCallSite("load_collection: filter mask keys")
      maskObject match {
        case theMask: MultibandTileLayerRDD[SpaceTimeKey] =>
          if (theMask.metadata.bounds.get._1.isInstanceOf[SpaceTimeKey]) {

            //TODO: this partioner is none most of the time
            // Perhaps try using the partitioner from the mask, but only valid after reprojection
            val partitioner = requiredSpacetimeKeys.partitioner
            // filtered mask to tiles with at least one valid pixel, remove others, so need to perform inner join
            val filtered = prepareMask(theMask, metadata, partitioner)

            if (logger.isDebugEnabled) {
              // the number of jobs/stages effectively depends on whether logging is correctly configured
              logger.debug(s"SpacetimeMask mask reduces the input to: ${filtered.countApproxDistinct()} keys.")
            }

            datacubeParams.get.maskingCube = Some(filtered)
            val result = requiredSpacetimeKeys.join(filtered).map(tuple => (tuple._1, tuple._2._1))
            requiredSpacetimeKeys.sparkContext.clearCallSite()
            return result
          }
        case _ =>
      }
    }
    return requiredSpacetimeKeys
  }

  def applySpatialMask[M](datacubeParams: Option[DataCubeParameters], requiredSpatialKeys: RDD[(SpatialKey, M)], metadata: TileLayerMetadata[SpaceTimeKey])(implicit vt: ClassTag[M]): RDD[(SpatialKey, M)] = {
    if (datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject = datacubeParams.get.maskingCube.get
      maskObject match {
        case theSpatialMask: MultibandTileLayerRDD[SpatialKey] =>
          if (theSpatialMask.metadata.bounds.get._1.isInstanceOf[SpatialKey]) {
            val filtered = theSpatialMask.withContext {
              _.filter(_._2.band(0).toArray().exists(pixel => pixel == 0)).distinct()
            }
            val maskSpatialKeys =
              if (theSpatialMask.metadata.crs.equals(metadata.crs) && theSpatialMask.metadata.layout.equals(metadata.layout)) {
                filtered
              } else {
                logger.debug(s"mask: automatically resampling mask to match datacube: ${theSpatialMask.metadata}")
                filtered.reproject(metadata.crs, metadata.layout, 16, requiredSpatialKeys.partitioner)._2
              }
            if (logger.isDebugEnabled) {
              logger.debug(s"Spatial mask reduces the input to: ${maskSpatialKeys.countApproxDistinct()} keys.")
            }
            return requiredSpatialKeys.join(maskSpatialKeys).map(tuple => (tuple._1, tuple._2._1))
          }
        case _ =>
      }
    }
    return requiredSpatialKeys
  }

  private def tileSourcesToDataCube(rasterSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey], requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])], sc: SparkContext, retainNoDataTiles: Boolean, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, useSparsePartitioner: Boolean = true, datacubeParams : Option[DataCubeParameters] = None, inputFeatures: Option[Seq[Feature]] = None): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val localSpatialKeys = applySpatialMask(datacubeParams,requiredSpatialKeys,metadata)

    var spatialKeyCount = localSpatialKeys.countApproxDistinct()

    // Remove all source files that do not intersect with the 'interior' of the requested extent.
    // Note: A normal intersect would also include sources that exactly border the requested extent.
    val filteredSources: RDD[LayoutTileSource[SpaceTimeKey]] = rasterSources.filter({ tiledLayoutSource =>
      tiledLayoutSource.source.extent.interiorIntersects(tiledLayoutSource.layout.extent)
    })

    val partitioner = createPartitioner(datacubeParams, localSpatialKeys, filteredSources, metadata)

    //use spatialkeycount as heuristic to choose code path

    var requestedRasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))]  =
    if(spatialKeyCount < 1000) {
      val keys = sc.broadcast(requiredSpatialKeys.map(_._1).collect())
      filteredSources
        .flatMap { tiledLayoutSource =>
          {
            val spaceTimeKeys: Array[SpaceTimeKey] = keys.value.map(tiledLayoutSource.tileKeyTransform(_))
            spaceTimeKeys
              .map(key => (key, tiledLayoutSource.rasterRegionForKey(key))).filter(_._2.isDefined).map(t=>(t._1,t._2.get))
              .filter({case(key, rasterRegion) => metadata.extent.interiorIntersects(key.spatialKey.extent(metadata.layout)) } )
              .map { case (key, rasterRegion) => (key, (rasterRegion, tiledLayoutSource.source.name)) }
          }
        }
    }else{
      // Convert RasterSources to RasterRegions.
      val rasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] =
        filteredSources
          .flatMap { tiledLayoutSource =>
            tiledLayoutSource.keyedRasterRegions()
              //this filter step reduces the 'Shuffle Write' size of this stage, so it already
              .filter({case(key, rasterRegion) => metadata.extent.interiorIntersects(key.spatialKey.extent(metadata.layout)) } )
              .map { case (key, rasterRegion) => (key, (rasterRegion, tiledLayoutSource.source.name)) }
          }

      // Only use the regions that correspond with a requested spatial key.

        rasterRegions
          .map { tuple => (tuple._1.spatialKey, tuple) }
          //for sparse keys, this takes a silly amount of time and memory. Just broadcasting spatialkeys and filtering on that may be a lot easier...
          //stage boundary, first stage of data loading ends here!
          .join[Null](requiredSpatialKeys.map(t=>(t._1,null))).map { t => t._2._1 }

    }

    requestedRasterRegions.name = rasterSources.name
    rasterRegionsToTiles(requestedRasterRegions, metadata, retainNoDataTiles, cloudFilterStrategy, partitioner, datacubeParams)
  }


  private def productsToSpatialKeys(inputFeatures: Option[Seq[Feature]], metadata: TileLayerMetadata[SpaceTimeKey], sc: SparkContext) = {
    inputFeatures.get.foreach(f => {
      val extent = f.geometry.getOrElse(f.bbox.toPolygon()).extent
      if (!checkLatLon(extent)) throw new IllegalArgumentException(s"Geometry or Bounding box provided by the catalog has to be in EPSG:4326, but got ${extent} for catalog entry ${f}")
    })

    //avoid computing keys that are anyway out of bounds, with some buffering to avoid throwing away too much
    val boundsLatLng = ProjectedExtent(metadata.extent, metadata.crs).reproject(LatLng).buffer(0.0001).toPolygon()
    val geometricFeatures = inputFeatures.get.map(f => geotrellis.vector.Feature(f.geometry.getOrElse(f.bbox.toPolygon()), f))
    val keysForfeatures: RDD[(SpatialKey, vector.Feature[Geometry, Feature])] = sc.parallelize(geometricFeatures, math.max(1, geometricFeatures.size)).map(_.mapGeom(_.intersection(boundsLatLng)).reproject(LatLng, metadata.crs))
      .clipToGrid(metadata)
    keysForfeatures
  }

  def convertNetcdfLinksToGDALFormat(link: Link, bandName: String, bandIndex: Int) = {
    // 1 netCDF asset can contain n bands, but a GDALRasterSource can only handle 1 band/wants the
    //  band embedded in the path: NETCDF:$href:$bandName
    if ((link.href.toString contains ".nc") && !link.href.toString.startsWith("NETCDF:")) {
      val netCdfDataset = {
        if(link.href.getScheme == "file") {
          s"NETCDF:${link.href.getPath}:$bandName"
        }else{
          //note that /vsicurl/ is added for http urls later on, perhaps this can also happen here?
          s"NETCDF:${link.href}:$bandName"
        }
      }
      val netCdfDatasetBandIndex = 0
      Some((link.copy(href = URI.create(netCdfDataset)), netCdfDatasetBandIndex))
    } else if ((link.href.toString contains ".hdf") && !link.href.toString.startsWith("HDF4:")) {
      val hdfDataset = {
        if(link.href.getScheme == "file") {
          s"${link.href.getPath}:MOD_Grid_Snow_500m:$bandName"
        }else{
          s"${link.href}:MOD_Grid_Snow_500m:$bandName"
        }
      }
      Some((link.copy(href = URI.create(hdfDataset)), 0))
    } else Some((link, bandIndex))
  }

  def createPartitioner(datacubeParams: Option[DataCubeParameters], requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])], filteredSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey]): Some[SpacePartitioner[SpaceTimeKey]] = {
    val requiredSpacetimeKeys: RDD[SpaceTimeKey] = filteredSources.flatMap(_.keys).map {
      tuple => (tuple.spatialKey, tuple)
    }.rightOuterJoin(requiredSpatialKeys).flatMap(_._2._1.toList)
    DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys, metadata)
  }


  private val PIXEL_COUNTER = "InputPixels"

  private def rasterRegionsToTiles(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
                                   metadata: TileLayerMetadata[SpaceTimeKey],
                                   retainNoDataTiles: Boolean,
                                   cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy,
                                   partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = None,
                                   datacubeParams : Option[DataCubeParameters] = None,
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
    tiledRDD = DatacubeSupport.applyDataMask(datacubeParams,tiledRDD,metadata, pixelwiseMasking = true)

    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD
  }


  private def loadPartitionBySource(partitionIterator: Iterator[(SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs :CRS, layout:LayoutDefinition, cellType: CellType )= {
    var totalPixelsPartition = 0
    val tiles: Iterator[(SpaceTimeKey, (Int,MultibandTile, SourceName))] = partitionIterator.flatMap((tuple: (SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])) =>{
      val keys = tuple._2.map(_._2).asJavaCollection
      val source = tuple._2.head._3.asInstanceOf[GridBoundsRasterRegion].source
      val bounds = tuple._2.map(_._3.asInstanceOf[GridBoundsRasterRegion].bounds).toSeq
      val intersections: Seq[Option[GridBounds[Long]]] = bounds.map(_.intersection(source.dimensions)).toSeq
      //TODO this assumes that the index is actually the index of this band in the eventual multiband tile, not the index to read from the source
      val theIndex = tuple._2.flatMap(_._1).head

      val allRasters =
        try {
          source.readBounds(bounds).map(_.mapTile { _ convert cellType }).toSeq
        } catch {
          case e: Exception => throw new IOException(s"load_collection/load_stac: error while reading from: ${source.name.toString}. Detailed error: ${e.getMessage}")
        }

      val totalPixels = allRasters.map(tile => tile.cols * tile.rows * tile.tile.bandCount).sum
      val paddedRasters = allRasters.zipWithIndex.flatMap {case (raster,index) => {
        val intersection = intersections(index)
        val theBounds = bounds(index)
        //apply padding, as done in GridBoundsRasterRegion
        if(intersection.isEmpty) {
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
      }}

      totalPixelsPartition += totalPixels
      totalChunksAcc.add(totalPixels / (256 * 256))
      tracker.add(PIXEL_COUNTER, totalPixels)
      keys.iterator().asScala.zip(paddedRasters.map(b=>(theIndex,b.tile,tuple._1)).iterator)

    })
    (tiles,totalPixelsPartition)
  }

  private def loadPartition(partitionIterator: Iterator[(SpaceTimeKey, Iterable[(RasterRegion, SourceName)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs :CRS, layout:LayoutDefinition ) = {
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
                cloudFilterStrategy.loadMasked(new MaskTileLoader {
                  override def loadMask(bufferInPixels: Int, sclBandIndex: Int): Option[Raster[MultibandTile]] = Option.empty

                  override def loadData: Option[MultibandTile] = {
                    val tile: Option[MultibandTile] = rasterRegion.raster.map(_.tile)
                    if (tile.isDefined) {
                      val compositeRasterSource = rasterRegion.asInstanceOf[GridBoundsRasterRegion].source.asInstanceOf[BandCompositeRasterSource]
                      val cloudRasterSource = (compositeRasterSource.sources.head match {
                        case rsOffset: ValueOffsetRasterSource => rsOffset.rasterSource
                        case rs => rs
                      }).asInstanceOf[GDALCloudRasterSource]

                      val cloudPolygons: Seq[Polygon] = cloudRasterSource.getMergedPolygons(l1cFilterStrategy.bufferInMeters)
                      val cloudPolygon = MultiPolygon(cloudPolygons).reproject(cloudRasterSource.crs, crs)
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
    (loadedPartitions,totalPixelsPartition)
  }

  /**
   * use static function for rdd construction to try and reduce task deserialization time
   * @param sc
   * @param keys
   * @return
   */
  private def keysRDD(sc: SparkContext, keys: Set[SpatialKey]): RDD[(SpatialKey, Iterable[Geometry])] = {
    sc.parallelize(keys.toSeq, 1).map((_, null))
  }

  private def featuresRDD(geometricFeatures: Seq[vector.Feature[Geometry, (RasterSource, Feature)]], metadata: TileLayerMetadata[SpaceTimeKey], targetCRS: CRS,  maybeKeys: Option[RDD[(SpatialKey, Iterable[Geometry])]], sc: SparkContext, datacubeParams: Option[DataCubeParameters]) = {
    val cubeExtent = metadata.extent

    val inputNumberOfPartitions = if(maybeKeys.isDefined) {
      //spatial keys are already known and will determine partitioning?
      10
    }else{
      //cliptogrid generates a lot of keys, so requires more memory
      math.max(1, geometricFeatures.size)
    }

    val clippedFeatures: RDD[vector.Feature[Geometry, (RasterSource, Feature)]] = sc.parallelize(geometricFeatures, inputNumberOfPartitions)
      .flatMap { case vector.Feature(productGeometry, data @ (_, feature)) =>
        val productCRSOrDefault = feature.crs.getOrElse(targetCRS)
        val intersection =
          try {
            val intersection = if (datacubeParams.getOrElse(new DataCubeParameters).useNewFeatureExtentIntersection2) {
              val productGeometryProjected = ProjectedPolygons(productGeometry, LatLng).safeReproject(productCRSOrDefault, refine = true)

              val cubeExtentCrs = ProjectedExtent(cubeExtent, targetCRS)
              val cubeExtentPolygon = safeReprojectToPolygon(cubeExtentCrs, productCRSOrDefault)

              productGeometryProjected.getFlatMultiPolygon.intersection(cubeExtentPolygon.getFlatMultiPolygon)
            } else {
              productGeometry.reproject(LatLng, productCRSOrDefault).intersection(cubeExtent.reprojectAsPolygon(targetCRS, productCRSOrDefault, 0.01))
            }

            if (intersection.isValid && intersection.getArea > 0.0)
              Some(intersection.reproject(productCRSOrDefault, targetCRS))
            else {
              // consider rasterExtent as a better representation of an item's geometry in its native CRS
              (feature.rasterExtent, feature.crs) match {
                case (Some(rasterExtent), Some(crs)) if crs == targetCRS =>
                  val intersection = rasterExtent.toPolygon() intersection cubeExtent.toPolygon()
                  if (intersection.isValid && intersection.getArea > 0.0) Some(intersection)
                  else None
                case _ => None
              }
            }
          } catch {
            case e: Exception => logger.warn("Exception while determining intersection.", e); None
          }

        intersection.map(vector.Feature(_, data))
      }

    if(maybeKeys.isDefined) {
      val transform = metadata.mapTransform
      val geometryToKey: RDD[vector.Feature[Polygon, SpatialKey]] = maybeKeys.get.keys.map(k=>{
        vector.Feature(transform.apply(k).toPolygon(),k)
      })

      implicit val theContext: SparkContext = sc
      val joined: RDD[(vector.Feature[Geometry, (RasterSource, Feature)], vector.Feature[Polygon, SpatialKey])] = VectorJoin(clippedFeatures,geometryToKey, (a, b)=>{a.intersects(b)})
      joined.map(t=>(t._2.data,t._1))

    }else{
      val metadataCubePartitioner = SpacePartitioner(metadata.bounds.get.toSpatial)(implicitly,implicitly,new ConfigurableSpatialPartitioner(3))
      clippedFeatures.clipToGrid(metadata.layout).partitionBy(metadataCubePartitioner)
    }

  }

  private val metadataCache =
    Caffeine.newBuilder()
      .refreshAfterWrite(15, TimeUnit.MINUTES)
      .build(new CacheLoader[CacheKey, Option[(ProjectedExtent, Array[ZonedDateTime])]] {
        override def load(key: CacheKey): Option[(ProjectedExtent, Array[ZonedDateTime])] = {
          val bbox = fetchExtentFromOpenSearch(key.openSearch, key.openSearchCollectionId)
          val dates = key.pathDateExtractor.extractDates(key.rootPath)

          Some(bbox, dates)
        }
      })
}

class FileLayerProvider private(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
                        maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any], layoutScheme: LayoutScheme,
                        bandIndices: Seq[Int], correlationId: String, experimental: Boolean,
                        maxSoftErrorsRatio: Double, disambiguateConstructors: Null) extends LayerProvider { // workaround for: constructors have the same type after erasure

  import DatacubeSupport._
  import FileLayerProvider._

  @deprecated("call a constructor/factory method with flattened bandIndices instead of nested bandIds")
  // TODO: remove this eventually (e.g. after updating geotrellistimeseries)
  def this(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
           maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
           bandIds: Seq[Seq[Int]] = Seq(), correlationId: String = "", experimental: Boolean = false,
           maxSoftErrorsRatio: Double = 0.0) = this(openSearch, openSearchCollectionId,
           openSearchLinkTitles = NonEmptyList.fromListUnsafe(for {
             (title, bandIndices) <- openSearchLinkTitles.toList.zipAll(bandIds, thisElem = "", thatElem = Seq(0))
             _ <- bandIndices
           } yield title),
           rootPath, maxSpatialResolution, pathDateExtractor, attributeValues, layoutScheme,
           bandIndices = bandIds.flatten,
           correlationId, experimental,
           maxSoftErrorsRatio, disambiguateConstructors = null)

  assert(bandIndices.isEmpty || bandIndices.size == openSearchLinkTitles.size)

  if(experimental) {
    logger.warn("Experimental features enabled for: " + openSearchCollectionId)
  }

  private val _rootPath = if(rootPath != null) Paths.get(rootPath) else null
  private val fromLoadStac = openSearch.isInstanceOf[FixedFeaturesOpenSearchClient]
  private val softErrors = maxSoftErrorsRatio > 0.0
  private val rasterSourceProviderChain: Seq[RasterSourceProvider] = List(SyntheticDataRasterSourceProvider, SentinelXmlMetadataRasterSourceProvider, ZarrRasterSourceProvider, HDFRasterSourceProvider, NetCDFRasterSourceProvider, JPEGRasterSourceProvider, DefaultRasterSourceProvider)

  private val openSearchLinkTitlesWithBandId: Seq[(String, Int)] = {
    if (bandIndices.nonEmpty) {
      //case 1: PROBA-V, geotiff file containing multiple bands, bandids parameter is used to indicate which bands to load
      openSearchLinkTitles.toList zip bandIndices
    } else {
      //case 2: Sentinel-2 angle metadata: band number is encoded in the oscars link title directly, maybe proba could use this system as well...
      openSearchLinkTitles
        .map { title =>
          val Array(t, bandIndex @ _*) = title.split("##")
          (t, if (bandIndex.nonEmpty) bandIndex.head.toInt else 0)
        }
        .toList
    }
  }

  val maxZoom: Int = layoutScheme match {
    case z: ZoomedLayoutScheme => z.zoom(0, 0, maxSpatialResolution)
    case _ => 14
  }

  def determineCelltype(overlappingRasterSources: Seq[(RasterSource, Feature)]): CellType = {
    val (arbitraryRasterSource, _) = overlappingRasterSources.head
    try {
      val commonCellType = arbitraryRasterSource.cellType
      commonCellType match {
        case integralNoNoData: NoNoData if !integralNoNoData.isFloatingPoint => commonCellType.withNoData(Some(0))
        case _: NoNoData => commonCellType.withDefaultNoData()
        case _ => commonCellType
      }
    } catch {
      case e: Exception => {
        // Geotrellis GDALException errors are not descriptive enough. Attempt to add some more useful information.
        var fileExistsMessage = ""
        try {
          val path = Paths.get(arbitraryRasterSource.name match {
            case p: SourcePath => {
              if (p.value.startsWith("NETCDF:")) {
                // Netcdf files can specify a variable using NETCDF:/file/path:variablename
                p.value.replace("NETCDF:", "").split(":").head
              } else {
                p.value
              }
            }
            case _ => "Path could not be determined"
          })
          fileExistsMessage = s"File ${if (Files.exists(path)) "exists" else "does not exist"}: $path."
        } catch {
          case e2: Exception => {
            fileExistsMessage = s"Exception while trying to determine if RasterSource path exists: ${e2.getMessage}."
          }
        }
        throw new IOException(s"Exception while reading RasterSource ${arbitraryRasterSource.name} in collection $openSearchCollectionId. Detailed message: ${e.getMessage}. $fileExistsMessage", e)
      }
    }
  }

  def readKeysToRasterSources(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon], polygons_crs: CRS, zoom: Int, sc: SparkContext, datacubeParams : Option[DataCubeParameters]): (RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])], TileLayerMetadata[SpaceTimeKey], Option[CloudFilterStrategy], Seq[(RasterSource, Feature)]) = {
    val multiple_polygons_flag = polygons.length > 1

    val buffer = math.max(datacubeParams.map(_.pixelBufferX).getOrElse(0.0), datacubeParams.map(_.pixelBufferY).getOrElse(0.0))
    val bufferedPolygons: Array[MultiPolygon]=
      if(buffer >0) {
        AbstractPyramidFactory.preparePolygons(polygons, polygons_crs, sc,bufferSize = buffer * maxSpatialResolution.resolution)
      }else{
        polygons
      }

    val fullBBox = ProjectedExtent(bufferedPolygons.toSeq.extent,polygons_crs)
    val selectedLayoutScheme: LayoutScheme = selectLayoutScheme(fullBBox, multiple_polygons_flag, datacubeParams)
    val worldLayout: LayoutDefinition = DatacubeSupport.getLayout(selectedLayoutScheme, fullBBox, zoom min maxZoom, maxSpatialResolution, globalBounds = datacubeParams.flatMap(_.globalExtent), multiple_polygons_flag = multiple_polygons_flag)
    val reprojectedBoundingBox: ProjectedExtent = DatacubeSupport.targetBoundingBox(fullBBox, layoutScheme)
    val alignedExtent = worldLayout.createAlignedRasterExtent(reprojectedBoundingBox.extent)


    logger.info(s"Loading ${openSearchCollectionId} with params ${datacubeParams.getOrElse(new DataCubeParameters)} and bands ${openSearchLinkTitles.toList.mkString(";")} initial layout: ${worldLayout}")

    var overlappingRasterSources: Seq[(RasterSource, Feature)] = loadRasterSourceRDD(ProjectedExtent(alignedExtent.extent,reprojectedBoundingBox.crs), from, to, zoom, datacubeParams, Some(worldLayout.cellSize))

    val dates = overlappingRasterSources.map(_._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC"))).distinct

    //Feature objects will be part of RDD, remove potentially large metadata that is no longer needed beyond this point
    //Certain STAC items can have large number of links!
    overlappingRasterSources = overlappingRasterSources.map(source_feature => (source_feature._1,source_feature._2.copy(links = Array.empty, generalProperties = null)))

    val commonCellType: CellType = determineCelltype(overlappingRasterSources)

    var metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, dates.minBy(_.toEpochSecond), dates.maxBy(_.toEpochSecond), commonCellType)
    val spatialBounds = metadata.bounds.get.toSpatial
    val maxSpatialKeyCount = (spatialBounds.maxKey.col - spatialBounds.minKey.col + 1) * (spatialBounds.maxKey.row - spatialBounds.minKey.row + 1)
    val targetCRS = metadata.crs
    val isUTM = targetCRS.proj4jCrs.getProjection.getName == "utm"

    // Handle maskingStrategyParameters.
    var maskStrategy: Option[CloudFilterStrategy] = None
    if (datacubeParams.isDefined && datacubeParams.get.maskingStrategyParameters != null) {
      val maskParams = datacubeParams.get.maskingStrategyParameters
      val maskMethod = maskParams.getOrDefault("method", "").toString
      if (maskMethod == "mask_scl_dilation") {
        maskStrategy = for {
          (_, sclBandIndex) <- openSearchLinkTitles.zipWithIndex.find {
            case (linkTitle, _) => linkTitle.contains("SCENECLASSIFICATION") || linkTitle.contains("SCL")
          }
        } yield new SCLConvolutionFilterStrategy(sclBandIndex, datacubeParams.get.maskingStrategyParameters)
      }
      else if (maskMethod == "mask_l1c") {
        overlappingRasterSources = L1CFunctions.filterRasterSources(overlappingRasterSources, maskParams)
        maskStrategy = Some(new L1CCloudFilterStrategy(L1CFunctions.getDilationDistance(maskParams.asScala.toMap)))
      }
    }

    sc.setCallSite(s"load_collection: $openSearchCollectionId resolution $maxSpatialResolution construct input product metadata" )

    val requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] =
      if(maxSpatialKeyCount<=2 && bufferedPolygons.length==1) {
        //reduce complexity for small (synchronous) requests
        val keys = metadata.keysForGeometry(toPolygon(metadata.extent))
        keysRDD(sc, keys)
      }else{
        val polygonsRDD = sc.parallelize(bufferedPolygons, math.max(1, bufferedPolygons.length / 2)).map {
          _.reproject(polygons_crs, targetCRS)
        }

        val clipped: RDD[(SpatialKey, Geometry)] = clipToGridWithErrorHandling(polygonsRDD, metadata)

        val spatialKeyCount: Long =
          if (polygons.length == 1) {
            //special case for single bbox request
            maxSpatialKeyCount
          } else {
            clipped.map(_._1).countApproxDistinct()
          }
        logger.info(s"Datacube requires approximately ${spatialKeyCount} spatial keys.")

        val metadataCubePartitioner: Partitioner = {
          if(spatialKeyCount.floatValue() / maxSpatialKeyCount.floatValue() < 0.5) {
            // here we attempt to avoid creating a partitioner with a large amount of empty partitions, in case we are
            // processing a low number of spatial keys. This can happen with sparse data loading.
            new HashPartitioner(math.max((spatialKeyCount / 100).intValue(),1))
          }else{

            /**
             * Max size of metadata partition depends on the number of items returned by the catalog.
             * Too many partitions requires extra executors, often at the very beginning of a job, so we try to limit this.
             * We use the number of dates as a proxy for the max number of items intersecting a given spatial key.
             * For cases like Sentinel-2, we can however have  items intersecting at a given location on the same date.
             */
            val maxPartitionSizeBytes = 40 * 1024 * 1024
            val sampleCount = math.min(10, overlappingRasterSources.size)
            val averageItemSizeInBytes = overlappingRasterSources.take(sampleCount).map(item => SizeEstimator.estimate(item)).sum / sampleCount
            val estimatedSizePerKey = averageItemSizeInBytes * dates.length
            val maxSpatialKeysPerPartition = maxPartitionSizeBytes / estimatedSizePerKey
            val indexReduction = math.max(math.ceil(math.log(maxSpatialKeysPerPartition) / math.log(2)).toInt - 1, 1)
            SpacePartitioner(metadata.bounds.get.toSpatial)(implicitly,implicitly,new ConfigurableSpatialPartitioner(indexReduction))
          }
        }

        var requiredSpatialKeysLocal: RDD[(SpatialKey, Iterable[Geometry])] = clipped.groupByKey(metadataCubePartitioner)


        val retiledMetadata: Option[TileLayerMetadata[SpaceTimeKey]] = DatacubeSupport.optimizeChunkSize(metadata, bufferedPolygons, datacubeParams, spatialKeyCount)
        metadata = retiledMetadata.getOrElse(metadata)

        if (retiledMetadata.isDefined) {
          requiredSpatialKeysLocal = clipToGridWithErrorHandling(polygonsRDD, retiledMetadata.get).groupByKey(metadataCubePartitioner)
        }
        requiredSpatialKeysLocal
      }


    overlappingRasterSources.map(_._2).foreach(f => {
      val extent = f.geometry.getOrElse(f.bbox.toPolygon()).extent
      if (!checkLatLon(extent)) throw new IllegalArgumentException(s"Geometry or Bounding box provided by the catalog has to be in EPSG:4326, but got ${extent} for catalog entry ${f}")
    })


    //extra check on interior, disabled because it requires an (expensive) lookup of the extent
    /*
    overlappingRasterSources = overlappingRasterSources.filter({ t =>
      t._1.extent.interiorIntersects(cubeExtent)
    })*/

    //avoid computing keys that are anyway out of bounds, with some buffering to avoid throwing away too much

    val geometricFeatures = overlappingRasterSources.map(f => geotrellis.vector.Feature(f._2.geometry.getOrElse(f._2.bbox.toPolygon()), f))


    val keysIfSparse: Option[RDD[(SpatialKey, Iterable[Geometry])]] =
      if (maxSpatialKeyCount > 2) {
        Some(requiredSpatialKeys)
      } else {
        None
      }

    val griddedRasterSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] =  featuresRDD(geometricFeatures, metadata, targetCRS, keysIfSparse, sc, datacubeParams)


    val filteredSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] = applySpatialMask(datacubeParams, griddedRasterSources,metadata)


    var requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = filteredSources.map(t => (SpaceTimeKey(t._1, TemporalKey(t._2.data._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC")))), t._2))

    requiredSpacetimeKeys = applySpaceTimeMask(datacubeParams, requiredSpacetimeKeys,metadata)

    if (isUTM && datacubeParams.forall(_.resolveTileOverlap)) {
      //only for utm is just a safeguard to limit to Sentinel-1/2 for now
      //try to resolve overlap before actually reading the data
      requiredSpacetimeKeys = requiredSpacetimeKeys
        .groupByKey().flatMap { case (key, sources) =>
          lazy val return_original = sources.map(source => (key, source))

          if (sources.size == 1)
            return_original
          else {
            val keyExtent = metadata.keyToExtent(key.spatialKey)

            val distances = sources.map { case source @ vector.Feature(_, (_, feature)) =>
              //try to detect tiles that are on the edge of the footprint
              val sourceFootprint = feature.geometry.getOrElse(feature.bbox.toPolygon()).reproject(LatLng, targetCRS)
              /**-
               * Effect of buffer multiplication factor:
               *  - larger buffer -> shrink source footprint more -> tiles close to edge get discarded faster, this matters for scl_dilation
               */
              val fullyContained = sourceFootprint.contains(keyExtent)

              val shrunkSourceFootprint = sourceFootprint.buffer(-1.5 * math.max(keyExtent.width, keyExtent.height))
              val distanceToFootprint =
                if (shrunkSourceFootprint.isEmpty)
                  if (fullyContained) keyExtent.distance(sourceFootprint.getCentroid)
                  else keyExtent.distance(sourceFootprint.getCentroid) + 0.00001 //avoid that distance become zero
                else
                  keyExtent.distance(shrunkSourceFootprint)

              val sourceExtent = sourceFootprint.extent
              val distanceBetweenCenters = keyExtent.center.distance(sourceExtent.center)
              ((distanceBetweenCenters, distanceToFootprint, fullyContained), source)
            }

            val smallestDistanceToFootprint = distances
              .map { case ((_, distanceToFootprint, _ ), _) => distanceToFootprint }.min

            if (smallestDistanceToFootprint > 0) {
              val fullyContained = distances
                .filter { case ((_, _, contains), _) => contains }
                .map { case (_, source) => (key, source) }

              if (fullyContained.nonEmpty) fullyContained
              else return_original
            } else {

              /**
               * In case of overlap, we want to select the extent that is either fully inside the footprint
               * Or, in case multiple sources satisfy the distance constraint, we prefer the one that has a CRS matching the target CRS
               *
               */

              val filteredByDistance = distances.filter { case ((_, distanceToFootprint, _), _) =>
                distanceToFootprint == 0
              }

              val filteredByCRS = filteredByDistance.filter { case (_, vector.Feature(_, (_, feature))) =>
                feature.crs contains targetCRS
              }

              if (filteredByCRS.nonEmpty)
                filteredByCRS.map { case (_, source) => (key, source) }
              else
                Seq(filteredByDistance.minBy { case ((centerDistance, _, _), _) => centerDistance })
                  .map { case (_, source) => (key, source) }
            }
          }
        }
    }

    (requiredSpacetimeKeys, metadata, maskStrategy, overlappingRasterSources)
  }


  private def clipToGridWithErrorHandling(polygonsRDD: RDD[MultiPolygon], metadata: TileLayerMetadata[SpaceTimeKey]) = {
    // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
    val polygonFeatureRDD: RDD[vector.Feature[MultiPolygon, Unit]] = polygonsRDD.map(vector.Feature(_, ()))
    val clippingFunction: (Extent, vector.Feature[MultiPolygon, Unit], ClipToGrid.Predicates) => Option[vector.Feature[Geometry, Unit]] = (e, f, p) => {
      try {
        clipFeatureToExtent[MultiPolygon, Unit](e, f, p)
      } catch {
        case ex: Exception => throw new IOException(s"load_collection/load_stac: internal error while clipping input geometry ${f.geom} to extent ${e}. Original message: ${ex.getMessage} ", ex)
      }

    }
    val clipped = ClipToGrid.apply[MultiPolygon, Unit](rdd = polygonFeatureRDD, layout = metadata.layout, clipFeature = clippingFunction).mapValues(_.geom)
    clipped
  }

  def nextPowerOfTwo(n: Int): Int = {
    if (n <= 0) 1
    else 1 << (32 - Integer.numberOfLeadingZeros(n - 1))
  }

  def selectLayoutScheme(extent: ProjectedExtent, multiple_polygons_flag: Boolean, datacubeParams: Option[DataCubeParameters]) = {
    val selectedLayoutScheme = if (layoutScheme.isInstanceOf[FloatingLayoutScheme]) {
      if( (extent.extent.width <= maxSpatialResolution.width) || (extent.extent.height <= maxSpatialResolution.height ) ){
        FloatingLayoutScheme(32)
      }else{
        val rasterExtent = RasterExtent(datacubeParams.map(_.globalExtent.getOrElse(extent).reproject(extent.crs)).getOrElse(extent.extent), maxSpatialResolution)
        val minTiles = math.min(math.floor(rasterExtent.rows / 256), math.floor(rasterExtent.cols / 256)).toInt
        val tileSize:Int = {
          if (datacubeParams.isDefined && datacubeParams.get.tileSize != 256) {
            datacubeParams.get.tileSize
          }else if(rasterExtent.cols<256 && rasterExtent.rows<256) {
            math.max(16, math.max(nextPowerOfTwo(rasterExtent.cols), nextPowerOfTwo(rasterExtent.rows))).toInt
          }
          else if ( experimental && !multiple_polygons_flag && minTiles >= 8) {
            1024
          } else if ( !multiple_polygons_flag && minTiles >= 2) {
            512
          } else {
            256
          }
        }
        FloatingLayoutScheme(tileSize)
      }

    } else {
      layoutScheme
    }
    selectedLayoutScheme
  }

  def readMultibandTileLayer(
    from: ZonedDateTime,
    to: ZonedDateTime,
    boundingBox: ProjectedExtent,
    polygons: Array[MultiPolygon],
    polygons_crs: CRS,
    zoom: Int,
    sc: SparkContext,
    datacubeParams: Option[DataCubeParameters]
  ): MultibandTileLayerRDD[SpaceTimeKey] = {
    val rasterRegionContext = prepareRasterRegions(
      from, to, boundingBox, polygons, polygons_crs, zoom, sc, datacubeParams
    )
    try {
      val cube = RasterTileLoader.loadRasterRegionsToTiles(
        rasterRegionContext.regions,
        rasterRegionContext.metadata,
        rasterRegionContext.maskStrategy,
        rasterRegionContext.partitioner,
        datacubeParams,
        rasterRegionContext.sources,
        openSearchLinkTitlesWithBandId,
        softErrors
      )
      logger.info(
        s"Created cube for $openSearchCollectionId with metadata ${cube.metadata} " +
          s"and partitioner ${cube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index}"
      )
      cube
    } finally {
      rasterRegionContext.unpersist()
    }
  }

  def prepareRasterRegions(
    from: ZonedDateTime,
    to: ZonedDateTime,
    boundingBox: ProjectedExtent,
    polygons: Array[MultiPolygon],
    polygons_crs: CRS,
    zoom: Int,
    sc: SparkContext,
    datacubeParams: Option[DataCubeParameters]
  ): RasterRegionContext = {

    val (keys, metadata, maskStrategy, sources) =
      readKeysToRasterSources(
        from, to, boundingBox, polygons, polygons_crs, zoom, sc, datacubeParams
      )

    val requiredKeys = keys.persist()
    requiredKeys.setName(
      s"FileLayerProvider_keys_${openSearchCollectionId}_${from}_${to}"
    )

    try {
      val partitioner =
        createSpaceTimePartitioner(
          metadata.bounds.get.toSpatial,
          datacubeParams,
          requiredKeys.keys,
          metadata,
          sources
        )

      val regions =
        convertToRasterRegions(
          requiredKeys,
          metadata.layout,
          datacubeParams
        )

      regions.setName(s"FileCollection-$openSearchCollectionId")

      RasterRegionContext(
        requiredKeys = requiredKeys,
        regions = regions,
        metadata = metadata,
        maskStrategy = maskStrategy,
        partitioner = partitioner,
        sources = sources
      )
    } catch {
      case e: Throwable =>
        requiredKeys.unpersist(false)
        throw e
    }
  }

  private def convertToRasterRegions(
    requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])],
    layoutDefinition: LayoutDefinition,
    datacubeParams: Option[DataCubeParameters]
  ): RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = {
    val resample = math.abs(layoutDefinition.cellSize.resolution - maxSpatialResolution.resolution) >= 0.0000001 * layoutDefinition.cellSize.resolution
    // Resampling is still needed in case bounding boxes are not aligned with pixels
    // https://github.com/Open-EO/openeo-geotrellis-extensions/issues/69
    val theResampleMethod = datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor)

    requiredSpacetimeKeys.sparkContext.setCallSite(s"load_collection: determine raster regions to read resample: ${resample}")

    requiredSpacetimeKeys
      .groupBy { case (_, vector.Feature(_, (rasterSource, _))) => rasterSource }
      .flatMap { case (rasterSource, keyedFeatures) =>
        val source = if (resample) {
          //slow path
          rasterSource.tileToLayout(layoutDefinition, theResampleMethod)
        } else {
          //fast path
          new LayoutTileSourceFixed(rasterSource, layoutDefinition, identity)
        }

        keyedFeatures
          .map { case (spaceTimeKey, vector.Feature(_, (rasterSource, _))) =>
            (spaceTimeKey, (source.rasterRegionForKey(spaceTimeKey.spatialKey), rasterSource.name))
          }
          .filter { case (spaceTimeKey, (rasterRegion, sourceName)) =>
            val canRead = rasterRegion.isDefined
            if (!canRead) logger.warn(s"no RasterRegion for $spaceTimeKey in $sourceName")
            canRead
          }
          .map { case (spaceTimeKey, (Some(rasterRegion), sourceName)) => (spaceTimeKey, (rasterRegion, sourceName)) }
      }
  }

  private def createSpaceTimePartitioner(
    spatialBounds: KeyBounds[SpatialKey],
    datacubeParams: Option[DataCubeParameters],
    spaceTimeKeys: RDD[SpaceTimeKey],
    metadata: TileLayerMetadata[SpaceTimeKey],
    sources: Seq[(RasterSource, Feature)]
  ): Option[SpacePartitioner[SpaceTimeKey]] = {
    val maxKeys = (spatialBounds.maxKey.col - spatialBounds.minKey.col + 1) * (spatialBounds.maxKey.row - spatialBounds.minKey.row + 1)
    if (maxKeys > 4) {
      DatacubeSupport.createPartitioner(datacubeParams, spaceTimeKeys, metadata)
    } else {
      //for low number of spatial keys, we can construct sparse partitioner in a cheaper way
      val reduction: Int = datacubeParams.map(_.partitionerIndexReduction).getOrElse(Option.empty).getOrElse(SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION)
      val keys = metadata.keysForGeometry(toPolygon(metadata.extent))
      val dates = sources.map(_._2.nominalDate).distinct
      val allKeys: Set[SpaceTimeKey] = for {x <- keys; y <- dates} yield SpaceTimeKey(x, TemporalKey(y))
      val indices = allKeys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = reduction)).toArray.sorted
      Some(SpacePartitioner(metadata.bounds)(SpaceTimeKey.Boundable, ClassTag(classOf[SpaceTimeKey]), new SparseSpaceTimePartitioner(indices, reduction, theKeys = Some(allKeys.toArray))))
    }
  }

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, zoom: Int = maxZoom, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val targetBBox =
    if(this.layoutScheme.isInstanceOf[FloatingLayoutScheme] && this.maxSpatialResolution.resolution > 2 && this.maxSpatialResolution.resolution < 200) {
      //this check for utm is not good, ideally fileLayerProvider has access to collection metadata that contains information about native projection system
      val center = boundingBox.extent.center.reproject(boundingBox.crs,LatLng)
      val epsg = autoUtmEpsg(center.getX,center.getY)
      val targetCRS = CRS.fromEpsgCode(epsg)
      ProjectedExtent(boundingBox.reproject(targetCRS),targetCRS)
    }else{
      boundingBox
    }
    this.readMultibandTileLayer(from,to,targetBBox,Array(MultiPolygon(targetBBox.extent.toPolygon())),targetBBox.crs,zoom,sc,datacubeParams = Option.empty)
  }


  private def deriveFilePath(href: URI): String = href.getScheme match {
    // as oscars requests now use accessedFrom=MEP, we will normally always get file paths
    case "file" => // e.g. file:/data/MTDA_DEV/CGS_S2_DEV/FAPAR_V2/2020/03/19/S2A_20200319T032531_48SXD_FAPAR_V200/10M/S2A_20200319T032531_48SXD_FAPAR_10M_V200.tif
      href.getPath.replaceFirst("CGS_S2_DEV", "CGS_S2") // temporary workaround?
    case "https" if( _rootPath !=null ) =>
      val hrefString = href.toString
      if (hrefString.contains("artifactory.vgt.vito.be/artifactory/testdata-public")) {
        hrefString
      } else {
        // e.g. https://oscars-dev.vgt.vito.be/download/FAPAR_V2/2020/03/20/S2B_20200320T102639_33VVF_FAPAR_V200/10M/S2B_20200320T102639_33VVF_FAPAR_10M_V200.tif
        val subPath = href.getPath
          .split("/")
          .drop(4) // the empty string at the front too
          .mkString("/")

        (_rootPath resolve subPath).toString
      }
    case _ => href.toString
  }

  private def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
    Extent(
      extent.xmin,
      extent.ymin,
      math.max(extent.xmax, extent.xmin + cellSize.width),
      math.max(extent.ymax, extent.ymin + cellSize.height),
    )


  /**
   *
   * @param feature          The feature
   * @param targetExtent     The target extent to read from 'feature'
   * @param datacubeParams   Data cube parameters
   * @param targetResolution Target resolution to read.
   * @return
   */
  private def deriveRasterSourcesUsingRasterSourceProviders(feature: Feature, targetExtent: ProjectedExtent, datacubeParams: Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty, resolver: BandAssetLinkResolver): Option[(RasterSource, Feature)] = {

    val theResolution = targetResolution.getOrElse(maxSpatialResolution)
    val re = RasterExtent(expandToCellSize(targetExtent.extent,theResolution), theResolution)

    val featureExtentInLayout: Option[GridExtent[Long]] = computeItemExtentInTargetLayout(feature, re, targetExtent, datacubeParams)
    var predefinedExtent: Option[GridExtent[Long]] = None
    val bandNames = openSearchLinkTitles.toList

    val byLinkTitle = !fromLoadStac

    val expectedNumberOfBands = openSearchLinkTitlesWithBandId.size

    val rasterSources: Seq[Option[(RasterSource, Int)]] =
      resolver.getBandAssets(feature).map {
        case Some((link, bandIndex)) =>
          val pixelValueScale: Double = link.pixelValueScale.getOrElse(1)
          val pixelValueOffset: Double = link.pixelValueOffset.getOrElse(0)

          //special case handling for data that does not declare nodata properly
          val targetCellType = link.title match {
            // An un-used band called "IMG_DATA_Band_SCL_60m_Tile1_Unit" exists, so not specifying the resulution in the if-check.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0)))
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(UShortConstantNoDataCellType))
            case Some(title) if fromLoadStac && title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(UShortConstantNoDataCellType)) // TODO: get info from Link object
            case Some(title) if fromLoadStac && Seq("SCL_20m", "SCL_60m").contains(title) => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0))) // TODO: get info from Link object
            case _ => None
          }

          val targetTargetCellType: Option[TargetCellType] = link.title match {
            // Sentinel 2 bands can have negative values now.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => None
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(ShortConstantNoDataCellType))
            case Some(title) if fromLoadStac && title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(ShortConstantNoDataCellType)) // TODO: get info from Link object
            case _ => None
          }
          val definition = RasterSourceDefinition(link, bandIndex, feature, rootPath, targetCellType, targetExtent, featureExtentInLayout, targetResolution, maxSpatialResolution, datacubeParams, experimental)
          val maybeSource: Option[RasterSource] = rasterSourceProviderChain.find(
              _.canProcess(definition)
            ).map(
              p => {
                if (p.usePredefinedExtent(definition)) {
                  predefinedExtent = featureExtentInLayout
                }
                p.rasterSource(definition)
              }
            )
            .map(ValueOffsetRasterSource.wrapRasterSource(_, pixelValueScale, pixelValueOffset, targetTargetCellType))
          if (maybeSource.isDefined) {
            if (bandIndex > 0) {
              Some((IndexedRasterSource(maybeSource.get, bandIndex), 0))
            } else {
              Some(maybeSource.get, 0)
            }
          } else {
            None
          }
        case _ => None
      }

    if (rasterSources.isEmpty) {
      logger.warn(s"Excluding item ${feature.id} with available assets ${feature.links.map(_.title).mkString("(", ", ", ")")}")
      None
    } else {
      lazy val gridExtent = predefinedExtent
        .orElse {
          rasterSources.collectFirst {
            case Some((rasterSource, _)) => rasterSource.gridExtent
          }
        }.getOrElse(return None)

      val sources = NonEmptyList.fromListUnsafe(rasterSources.toList)
        .map {
          case Some(rasterSource) => rasterSource
          case _ => (NoDataRasterSource.instance(gridExtent, targetExtent.crs), 0)
        }

      val attributes = Predef.Map("date" -> feature.nominalDate.toString)

      if (byLinkTitle && bandIndices.isEmpty) {
        val actualNumberOfBands = rasterSources.size

        if (actualNumberOfBands != expectedNumberOfBands) {
          logger.warn(s"Did not find expected number of bands $expectedNumberOfBands (actual: $actualNumberOfBands) for feature ${feature.id} with links ${feature.links.mkString("Array(", ", ", ")")}")
          return None
        }

        Some((new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetExtent.crs, attributes, predefinedExtent = predefinedExtent, softErrors = softErrors), feature))
      } else if (sources.forall { case(_, idx) => idx == 0}) {
        Some((new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource}, targetExtent.crs, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent), feature))
      } else {
        logger.warn("Unexpected use of MultibandCompositeRasterSource")
        Some((new MultibandCompositeRasterSource(sources.map { case (rasterSource, bandIndex) => (rasterSource, Seq(bandIndex))}, targetExtent.crs, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent), feature))
      }
    }
  }


  /**
   *
   * @param feature
   * @param targetExtent The target extent to read from 'feature'
   * @param datacubeParams Data cube parameters
   * @param targetResolution Target resolution to read.
   * @return
   */
  private def deriveRasterSources(feature: Feature, targetExtent:ProjectedExtent, datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Option[(RasterSource, Feature)] = {
    val resolver = BandAssetLinkResolver(openSearch, openSearchLinkTitles, rootPath, maxSpatialResolution, bandIndices, experimental, maxSoftErrorsRatio)
    deriveRasterSourcesUsingRasterSourceProviders(feature, targetExtent, datacubeParams, targetResolution, resolver)
  }

  private def computeItemExtentInTargetLayout(item: Feature, re: RasterExtent, targetExtent: ProjectedExtent, datacubeParams: Option[DataCubeParameters]) = {
    if (item.rasterExtent.isDefined && item.crs.isDefined) {
      val useNewFeatureExtentIntersectionPossible = isCrsCoveredInHealthCheck(item.crs.get) && isCrsCoveredInHealthCheck(targetExtent.crs)
      val alignedToTargetExtent = if (!datacubeParams.exists(_.useNewFeatureExtentIntersection) || !useNewFeatureExtentIntersectionPossible) {
        // logger.info("Using old intersection method between Feature/Item and target extent.")
        // TODO: Remove this after it has been deployed for a while
        /**
         * Several edge cases to cover:
         *  - if feature extent is whole world, it may be invalid in target crs
         *  - if feature is in utm, target extent may be invalid in feature crs
         *    this is why we take intersection
         */
        val targetExtentInLatLon = targetExtent.reproject(item.crs.get)
        val featureExtentInLatLon = item.rasterExtent.get.reproject(item.crs.get, LatLng)

        val intersection = featureExtentInLatLon.intersection(targetExtentInLatLon).map(_.buffer(1.0)).getOrElse(featureExtentInLatLon)
        val tmp = expandToCellSize(intersection.reproject(LatLng, targetExtent.crs), re.cellSize)
        re.createAlignedRasterExtent(tmp)
      } else {
        val featureProjectedExtent = ProjectedExtent(item.rasterExtent.get, item.crs.get)
        healthCheckExtentWarn(featureProjectedExtent, s"Feature/Item extent should be valid: ")
        healthCheckExtentWarn(targetExtent, s"Target extent should be valid: ")

        /**
         * Several edge cases to cover:
         *  - if feature extent is whole world, it may be invalid in target crs (tested in readDataCubeWithOpensearchClientUTM)
         *  - if feature is in utm, target extent may be invalid in feature crs
         *    this is why we take intersection.
         *    We convert both extents to a common CRS before taking the intersection.
         *    We give priority to use the target CRS as common CRS, because the intersection will be converted to it anyway
         *    In case the feature extent is invalid in the target CRS, we use the feature CRS as common CRS
         */
        val commonCrs = if (isExtentValidInCrs(featureProjectedExtent, targetExtent.crs)) targetExtent.crs
        else if (isExtentValidInCrs(targetExtent, item.crs.get)) item.crs.get
        else {
          logger.warn(s"Feature/Item and target extent are not valid within each others range. Using LatLng as fallback.")
          LatLng
        }

        val featureExtentInCommonCRS = safeReproject(featureProjectedExtent, commonCrs)
        val targetExtentInCommonCRS = safeReproject(targetExtent, commonCrs)
        healthCheckExtentWarn(featureExtentInCommonCRS, s"Item extent (${item.id}) should be valid in common CRS: ")

        val intersection = featureExtentInCommonCRS.extent.intersection(targetExtentInCommonCRS.extent)
        val intersectionTargetCrs = intersection match {
          case None =>
            // Item, Asset and Feature mean the same thing in this context.
            logger.warn(s"Item extent $featureExtentInCommonCRS and target extent $targetExtentInCommonCRS do not intersect. (${item.id})")
            // return None // Discard the feature
            // TODO: feature.rasterExtent is not accurate when going over the antimeridian.
            // TODO: Fall back to feature.geometry? Now the fallback is to load the whole tile (Just like old intersection code)
            targetExtent.extent
          case Some(value) => value.reproject(commonCrs, targetExtent.crs)
        }
        var tmp = expandToCellSize(intersectionTargetCrs, re.cellSize)
        val dcp = datacubeParams.getOrElse(new DataCubeParameters())
        val p = math.max(1, dcp.maskingStrategyParameters
          .getOrDefault("erosion_kernel_size", 0.asInstanceOf[Object]).asInstanceOf[Integer]) * 1.0
        val pixelBuffer = (math.max(p, dcp.pixelBufferX), math.max(p, dcp.pixelBufferY))
        tmp = Extent(
          tmp.xmin - re.cols * pixelBuffer._1, tmp.ymin - re.rows * pixelBuffer._2,
          tmp.xmax + re.cols * pixelBuffer._1, tmp.ymax + re.rows * pixelBuffer._2,
        )
        healthCheckExtentWarn(ProjectedExtent(tmp, targetExtent.crs), s"Item extent (${item.id}) should be valid in target CRS: ")
        re.createAlignedRasterExtent(tmp)
      }
      Some(alignedToTargetExtent.toGridType[Long])
    } else {
      Some(re.toGridType[Long])
    }
  }

  def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Seq[(RasterSource,Feature)] = {
    require(zoom >= 0) // TODO: remove zoom and sc parameters

    var overlappingFeatures: Seq[Feature] = openSearch.getProducts(
      collectionId = openSearchCollectionId,
      (from.toLocalDate, to.toLocalDate), boundingBox,
      attributeValues, correlationId, ""
    )

    val filter = datacubeParams.map(_.timeDimensionFilter)
    if (filter.isDefined && filter.get.isDefined) {
      val condition = filter.get.get.asInstanceOf[OpenEOProcessScriptBuilder]
      //TODO how do we pass in user context
      overlappingFeatures=overlappingFeatures.filter(f=>condition.inputFunction.asInstanceOf[AnyProcess].apply(Map("value"->f.nominalDate)).apply(f.nominalDate).asInstanceOf[Boolean])
    }

    if (datacubeParams.getOrElse(new DataCubeParameters()).useNewFeatureExtentIntersection2) {
      overlappingFeatures = overlappingFeatures.map(f => {
        f.geometry match {
          case None => f
          case Some(geom) =>
            var pp = ProjectedPolygons(geom, LatLng)
            val fileIdPattern: Regex = "^.*_(\\d\\d)[^\\d]+_[^_]+_[^_]+$".r
            f.id match {
              case fileIdPattern(zone) => {
                val crs = CRS.fromName("EPSG:326" + zone)

                // The geom in the catalog does not take into account curvature.
                // Doing a basic projection and a refined projection back fixes this.
                pp = pp
                  .safeReproject(crs, refine = false)
                  .safeReproject(LatLng, refine = true)
                  .splitPolygonsOnWrapPoint()

                var ps = pp.getFlatMultiPolygon.polygons
                // This collection has huge chunks of nodata in tiles around the antimeridian, causing artifacts.
                // Remove the polygons that cross the line to mitigate this
                if (zone == "60") ps = ps.filter(p => p.getCoordinate.x > 0)
                if (zone == "01") ps = ps.filter(p => p.getCoordinate.x < 0)
                pp = ProjectedPolygons(MultiPolygon(ps), LatLng)
              }
              case _ => logger.debug(s"${f.id} does not match UTM zone regex")
            }
            f.copy(geometry = Some(pp.getFlatMultiPolygon))
        }
      })
    }

    val reprojectedBoundingBox: ProjectedExtent = targetBoundingBox(boundingBox, layoutScheme)
    val overlappingRasterSources = (for {
      feature <- overlappingFeatures
    } yield  deriveRasterSources(feature,reprojectedBoundingBox, datacubeParams,targetResolution)).flatMap(_.toList)

    val tracker = BatchJobMetadataTracker.tracker("")
    tracker.addInputProducts(
      openSearchCollectionId,
      overlappingRasterSources.map { case (_, feature) => feature.id }.asJava
    )

    tracker.addAuxiliaryFile(
      new DerivedFromDocumentWriter(inputFeatures = overlappingRasterSources.map { case (_, feature) => feature }),
      "application/geo+json",
    )

    // TODO: these geotiffs overlap a bit so for a bbox near the edge, not one but two or even four geotiffs are taken
    //  into account; it's more efficient to filter out the redundant ones

    if (overlappingRasterSources.isEmpty) throw new IllegalArgumentException(s"""Could not find data for your ${if (fromLoadStac) "load_stac" else "load_collection"} request with catalog ID "$openSearchCollectionId". The catalog query had correlation ID "$correlationId" and returned ${overlappingFeatures.size} results.""")

    overlappingRasterSources

  }

  override def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])] =
    metadataCache.get(CacheKey(openSearch, openSearchCollectionId, _rootPath, pathDateExtractor))

  override def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, zoom: Int = maxZoom, sc: SparkContext): TileLayerRDD[SpaceTimeKey] =
    readMultibandTileLayer(from, to, boundingBox, zoom, sc).withContext { singleBandTiles =>
      singleBandTiles.mapValues { multiBandTile => multiBandTile.band(0) }
    }

  override def readMetadata(zoom: Int, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    val Some((projectedExtent, dates)) = loadMetadata(sc)

    layerMetadata(projectedExtent, dates.head, dates.last, zoom min maxZoom, FloatConstantNoDataCellType, layoutScheme,
      maxSpatialResolution)
  }

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) = loadMetadata(sc).get

  override def toString: String =
    s"${getClass.getSimpleName}($openSearchCollectionId, ${openSearchLinkTitlesWithBandId.map(_._1).toList.mkString("[", ", ", "]")}, $rootPath)"
}
