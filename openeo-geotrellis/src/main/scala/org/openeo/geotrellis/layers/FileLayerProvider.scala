package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import com.azavea.gdal.GDALWarp
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import geotrellis.layer.{TemporalKeyExtractor, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, NoNoData, PaddedTile, Raster, RasterExtent, RasterMetadata, RasterRegion, RasterSource, ResampleMethod, ResampleTarget, SourceName, SourcePath, TargetAlignment, TargetCellType, TargetRegion, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.partition.PartitionerIndex.SpatialPartitioner
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrellis.file.AbstractPyramidFactory
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, CloudFilterStrategy, DataCubeParameters, DatacubeSupport, L1CCloudFilterStrategy, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.LoggerFactory

import java.io.{File, IOException}
import java.net.{URI, URL}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time._
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.reflect.ClassTag
import scala.reflect.io.Directory
import scala.util.matching.Regex

// TODO: are these attributes typically propagated as RasterSources are transformed? Maybe we should find another way to
//  attach e.g. a date to a RasterSource.
class BandCompositeRasterSource(override val sources: NonEmptyList[RasterSource], override val crs: CRS, override val attributes: Map[String, String] = Map.empty, predefinedExtent: Option[GridExtent[Long]] = None)
  extends MosaicRasterSource { // TODO: don't inherit?

  protected def reprojectedSources: NonEmptyList[RasterSource] = sources map { _.reproject(crs) }
  protected def reprojectedSources(bands: Seq[Int]): NonEmptyList[RasterSource] = {
    val selectedBands =  (NonEmptyList.fromList(bands.map(sources.toList).toList)).get
    selectedBands map { _.reproject(crs)}
  }

  override def gridExtent: GridExtent[Long] = predefinedExtent.getOrElse{
    try {
      sources.head.gridExtent
    }  catch {
      case e: Exception => throw new IOException(s"Error while reading extent of: ${sources.head.name.toString}", e)
    }

  }
  override def cellType: CellType = sources.map(_.cellType).reduceLeft(_ union _)

  override def name: SourceName = sources.head.name
  override def bandCount: Int = sources.size

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = reprojectedSources(bands)
    val singleBandRasters = selectedSources.toList.par
      .map { _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == selectedSources.size) Some(Raster(MultibandTile(singleBandRasters.map(_.tile).seq), singleBandRasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = reprojectedSources(bands)
    val singleBandRasters = selectedSources.toList.par
      .map  {source =>
        try{
          source.read(bounds, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) }
        }   catch {
          case e: Exception => throw new IOException(s"Error while reading ${bounds} from: ${source.name.toString}", e)
        }

      }
      .collect { case Some(raster) => raster }

    try {
      if(singleBandRasters.isEmpty) {
        None
      }else{
        val intersection = singleBandRasters.map(_.extent).reduce((left,right) => left.intersection(right).get)
        val croppedRasters = singleBandRasters.map(_.crop(intersection))
        if (singleBandRasters.size == selectedSources.size) Some(Raster(MultibandTile(croppedRasters.map(_.tile.convert(cellType)).seq), intersection))
        else None
      }
    }catch {
      case e: Exception => throw new IOException(s"Error while reading ${bounds} from: ${selectedSources.head.name.toString}", e)
    }
  }

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = new BandCompositeRasterSource(
    reprojectedSources map { _.resample(resampleTarget, method, strategy) }, crs)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new BandCompositeRasterSource(reprojectedSources map { _.convert(targetCellType) }, crs)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new BandCompositeRasterSource(
      reprojectedSources map { _.reproject(targetCRS, resampleTarget, method, strategy) },
      crs
    )
}

// TODO: is this class necessary? Looks like a more general case of BandCompositeRasterSource so maybe the inheritance
//  relationship should be reversed; or maybe the BandCompositeRasterSource could be made more general and accept
//  multi-band RasterSources too.
class MultibandCompositeRasterSource(val sourcesListWithBandIds: NonEmptyList[(RasterSource, Seq[Int])], override val crs: CRS, override val attributes: Map[String, String] = Map.empty)
  extends BandCompositeRasterSource(sourcesListWithBandIds.map(_._1), crs, attributes) {

  override def bandCount: Int = sourcesListWithBandIds.map(_._2.size).toList.sum

  private def sourcesWithBandIds = NonEmptyList.fromListUnsafe(reprojectedSources.toList.zip(sourcesListWithBandIds.map(_._2).toList))

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters = sourcesWithBandIds
      .map { s => s._1.read(extent, s._2) }
      .collect { case Some(raster) => raster }

    if (rasters.size == sources.size) Some(Raster(MultibandTile(rasters.flatMap(_.tile.bands)), rasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val rasters = sourcesWithBandIds
      .map { s => s._1.read(bounds, s._2) }
      .collect { case Some(raster) => raster }

    if (rasters.size == sources.size) Some(Raster(MultibandTile(rasters.flatMap(_.tile.convert(cellType).bands)), rasters.head.extent))
    else None
  }

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = new MultibandCompositeRasterSource(
    sourcesWithBandIds map { case (source, bands) => (source.resample(resampleTarget, method, strategy), bands) }, crs)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new MultibandCompositeRasterSource(sourcesWithBandIds map { case (source, bands) => (source.convert(targetCellType), bands) }, crs)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new MultibandCompositeRasterSource(
      sourcesWithBandIds map { case (source, bands) => (source.reproject(targetCRS, resampleTarget, method, strategy), bands) },
      crs
    )
}

object FileLayerProvider {

  private val logger = LoggerFactory.getLogger(classOf[FileLayerProvider])

  {
    try {
      GDALWarp.init(32)
    } catch {
      case e: java.lang.UnsatisfiedLinkError =>
        // Error message probably looks like this:
        // "java.lang.UnsatisfiedLinkError: C:\Users\...\gdalwarp_bindings.dll: Can't find dependent libraries"
        // Ignore GDAL init error so that tests that don't require it will be ok.
        // Tests that require it will still crash when it is not installed.
        logger.warn("GDAL library not found: " + e.getMessage)
    }
  }

  // important: make sure to implement object equality for CacheKey's members
  private case class CacheKey(openSearch: OpenSearchClient, openSearchCollectionId: String, rootPath: Path,
                              pathDateExtractor: PathDateExtractor)

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
      def getMetadata(rs: RasterMetadata): ZonedDateTime = ZonedDateTime.parse(rs.attributes("date")).truncatedTo(ChronoUnit.DAYS)
    }
    val sources = sc.parallelize(rasterSources,rasterSources.size)

    val noResampling = metadata.crs.proj4jCrs.getProjection.getName == "utm" && math.abs(metadata.layout.cellSize.resolution - maxSpatialResolution.resolution) < 0.0000001 * metadata.layout.cellSize.resolution
    sc.setJobDescription("Load tiles: " + collection + ", rs: " + noResampling)
    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => SpaceTimeKey = { sk => keyExtractor.getKey(m, sk) }
        //The first form 'rs.tileToLayout' will check if rastersources are aligned, requiring reading of metadata, which has a serious performance impact!
        try{
          if(noResampling)
            LayoutTileSource(rs,metadata.layout,tileKeyTransform)
          else
            rs.tileToLayout(metadata.layout, tileKeyTransform)
        }  catch {
          case e: IllegalArgumentException => {
            logger.error(s"Error tiling rastersource ${rs.name} to layout: ${metadata.layout}, ${rs.gridExtent}, ${rs.cellSize}")
            throw e
          }
        }
      }

    tiledLayoutSourceRDD
  }

  def readMultibandTileLayer(rasterSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey], polygons: Array[MultiPolygon], polygons_crs: CRS, sc: SparkContext, retainNoDataTiles: Boolean, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, useSparsePartitioner: Boolean = true, datacubeParams : Option[DataCubeParameters] = None): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val polygonsRDD = sc.parallelize(polygons).map {
      _.reproject(polygons_crs, metadata.crs)
    }
    // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
    var requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] = polygonsRDD.clipToGrid(metadata.layout).groupByKey()

    tileSourcesToDataCube(rasterSources, metadata, requiredSpatialKeys, sc, retainNoDataTiles, cloudFilterStrategy, useSparsePartitioner, datacubeParams)
  }

  private def checkLatLon(extent:Extent):Boolean = {
    if(extent.xmin < -181 || extent.xmax > 181 || extent.ymin < -92 || extent.ymax > 92) {
      false
    }else{
      true
    }
  }

  def applySpatialMask[M](datacubeParams : Option[DataCubeParameters] , requiredSpatialKeys: RDD[(SpatialKey, M)],metadata:TileLayerMetadata[SpaceTimeKey])(implicit vt: ClassTag[M]): RDD[(SpatialKey, M)] = {
    if (datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject = datacubeParams.get.maskingCube.get
      maskObject match {
        case theSpatialMask: MultibandTileLayerRDD[SpatialKey] =>
          if (theSpatialMask.metadata.bounds.get._1.isInstanceOf[SpatialKey]) {
            val filtered = theSpatialMask.withContext{_.filter(_._2.band(0).toArray().exists(pixel => pixel == 0)).distinct()}
            val maskSpatialKeys =
              if(theSpatialMask.metadata.crs.equals(metadata.crs) && theSpatialMask.metadata.layout.equals(metadata.layout)) {
                filtered
              }else{
                logger.debug(s"mask: automatically resampling mask to match datacube: ${theSpatialMask.metadata}")
                filtered.reproject(metadata.crs,metadata.layout,16,requiredSpatialKeys.partitioner)._2
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

    // The requested sources already contain the requested dates for every tile (if they exist).
    // We can join these dates with the requested spatial keys.

    val partitioner = useSparsePartitioner match {
      case true => {
        if(inputFeatures.isDefined) {
          //using metadata inside features is a much faster way of determining Spacetime keys
          val keysForfeatures: _root_.org.apache.spark.rdd.RDD[(_root_.geotrellis.layer.SpatialKey, _root_.geotrellis.vector.Feature[_root_.geotrellis.vector.Geometry, _root_.org.openeo.opensearch.OpenSearchResponses.Feature])] = productsToSpatialKeys(inputFeatures, metadata, sc)
          val griddedFeatures = keysForfeatures.join(localSpatialKeys)

          val requiredSpacetimeKeys: RDD[(SpaceTimeKey)] = griddedFeatures.map(t=>SpaceTimeKey(t._1,TemporalKey(t._2._1.data.nominalDate)))

          DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys, metadata)
        }else{
          createPartitioner(datacubeParams, localSpatialKeys, filteredSources, metadata)
        }
      }
      case false => Option.empty
    }

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

    requestedRasterRegions = DatacubeSupport.applyDataMask(datacubeParams,requestedRasterRegions,metadata)

    requestedRasterRegions.name = rasterSources.name
    rasterRegionsToTiles(requestedRasterRegions, metadata, retainNoDataTiles, cloudFilterStrategy, partitioner)
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

  def createPartitioner(datacubeParams: Option[DataCubeParameters], requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])], filteredSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey]): Some[SpacePartitioner[SpaceTimeKey]] = {
    val requiredSpacetimeKeys: RDD[SpaceTimeKey] = filteredSources.flatMap(_.keys).map {
      tuple => (tuple.spatialKey, tuple)
    }.rightOuterJoin(requiredSpatialKeys).flatMap(_._2._1.toList)
    DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys, metadata)
  }


  private val PIXEL_COUNTER = "InputPixels"

  private def rasterRegionsToTiles(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))], metadata: TileLayerMetadata[SpaceTimeKey], retainNoDataTiles: Boolean, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = None) = {
    val partitioner = partitionerOption.getOrElse(SpacePartitioner(metadata.bounds))
    logger.info(s"Cube partitioner index: ${partitioner.index}")
    val totalChunksAcc: LongAccumulator = rasterRegionRDD.sparkContext.longAccumulator("ChunkCount_" + rasterRegionRDD.name)
    val tracker = BatchJobMetadataTracker.tracker("")
    tracker.registerCounter(PIXEL_COUNTER)
    val loadingTimeAcc = rasterRegionRDD.sparkContext.doubleAccumulator("SecondsPerChunk_" + rasterRegionRDD.name)
    val crs = metadata.crs
    val layout = metadata.layout
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner)
        .mapPartitions(partitionIterator => {
          var totalPixelsPartition = 0
          val startTime = System.currentTimeMillis()

          val (loadedPartitions,partitionPixels) = loadPartition(partitionIterator, cloudFilterStrategy, totalChunksAcc, tracker,crs,layout )
          totalPixelsPartition += partitionPixels

          val durationMillis = System.currentTimeMillis() - startTime
          if (totalPixelsPartition > 0) {
            val secondsPerChunk = (durationMillis / 1000.0) / (totalPixelsPartition / (256 * 256))
            loadingTimeAcc.add(secondsPerChunk)
          }
          loadedPartitions

        }
          .filter { case (_, tile) => retainNoDataTiles || tile.isDefined && !tile.get.bands.forall(_.isNoDataTile) }
          .map(t => (t._1,t._2.get)).iterator,
          preservesPartitioning = true)


    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD
  }

  private def loadPartition(partitionIterator: Iterator[(SpaceTimeKey, Iterable[(RasterRegion, SourceName)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs :CRS, layout:LayoutDefinition ) = {
    var totalPixelsPartition = 0
    val loadedPartitions = partitionIterator.toParArray.map(tuple => {
      val allRegions = tuple._2.toSeq

      val tilesForRegion = allRegions
        .flatMap { case (rasterRegion, sourcePath: SourcePath) =>
          val result: Option[(MultibandTile, SourcePath)] = cloudFilterStrategy match {
            case l1cFilterStrategy: L1CCloudFilterStrategy =>
              if (GDALCloudRasterSource.isRegionFullyClouded(rasterRegion, crs, layout, l1cFilterStrategy.bufferInMeters)) {
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
                      val cloudRasterSource = compositeRasterSource.sources.head.asInstanceOf[GDALCloudRasterSource]
                      val cloudPolygons: Seq[Polygon] = cloudRasterSource.getMergedPolygons(l1cFilterStrategy.bufferInMeters)
                      val cloudPolygon = MultiPolygon(cloudPolygons).reproject(cloudRasterSource.crs, crs)
                      val cloudTile = Rasterizer.rasterizeWithValue(cloudPolygon, RasterExtent(rasterRegion.extent, tile.get.cols, tile.get.rows), 1)
                      val cloudMultibandTile = MultibandTile(List.fill(tile.get.bandCount)(cloudTile))
                      val maskedTile = tile.get.localMask(cloudMultibandTile, 1, 0).convert(tile.get.cellType)
                      Some(maskedTile)
                    } else Option.empty
                  }
                }).map((_, sourcePath))
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
                  val maybeTile = rasterRegion.raster.map(_.tile)
                  if (maybeTile.isDefined && maybeTile.get.cellType.isInstanceOf[NoNoData]) {
                    maybeTile.map(t => t.convert(t.cellType.withDefaultNoData()))
                  } else {
                    maybeTile
                  }

                }
              }).map((_, sourcePath))
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
          else leftSourcePath.value < rightSourcePath.value
        }
        .map { case (multibandTile, _) => multibandTile }
        .reduceOption(_ merge _)
      (tuple._1, tilesForRegion)
    })
    (loadedPartitions,totalPixelsPartition)
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

class FileLayerProvider(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
                        maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
                        bandIds: Seq[Seq[Int]] = Seq(), correlationId: String = "", experimental: Boolean = false,
                        retainNoDataTiles: Boolean = false) extends LayerProvider {

  import DatacubeSupport._
  import FileLayerProvider._

  if(experimental) {
    logger.warn("Experimental features enabled for: " + openSearchCollectionId)
  }

  private val _rootPath = if(rootPath != null) Paths.get(rootPath) else null

  val openSearchLinkTitlesWithBandIds: Seq[(String, Seq[Int])] = {
    if(bandIds.size>0) {
      //case 1: PROBA-V, files containing multiple bands, bandids parameter is used to indicate which bands to load
      openSearchLinkTitles.toList.zipAll(bandIds, "", Seq(0))
    }else{
      //case 2: Sentinel-2 angle metadata: band number is encoded in the oscars link title directly, maybe proba could use this system as well...
      val splitted = openSearchLinkTitles.map(title => {
        val split = title.split("##")
        if (split.length == 1) {
          (split(0), 0)
        }else{
          (split(0),split(1).toInt)
        }
      })//.toList.groupBy(_._1).mapValues(_.map(t=>t._2).toSeq).toSeq
      var previous = ""
      splitted.foldLeft(List[Tuple2[String,Seq[Int]]]()){
        case (head :: res, (linkTitle, bands)) if (linkTitle == previous) => {
          (head._1,(bands +: head._2)) :: res
        }
        case (theList, notMatchingElement) => {
          previous = notMatchingElement._1
          (notMatchingElement._1,Seq(notMatchingElement._2)) :: theList
        }
      }.reverse

    }
  }

  val maxZoom: Int = layoutScheme match {
    case z: ZoomedLayoutScheme => z.zoom(0, 0, maxSpatialResolution)
    case _ => 14
  }

  private val compositeRasterSource: (Feature, NonEmptyList[(RasterSource, Seq[Int])], CRS, Map[String, String]) => (BandCompositeRasterSource,Feature) = {
    (feature, sources, crs, attributes) =>
      {
        val gridExtent = if(feature.tileID.isDefined && feature.crs.isDefined && feature.crs.get == crs && experimental) {
          val tiles = TileGrid.computeFeaturesForTileGrid("100km", ProjectedExtent(ProjectedExtent(feature.bbox, LatLng).reproject(feature.crs.get),feature.crs.get)).filter(_._1.contains(feature.tileID.get))
          tiles.headOption.map(t=>GridExtent[Long](t._2.expandToInclude(t._2.xmax+9800,t._2.ymin-9800),CellSize(10,10)) )
        }else{
          None
        }
        if (bandIds.isEmpty) (new BandCompositeRasterSource(sources.map(_._1), crs, attributes,predefinedExtent = gridExtent),feature)
        else (new MultibandCompositeRasterSource(sources, crs, attributes),feature)
      }
  }

  def determineCelltype(overlappingRasterSources: Seq[(RasterSource, Feature)]): CellType = {
    try {
      var commonCellType = overlappingRasterSources.head._1.cellType
      if (commonCellType.isInstanceOf[NoNoData]) {
        commonCellType = commonCellType.withDefaultNoData()
      }
      commonCellType
    } catch {
      case e: Exception => throw new IOException(s"Exception while determining data type of collection ${this.openSearchCollectionId} and item ${overlappingRasterSources.head._1.name}. Detailed message: ${e.getMessage}",e)
    }
  }

  def readKeysToRasterSources(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon],polygons_crs: CRS, zoom: Int, sc: SparkContext, datacubeParams : Option[DataCubeParameters]): (RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])], TileLayerMetadata[SpaceTimeKey], Option[CloudFilterStrategy], Seq[(RasterSource, Feature)]) = {
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


    logger.info(s"Loading ${openSearchCollectionId} with params ${datacubeParams.getOrElse(new DataCubeParameters)} and bands ${openSearchLinkTitles.toList.mkString(";")} initial layout: ${worldLayout}")

    var overlappingRasterSources: Seq[(RasterSource, Feature)] = loadRasterSourceRDD(fullBBox, from, to, zoom, datacubeParams, Some(worldLayout.cellSize))

    val dates = overlappingRasterSources.map(_._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC")))

    var commonCellType: CellType = determineCelltype(overlappingRasterSources)

    var metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, dates.minBy(_.toEpochSecond), dates.maxBy(_.toEpochSecond), commonCellType)
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
        overlappingRasterSources = GDALCloudRasterSource.filterRasterSources(overlappingRasterSources, maskParams)
        maskStrategy = Some(new L1CCloudFilterStrategy(GDALCloudRasterSource.getDilationDistance(maskParams.asScala.toMap)))
      }
    }

    val polygonsRDD = sc.parallelize(bufferedPolygons).map {
      _.reproject(polygons_crs, targetCRS)
    }

    val workingPartitioner = SpacePartitioner(metadata.bounds.get.toSpatial)
    // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
    var requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] = polygonsRDD.clipToGrid(metadata.layout).groupByKey(workingPartitioner)

    var spatialKeyCount: Long =
      if(polygons.length == 1) {
        //special case for single bbox request
        val spatialBounds = metadata.bounds.get.toSpatial
        (spatialBounds.maxKey.col - spatialBounds.minKey.col + 1) * (spatialBounds.maxKey.row - spatialBounds.minKey.row + 1)
      } else{
        requiredSpatialKeys.map(_._1).countApproxDistinct()
      }
    logger.info(s"Datacube requires approximately ${spatialKeyCount} spatial keys.")


    val retiledMetadata: Option[TileLayerMetadata[SpaceTimeKey]] = DatacubeSupport.optimizeChunkSize(metadata, bufferedPolygons, datacubeParams, spatialKeyCount)
    metadata = retiledMetadata.getOrElse(metadata)

    if (retiledMetadata.isDefined) {
      requiredSpatialKeys = polygonsRDD.clipToGrid(retiledMetadata.get).groupByKey(workingPartitioner)
    }

    overlappingRasterSources.map(_._2).foreach(f => {
      val extent = f.geometry.getOrElse(f.bbox.toPolygon()).extent
      if (!checkLatLon(extent)) throw new IllegalArgumentException(s"Geometry or Bounding box provided by the catalog has to be in EPSG:4326, but got ${extent} for catalog entry ${f}")
    })

    val cubeExtent = metadata.extent
    //extra check on interior, disabled because it requires an (expensive) lookup of the extent
    /*
    overlappingRasterSources = overlappingRasterSources.filter({ t =>
      t._1.extent.interiorIntersects(cubeExtent)
    })*/

    //avoid computing keys that are anyway out of bounds, with some buffering to avoid throwing away too much

    val geometricFeatures = overlappingRasterSources.map(f => geotrellis.vector.Feature(f._2.geometry.getOrElse(f._2.bbox.toPolygon()), f))
    val emptyPoint = Point(0.0,0.0)
    val keysForfeatures = sc.parallelize(geometricFeatures, math.max(1, geometricFeatures.size))
      .map(eoProductFeature => {

        val productCRSOrDefault = eoProductFeature.data._2.crs.getOrElse(targetCRS)
        eoProductFeature.mapGeom(productGeometry => {
          try{
            val intersection = productGeometry.reproject(LatLng, productCRSOrDefault).intersection(cubeExtent.reprojectAsPolygon(targetCRS, productCRSOrDefault, 0.01))
            if(intersection.isValid && intersection.getArea > 0.0) {
              intersection.reproject(productCRSOrDefault,targetCRS)
            }else{
              emptyPoint
            }
          }catch {
            case e: Exception => logger.warn("Exception while determining intersection.",e); emptyPoint
          }

        } )
      }).filter(!_.geom.equals(emptyPoint) )
      .clipToGrid(metadata).partitionBy(workingPartitioner)


    //rdd
    val griddedRasterSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] = keysForfeatures.join(requiredSpatialKeys,workingPartitioner).map(t => (t._1, t._2._1))
    val filteredSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] = applySpatialMask(datacubeParams, griddedRasterSources,metadata)


    var requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = filteredSources.map(t => (SpaceTimeKey(t._1, TemporalKey(t._2.data._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC")))), t._2))
    requiredSpacetimeKeys = DatacubeSupport.applyDataMask(datacubeParams, requiredSpacetimeKeys,metadata)

    if (isUTM) {
      //only for utm is just a safeguard to limit to sentine-1/2 for now
      //try to resolve overlap before actually reqding the data
      requiredSpacetimeKeys = requiredSpacetimeKeys.groupByKey().flatMap(t => {

        def return_original = t._2.map(source => (t._1,source))
        if( t._2.size <= 1) {
          return_original
        }else{
          val key = t._1
          val extent = metadata.keyToExtent(key.spatialKey)
          val distances = t._2.map(source => {
            val sourceExtent = source.data._2.geometry.getOrElse(source.data._2.bbox.toPolygon()).reproject(LatLng, targetCRS).extent
            //try to detect tiles that are on the edge of the footprint
            /**-
             * Effect of buffer multiplication factor:
             *  - larger buffer -> shrink source footprint more -> tiles close to edge get discarded faster, this matters for scl_dilation
             */
            val sourcePolygonBuffered = source.data._2.geometry.getOrElse(source.data._2.bbox.toPolygon()).reproject(LatLng, targetCRS).buffer(-1.5*math.max(extent.width,extent.height))
            val distanceToFootprint = extent.distance(sourcePolygonBuffered)

            val distanceBetweenCenters = extent.center.distance(sourceExtent.center)
            ((distanceBetweenCenters,distanceToFootprint), source)
          })
          val smallestCenterDistance = distances.map(_._1._1).min
          val smallestDistanceToFootprint = distances.map(_._1._2).min
          if(smallestDistanceToFootprint > 0) {
            return_original
          }else{

            /**
             * In case of overlap, we want to select the extent that is either fully inside the footprint
             * Or, in case multiple sources satisfy the distance constraint, we prefer the one that has a CRS matching the target CRS
             *
             */

            val filteredByDistance = distances.filter(_._1._2 == 0)
            val filteredByCRS = filteredByDistance.filter(d => d._2.data._2.crs.isDefined && d._2.data._2.crs.get == targetCRS)
            if (filteredByCRS.nonEmpty) {
              filteredByCRS.map(distance_source => (key, distance_source._2))
            } else {
              filteredByDistance.filter(_._1._1 == smallestCenterDistance).map(distance_source => (key, distance_source._2))
            }

          }
        }

      })
    }
    (requiredSpacetimeKeys,metadata,  maskStrategy,overlappingRasterSources)
  }

  def selectLayoutScheme(extent: ProjectedExtent, multiple_polygons_flag: Boolean, datacubeParams: Option[DataCubeParameters]) = {
    val selectedLayoutScheme = if (layoutScheme.isInstanceOf[FloatingLayoutScheme]) {
      if( (extent.extent.width <= maxSpatialResolution.width) || (extent.extent.height <= maxSpatialResolution.height ) ){
        FloatingLayoutScheme(32)
      }else{val rasterExtent = RasterExtent(extent.extent, maxSpatialResolution)
        val minTiles = math.min(math.floor(rasterExtent.rows / 256), math.floor(rasterExtent.cols / 256)).toInt
        val tileSize = {
          if (datacubeParams.isDefined && datacubeParams.get.tileSize != 256) {
            datacubeParams.get.tileSize
          } else if ( experimental && !multiple_polygons_flag && minTiles >= 8) {
            1024
          } else if ( !multiple_polygons_flag && minTiles >= 4) {
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

  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon], polygons_crs: CRS, zoom: Int, sc: SparkContext, datacubeParams : Option[DataCubeParameters]): MultibandTileLayerRDD[SpaceTimeKey] = {

    val readKeysToRasterSourcesResult = readKeysToRasterSources(from,to, boundingBox, polygons, polygons_crs, zoom, sc, datacubeParams)

    var maskStrategy: Option[CloudFilterStrategy] = readKeysToRasterSourcesResult._3
    val metadata = readKeysToRasterSourcesResult._2
    val requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = readKeysToRasterSourcesResult._1.persist()
    val isUTM = metadata.crs.proj4jCrs.getProjection.getName == "utm"

    try{
      val partitioner = DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys.keys, metadata)

      val noResampling = isUTM && math.abs(metadata.layout.cellSize.resolution - maxSpatialResolution.resolution) < 0.0000001 * metadata.layout.cellSize.resolution
      //resampling is still needed in case bounding boxes are not aligned with pixels
      // https://github.com/Open-EO/openeo-geotrellis-extensions/issues/69
      var regions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = requiredSpacetimeKeys.groupBy(_._2.data._1, readKeysToRasterSourcesResult._4.size).flatMap(t=>{
        val source = if (noResampling) {
          LayoutTileSource(t._1, metadata.layout, identity)
        } else{
          t._1.tileToLayout(metadata.layout, datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor))
        }

        t._2.map(key_feature=>{
          (key_feature._1,(source.rasterRegionForKey(key_feature._1.spatialKey),key_feature._2.data._1.name))
        }).filter(_._2._1.isDefined).map(t=>(t._1,(t._2._1.get,t._2._2)))

      })

      regions.name = s"FileCollection-${openSearchCollectionId}"

      //convert to raster region
      val cube= rasterRegionsToTiles(regions, metadata, retainNoDataTiles, maskStrategy.getOrElse(NoCloudFilterStrategy), partitioner)
      logger.info(s"Created cube for ${openSearchCollectionId} with metadata ${cube.metadata} and partitioner ${cube.partitioner}")
      cube
    }finally{
      requiredSpacetimeKeys.unpersist(false)
    }


  }

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, zoom: Int = maxZoom, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    this.readMultibandTileLayer(from,to,boundingBox,Array(MultiPolygon(boundingBox.extent.toPolygon())),boundingBox.crs,zoom,sc,datacubeParams = Option.empty)
  }


  private def deriveFilePath(href: URI): String = href.getScheme match {
    // as oscars requests now use accessedFrom=MEP, we will normally always get file paths
    case "file" => // e.g. file:/data/MTDA_DEV/CGS_S2_DEV/FAPAR_V2/2020/03/19/S2A_20200319T032531_48SXD_FAPAR_V200/10M/S2A_20200319T032531_48SXD_FAPAR_10M_V200.tif
      href.getPath.replaceFirst("CGS_S2_DEV", "CGS_S2") // temporary workaround?
    case "https" if( _rootPath !=null ) => // e.g. https://oscars-dev.vgt.vito.be/download/FAPAR_V2/2020/03/20/S2B_20200320T102639_33VVF_FAPAR_V200/10M/S2B_20200320T102639_33VVF_FAPAR_10M_V200.tif
      val subPath = href.getPath
        .split("/")
        .drop(4) // the empty string at the front too
        .mkString("/")

      (_rootPath resolve subPath).toString
    case _ => href.toString
  }

  private def deriveRasterSources(feature: Feature, targetExtent:ProjectedExtent, datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Option[(BandCompositeRasterSource, Feature)] = {
    def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
      extent.expandBy(deltaX = math.max((cellSize.width - extent.width) / 2,0.0), deltaY = math.max((cellSize.height - extent.height) / 2,0.0))

    val theResolution = targetResolution.getOrElse(maxSpatialResolution)
    val re = RasterExtent(expandToCellSize(targetExtent.extent,theResolution), theResolution).alignTargetPixels

    val featureExtentInLayout: Option[GridExtent[Long]]=
    if (feature.rasterExtent.isDefined && feature.crs.isDefined) {
      val extentAligner = Extent(
        targetExtent.extent.xmin,
        targetExtent.extent.ymin,
        math.max(targetExtent.extent.xmax, targetExtent.extent.xmin + theResolution.width),
        math.max(targetExtent.extent.ymax, targetExtent.extent.ymin + theResolution.height),
      )
      val tmp = expandToCellSize(feature.rasterExtent.get.reproject(feature.crs.get, targetExtent.crs), theResolution)
      val alignedToTargetExtent = RasterExtent(extentAligner, theResolution).createAlignedRasterExtent(tmp)
      Some(alignedToTargetExtent.toGridType[Long])
    }else{
      None
    }

    var predefinedExtent: Option[GridExtent[Long]] = None
    /**
     * Benefit of targetregion: it can be valid in the target projection system
     * Downside of targetregion: it is a virtual cropping of the raster, so we're not able to load data beyond targetExtent
     *
     */
    val alignment =
      if(feature.crs.isDefined && feature.crs.get.proj4jCrs.getProjection.getName == "utm" && datacubeParams.map(_.maskingStrategyParameters.getOrDefault("method", "")).contains("mask_scl_dilation")) {
        //this hack avoid virtual cropping for Sentinel-2 (utm), which breaks mask_scl_dilation
        TargetAlignment(re)
      }else{
        TargetRegion(re)
      }


    val resampleMethod = datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor)

    def vsisToHttpsCreo(path: String): String = {
      path.replace("/vsicurl/", "").replace("/vsis3/eodata", "https://finder.creodias.eu/files")
    }

    def rasterSource(dataPath:String, cloudPath:Option[(String,String)], targetCellType:Option[TargetCellType], targetExtent:ProjectedExtent, bands : Seq[Int]): Seq[RasterSource] = {
      if(dataPath.endsWith(".jp2") || dataPath.contains("NETCDF:")) {
        val alignPixels = !dataPath.contains("NETCDF:") //align target pixels does not yet work with CGLS global netcdfs
        val warpOptions = GDALWarpOptions(alignTargetPixels = alignPixels, cellSize = Some(theResolution), targetCRS = Some(targetExtent.crs), resampleMethod = Some(resampleMethod),
          te = featureExtentInLayout.map(_.extent), teCRS = Some(targetExtent.crs)
        )
        if (cloudPath.isDefined) {
          Seq(GDALCloudRasterSource(cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = targetCellType))
        } else {
          predefinedExtent = featureExtentInLayout
          Seq(GDALRasterSource(dataPath.replace("/vsis3/eodata/", "/vsis3/EODATA/").replace("https", "/vsicurl/https"), options = warpOptions, targetCellType = targetCellType))
        }
      }else if(dataPath.endsWith("MTD_TL.xml")) {
        //TODO EP-3611 parse angles
        SentinelXMLMetadataRasterSource(new URL(vsisToHttpsCreo(dataPath)),bands)
      }
      else {
        if( feature.crs.isDefined && feature.crs.get != null && feature.crs.get.equals(targetExtent.crs)) {
          // when we don't know the feature (input) CRS, it seems that we assume it is the same as target extent???
          if(experimental) {
            Seq(GDALRasterSource(dataPath, options = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(theResolution), resampleMethod=Some(resampleMethod)), targetCellType = targetCellType))
          }else{
            Seq(GeoTiffResampleRasterSource(GeoTiffPath(dataPath.replace("/vsis3/eodata/","S3://EODATA/")), alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType, None))
          }
        }else{
          if(experimental) {
            val warpOptions = GDALWarpOptions(alignTargetPixels = false, cellSize = Some(theResolution), targetCRS=Some(targetExtent.crs), resampleMethod = Some(resampleMethod),te = Some(targetExtent.extent))
            Seq(GDALRasterSource(dataPath.replace("/vsis3/eodata/","/vsis3/EODATA/").replace("https", "/vsicurl/https"), options = warpOptions, targetCellType = targetCellType))
          }else{

            Seq(GeoTiffReprojectRasterSource(GeoTiffPath(dataPath.replace("/vsis3/eodata/","S3://EODATA/")), targetExtent.crs, alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType = targetCellType))
          }
        }
      }
    }

    val rasterSources: immutable.Seq[(Seq[RasterSource], Seq[Int])] = for {
      (title, bands) <- openSearchLinkTitlesWithBandIds.toList
      link <- feature.links.find(_.title.exists(_.toUpperCase contains title.toUpperCase))
      path = deriveFilePath(link.href)
      cloudPathOptions = (
        feature.links.find(_.title contains "FineCloudMask_Tile1_Data").map(_.href.toString),
        feature.links.find(_.title contains "S2_Level-1C_Tile1_Metadata").map(_.href.toString)
      )
      cloudPath = for(x <- cloudPathOptions._1; y <- cloudPathOptions._2) yield (x,y)

      //special case handling for data that does not declare nodata properly
      targetCellType = link.title match {
        case x if x.get.contains("SCENECLASSIFICATION_20M") =>  Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0)))
        case x if x.get.startsWith("IMG_DATA_Band_") =>  Some(ConvertTargetCellType(UShortConstantNoDataCellType))
        case _ => None
      }
    } yield (rasterSource(path, cloudPath, targetCellType, targetExtent, bands), bands)

    if(rasterSources.isEmpty) {
      return None
    }else{

      val sources = NonEmptyList.fromListUnsafe(rasterSources.flatMap(rs_b => rs_b._1.map(rs => (rs, rs_b._2))).toList)

      val attributes = Predef.Map("date" -> feature.nominalDate.toString)

      if (bandIds.isEmpty) return Some((new BandCompositeRasterSource(sources.map(_._1), targetExtent.crs, attributes, predefinedExtent = predefinedExtent), feature))
      else return Some((new MultibandCompositeRasterSource(sources, targetExtent.crs, attributes), feature))
    }

  }

  def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int,datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Seq[(RasterSource,Feature)] = {
    require(zoom >= 0) // TODO: remove zoom and sc parameters

    val overlappingFeatures: Seq[Feature] = openSearch.getProducts(
      collectionId = openSearchCollectionId,
      (from.toLocalDate, to.toLocalDate), boundingBox,
      attributeValues, correlationId, ""
    )

    BatchJobMetadataTracker.tracker("").addInputProducts(openSearchCollectionId,overlappingFeatures.map(_.id).asJava)

    val reprojectedBoundingBox: ProjectedExtent = targetBoundingBox(boundingBox, layoutScheme)
    val overlappingRasterSources = (for {
      feature <- overlappingFeatures
    } yield  deriveRasterSources(feature,reprojectedBoundingBox, datacubeParams,targetResolution)).flatMap(_.toList)

    // TODO: these geotiffs overlap a bit so for a bbox near the edge, not one but two or even four geotiffs are taken
    //  into account; it's more efficient to filter out the redundant ones

    if (overlappingRasterSources.isEmpty) throw new IllegalArgumentException(s"""Could not find data for your load_collection request with catalog ID "$openSearchCollectionId". The catalog query had correlation ID "$correlationId" and returned ${overlappingFeatures.size} results.""")

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

    layerMetadata(projectedExtent,dates.head,dates.last,zoom, FloatConstantNoDataCellType, layoutScheme, maxSpatialResolution)
  }

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) = loadMetadata(sc).get

  override def toString: String =
    s"${getClass.getSimpleName}($openSearchCollectionId, ${openSearchLinkTitlesWithBandIds.map(_._1).toList.mkString("[", ", ", "]")}, $rootPath)"
}
