package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import com.azavea.gdal.GDALWarp
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import geotrellis.layer.{TemporalKeyExtractor, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, CroppedTile, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, NoNoData, PaddedTile, Raster, RasterExtent, RasterMetadata, RasterRegion, RasterSource, ResampleMethod, ResampleTarget, ShortConstantNoDataCellType, SourceName, TargetAlignment, TargetCellType, TargetRegion, Tile, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.clip.ClipToGrid
import geotrellis.spark.clip.ClipToGrid.clipFeatureToExtent
import geotrellis.spark.join.VectorJoin
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector
import geotrellis.vector.Extent.toPolygon
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry
import org.openeo.geotrellis.OpenEOProcessScriptBuilder.AnyProcess
import org.openeo.geotrellis.file.{AbstractPyramidFactory, FixedFeaturesOpenSearchClient}
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrellis.{OpenEOProcessScriptBuilder, sortableSourceName}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, ByKeyPartitioner, CloudFilterStrategy, ConfigurableSpatialPartitioner, DataCubeParameters, DatacubeSupport, L1CCloudFilterStrategy, MaskTileLoader, NoCloudFilterStrategy, ResampledTile, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceTimePartitioner, autoUtmEpsg, retryForever}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.slf4j.LoggerFactory

import java.io.{IOException, Serializable}
import java.net.URI
import java.nio.file.{Path, Paths}
import java.time._
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.{ParMap, ParSeq}
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


// TODO: are these attributes typically propagated as RasterSources are transformed? Maybe we should find another way to
//  attach e.g. a date to a RasterSource.
class BandCompositeRasterSource(override val sources: NonEmptyList[RasterSource],
                                override val crs: CRS,
                                override val attributes: Map[String, String] = Map.empty,
                                val predefinedExtent: Option[GridExtent[Long]] = None,
                               )
  extends MosaicRasterSource { // TODO: don't inherit?

  private val maxRetries = sys.env.getOrElse("GDALREAD_MAXRETRIES", "10").toInt

  protected def reprojectedSources: NonEmptyList[RasterSource] = sources map { _.reproject(crs) }

  protected def reprojectedSources(bands: Seq[Int]): Seq[RasterSource] = {
    val selectedBands = bands.map(sources.toList)

    selectedBands map { rs =>
      try retryForever(Duration.ofSeconds(10), maxRetries)(rs.reproject(crs))
      catch {
        case e: Exception => throw new IOException(s"Error while reading: ${rs.name.toString}", e)
      }
    }
  }

  override def gridExtent: GridExtent[Long] = predefinedExtent.getOrElse {
    try {
      sources.head.gridExtent
    } catch {
      case e: Exception => throw new IOException(s"Error while reading extent of: ${sources.head.name.toString}", e)
    }
  }

  override def cellType: CellType = sources.map(_.cellType).reduceLeft(_ union _)

  override def name: SourceName = sources.head.name
  override def bandCount: Int = sources.size

  override def readBounds(bounds: Traversable[GridBounds[Long]]): Iterator[Raster[MultibandTile]] = {

    val rastersByBounds = reprojectedSources.zipWithIndex.toList.flatMap(s => {
      s._1.readBounds(bounds).zipWithIndex.map(raster_int => ((raster_int._2,(s._2,raster_int._1))))
    }).groupBy(_._1)
    rastersByBounds.toSeq.sortBy(_._1).map(_._2).map((rasters) => {
      val sortedRasters = rasters.toList.sortBy(_._2._1).map(_._2._2)
      Raster(MultibandTile(sortedRasters.map(_.tile.band(0).convert(cellType))), sortedRasters.head.extent)
    }).toIterator

  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = reprojectedSources(bands)

    val singleBandRasters = selectedSources.par
      .map { _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == selectedSources.size)
      Some(Raster(MultibandTile(singleBandRasters.map(_.tile.convert(cellType)).seq), singleBandRasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = reprojectedSources(bands)

    def readBounds(source:RasterSource):Option[Raster[Tile]] = {
      try {
        source.read(bounds, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) }
      } catch {
        case e: Exception => throw new IOException(s"Error while reading ${bounds} from: ${source.name.toString}", e)
      }
    }

    val singleBandRasters = selectedSources.par
      .map(rs => retryForever(Duration.ofSeconds(10), maxRetries)(readBounds(rs)))
      .collect { case Some(raster) => raster }

    try {
      if(singleBandRasters.isEmpty) {
        None
      }else{
        val intersection = singleBandRasters.map(_.extent).reduce((left,right) => left.intersection(right).get)
        val croppedRasters = singleBandRasters.map(_.crop(intersection))
        if (singleBandRasters.size == selectedSources.size) {
          val convertedRasters: Seq[Tile] = croppedRasters.map {
            case Raster(croppedTile: CroppedTile, extent) =>
              croppedTile.sourceTile match {
                case tile: ResampledTile => tile.cropAndConvert(croppedTile.gridBounds, cellType)
                case _ => croppedTile.convert(cellType)
              }
          }.seq
          Some(Raster(MultibandTile(convertedRasters), intersection))
        }
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
    new BandCompositeRasterSource(reprojectedSources map { _.reproject(targetCRS, resampleTarget, method, strategy) },
      crs)
}

// TODO: is this class necessary? Looks like a more general case of BandCompositeRasterSource so maybe the inheritance
//  relationship should be reversed; or maybe the BandCompositeRasterSource could be made more general and accept
//  multi-band RasterSources too.
class MultibandCompositeRasterSource(val sourcesListWithBandIds: NonEmptyList[(RasterSource, Seq[Int])],
                                     override val crs: CRS,
                                     override val attributes: Map[String, String] = Map.empty,
                                    )
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
      crs,
    )
}

object FileLayerProvider {

  private val logger = LoggerFactory.getLogger(classOf[FileLayerProvider])
  private val maxRetries = sys.env.getOrElse("GDALREAD_MAXRETRIES", "10").toInt

  {
    try {
      val gdaldatasetcachesize = Integer.valueOf(System.getenv().getOrDefault("GDAL_DATASET_CACHE_SIZE","32"))
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

  // important: make sure to implement object equality for CacheKey's members
  private case class CacheKey(openSearch: OpenSearchClient, openSearchCollectionId: String, rootPath: Path,
                              pathDateExtractor: PathDateExtractor)

  def apply(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
            maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
            bandIndices: Seq[Int] = Seq(), correlationId: String = "", experimental: Boolean = false,
            retainNoDataTiles: Boolean = false): FileLayerProvider = new FileLayerProvider(
    openSearch, openSearchCollectionId, openSearchLinkTitles, rootPath, maxSpatialResolution, pathDateExtractor,
    attributeValues, layoutScheme, bandIndices, correlationId, experimental, retainNoDataTiles,
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
    if(extent.xmin < -360 || extent.xmax > 360 || extent.ymin < -92 || extent.ymax > 92) {
      false
    }else{
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
      maskObject match {
        case theMask: MultibandTileLayerRDD[SpaceTimeKey] =>
          if (theMask.metadata.bounds.get._1.isInstanceOf[SpaceTimeKey]) {
            val filtered = theMask.withContext {
              _.filter(_._2.band(0).toArray().exists(pixel => pixel == 0)).distinct()
            }
            val maskKeys =
              if (theMask.metadata.crs.equals(metadata.crs) && theMask.metadata.layout.equals(metadata.layout)) {
                filtered
              } else {
                logger.debug(s"mask: automatically resampling mask to match datacube: ${theMask.metadata}")
                filtered.reproject(metadata.crs, metadata.layout, 16, requiredSpacetimeKeys.partitioner)._2
              }
            if (logger.isDebugEnabled) {
              logger.debug(s"SpacetimeMask mask reduces the input to: ${maskKeys.countApproxDistinct()} keys.")
            }
            return requiredSpacetimeKeys.join(maskKeys).map(tuple => (tuple._1, tuple._2._1))
          }
        case _ =>
      }
    }
    return requiredSpacetimeKeys
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
    } else Some((link, bandIndex))
  }

  def createPartitioner(datacubeParams: Option[DataCubeParameters], requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])], filteredSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey]): Some[SpacePartitioner[SpaceTimeKey]] = {
    val requiredSpacetimeKeys: RDD[SpaceTimeKey] = filteredSources.flatMap(_.keys).map {
      tuple => (tuple.spatialKey, tuple)
    }.rightOuterJoin(requiredSpatialKeys).flatMap(_._2._1.toList)
    DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys, metadata)
  }


  private val PIXEL_COUNTER = "InputPixels"

  private def rasterRegionsToTilesLoadPerProductStrategy(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))],
                                   metadata: TileLayerMetadata[SpaceTimeKey],
                                   retainNoDataTiles: Boolean,
                                   cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy,
                                   partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = None,
                                   datacubeParams : Option[DataCubeParameters] = None,
                                  ): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    if(cloudFilterStrategy!=NoCloudFilterStrategy) {
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

    val byBandSource = rasterRegionRDD.flatMap(key_region_sourcename => {
      val source = key_region_sourcename._2._1.asInstanceOf[GridBoundsRasterRegion].source
      val bounds = key_region_sourcename._2._1.asInstanceOf[GridBoundsRasterRegion].bounds
      val result: Seq[(SourceName, (Seq[Int], SpaceTimeKey, RasterRegion))] =
        source match {
          case source1: MultibandCompositeRasterSource =>
            //decompose into individual bands
            //TODO do something like line below, but make sure that band order is maintained! For now we just return the composite source.
            //source1.sourcesListWithBandIds.map(s => (s._1.name, (s._2,key_region_sourcename._1,GridBoundsRasterRegion(s._1, bounds))))
            Seq((source.name, (Seq(0), key_region_sourcename._1, key_region_sourcename._2._1)))

          case source1: BandCompositeRasterSource =>
            //decompose into individual bands
            source1.sources.map(s => (s.name, GridBoundsRasterRegion(new BandCompositeRasterSource(NonEmptyList.one(s),source1.crs,source1.attributes,source1.predefinedExtent), bounds))).zipWithIndex.map(t => (t._1._1, (Seq(t._2), key_region_sourcename._1, t._1._2))).toList.toSeq

          case _ =>
            Seq((source.name, (Seq(0), key_region_sourcename._1, key_region_sourcename._2._1)))

        }

      result
    })

    val allSources: Array[SourceName] =  byBandSource.keys.distinct().collect()
    val theCellType = metadata.cellType

    var tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] = byBandSource.groupByKey(new ByKeyPartitioner(allSources)).mapPartitions((partitionIterator: Iterator[(SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])]) => {
      var totalPixelsPartition = 0
      val startTime = System.currentTimeMillis()

      val (loadedPartitions: Iterator[(SpaceTimeKey, (Int, MultibandTile))],partitionPixels) = loadPartitionBySource(partitionIterator, cloudFilterStrategy, totalChunksAcc, tracker,crs,layout,theCellType )
      totalPixelsPartition += partitionPixels

      val durationMillis = System.currentTimeMillis() - startTime
      if (totalPixelsPartition > 0) {
        val secondsPerChunk = (durationMillis / 1000.0) / (totalPixelsPartition / (256 * 256))
        loadingTimeAcc.add(secondsPerChunk)
      }
      loadedPartitions

    },preservesPartitioning = true).groupByKey(partitioner).mapValues((tiles: Iterable[(Int, MultibandTile)]) => {
      val mergedBands: Map[Int, Option[MultibandTile]] = tiles.groupBy(_._1).mapValues(_.map(_._2).reduceOption(_ merge _))
      MultibandTile(mergedBands.toSeq.sortBy(_._1).flatMap(_._2.get.bands))

    } ).filter { case (_, tile) => retainNoDataTiles ||  !tile.bands.forall(_.isNoDataTile) }

    tiledRDD = DatacubeSupport.applyDataMask(datacubeParams,tiledRDD,metadata, pixelwiseMasking = true)

    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD

  }

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

    tiledRDD = DatacubeSupport.applyDataMask(datacubeParams,tiledRDD,metadata, pixelwiseMasking = true)

    val cRDD = ContextRDD(tiledRDD, metadata)
    cRDD.name = rasterRegionRDD.name
    cRDD
  }


  private def loadPartitionBySource(partitionIterator: Iterator[(SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])], cloudFilterStrategy: CloudFilterStrategy, totalChunksAcc: LongAccumulator, tracker: BatchJobMetadataTracker, crs :CRS, layout:LayoutDefinition, cellType: CellType )= {
    var totalPixelsPartition = 0
    val tiles: Iterator[(SpaceTimeKey, (Int,MultibandTile))] = partitionIterator.flatMap((tuple: (SourceName, Iterable[(Seq[Int], SpaceTimeKey, RasterRegion)])) =>{
      val keys = tuple._2.map(_._2).asJavaCollection
      val source = tuple._2.head._3.asInstanceOf[GridBoundsRasterRegion].source
      val bounds = tuple._2.map(_._3.asInstanceOf[GridBoundsRasterRegion].bounds).toSeq
      val intersections: Seq[Option[GridBounds[Long]]] = bounds.map(_.intersection(source.dimensions)).toSeq
      //TODO this assumes that the index is actually the index of this band in the eventual multiband tile, not the index to read from the source
      val theIndex = tuple._2.flatMap(_._1).head

      val allRasters =
        try{
          bounds.toIterator.flatMap(b => retryForever(Duration.ofSeconds(10),maxRetries)(source.read(b).iterator)).map(_.mapTile(_.convert(cellType))).toSeq
        } catch {
          case e: Exception => throw new IOException(s"load_collection/load_stac: error while reading from: ${source.name.toString}. Detailed error: ${e.getMessage}", e)
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
            _.mapBands { (_, band) => PaddedTile(band, colOffset.toInt, rowOffset.toInt, theBounds.width.toInt, theBounds.height.toInt) }
          })
        }
      }}

      totalPixelsPartition += totalPixels
      totalChunksAcc.add(totalPixels / (256 * 256))
      tracker.add(PIXEL_COUNTER, totalPixels)
      keys.iterator().asScala.zip(paddedRasters.map(b=>(theIndex,b.tile)).iterator)

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
                  val maybeTile = rasterRegion.raster.map(_.tile)
                  if (maybeTile.isDefined && maybeTile.get.cellType.isInstanceOf[NoNoData]) {
                    maybeTile.map(t => t.convert(t.cellType.withDefaultNoData()))
                  } else {
                    maybeTile
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

  private def featuresRDD(geometricFeatures: Seq[vector.Feature[Geometry, (RasterSource, Feature)]], metadata: TileLayerMetadata[SpaceTimeKey], targetCRS: CRS, workingPartitioner: SpacePartitioner[SpatialKey], maybeKeys: Option[RDD[(SpatialKey, Iterable[Geometry])]] ,sc: SparkContext) = {
    val emptyPoint = Point(0.0, 0.0)
    val cubeExtent = metadata.extent

    val inputNumberOfPartitions = if(maybeKeys.isDefined) {
      //spatial keys are already known and will determine partitioning?
      10
    }else{
      //cliptogrid generates a lot of keys, so requires more memory
      math.max(1, geometricFeatures.size)
    }

    val clippedFeatures: RDD[vector.Feature[Geometry, (RasterSource, Feature)]] = sc.parallelize(geometricFeatures, inputNumberOfPartitions)
      .map(eoProductFeature => {

        val productCRSOrDefault = eoProductFeature.data._2.crs.getOrElse(targetCRS)
        eoProductFeature.mapGeom(productGeometry => {
          try {
            val intersection = productGeometry.reproject(LatLng, productCRSOrDefault).intersection(cubeExtent.reprojectAsPolygon(targetCRS, productCRSOrDefault, 0.01))
            if (intersection.isValid && intersection.getArea > 0.0) {
              intersection.reproject(productCRSOrDefault, targetCRS)
            } else {
              emptyPoint
            }
          } catch {
            case e: Exception => logger.warn("Exception while determining intersection.", e); emptyPoint
          }

        })
      }).filter(!_.geom.equals(emptyPoint))

    if(maybeKeys.isDefined) {
      val transform = metadata.mapTransform
      val geometryToKey: RDD[vector.Feature[Polygon, SpatialKey]] = maybeKeys.get.keys.map(k=>{
        vector.Feature(transform.apply(k).toPolygon(),k)
      })

      implicit val theContext: SparkContext = sc
      val joined: RDD[(vector.Feature[Geometry, (RasterSource, Feature)], vector.Feature[Polygon, SpatialKey])] = VectorJoin(clippedFeatures,geometryToKey, (a, b)=>{a.intersects(b)})
      joined.map(t=>(t._2.data,t._1))

    }else{
      clippedFeatures.clipToGrid(metadata.layout).partitionBy(workingPartitioner)
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
                        retainNoDataTiles: Boolean,
                        disambiguateConstructors: Null) extends LayerProvider { // workaround for: constructors have the same type after erasure

  import DatacubeSupport._
  import FileLayerProvider._

  @deprecated("call a constructor/factory method with flattened bandIndices instead of nested bandIds")
  // TODO: remove this eventually (e.g. after updating geotrellistimeseries)
  def this(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
           maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
           bandIds: Seq[Seq[Int]] = Seq(), correlationId: String = "", experimental: Boolean = false,
           retainNoDataTiles: Boolean = false) = this(openSearch, openSearchCollectionId,
           openSearchLinkTitles = NonEmptyList.fromListUnsafe(for {
             (title, bandIndices) <- openSearchLinkTitles.toList.zipAll(bandIds, thisElem = "", thatElem = Seq(0))
             _ <- bandIndices
           } yield title),
           rootPath, maxSpatialResolution, pathDateExtractor, attributeValues, layoutScheme,
           bandIndices = bandIds.flatten,
           correlationId, experimental,
           retainNoDataTiles, disambiguateConstructors = null)

  assert(bandIndices.isEmpty || bandIndices.size == openSearchLinkTitles.size)

  if(experimental) {
    logger.warn("Experimental features enabled for: " + openSearchCollectionId)
  }

  private val _rootPath = if(rootPath != null) Paths.get(rootPath) else null

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
      if (commonCellType.isInstanceOf[NoNoData]) commonCellType.withDefaultNoData() else commonCellType
    } catch {
      case e: Exception => throw new IOException(s"Exception while determining data type of asset ${arbitraryRasterSource.name} in collection $openSearchCollectionId. Detailed message: ${e.getMessage}", e)
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

    val dates = overlappingRasterSources.map(_._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC")))

    var commonCellType: CellType = determineCelltype(overlappingRasterSources)

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
        overlappingRasterSources = GDALCloudRasterSource.filterRasterSources(overlappingRasterSources, maskParams)
        maskStrategy = Some(new L1CCloudFilterStrategy(GDALCloudRasterSource.getDilationDistance(maskParams.asScala.toMap)))
      }
    }


    val workingPartitioner = SpacePartitioner(metadata.bounds.get.toSpatial)(implicitly,implicitly,new ConfigurableSpatialPartitioner(3))
    val requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] =
      if(maxSpatialKeyCount<=2 && bufferedPolygons.length==1) {
        //reduce complexity for small (synchronous) requests
        val keys = metadata.keysForGeometry(toPolygon(metadata.extent))
        keysRDD(sc, keys)
      }else{
        val polygonsRDD = sc.parallelize(bufferedPolygons, math.max(1, bufferedPolygons.length / 2)).map {
          _.reproject(polygons_crs, targetCRS)
        }


        val clipped = clipToGridWithErrorHandling(polygonsRDD, metadata)


        var requiredSpatialKeysLocal: RDD[(SpatialKey, Iterable[Geometry])] = clipped.groupByKey(workingPartitioner)

        val spatialKeyCount: Long =
          if (polygons.length == 1) {
            //special case for single bbox request
            maxSpatialKeyCount
          } else {
            requiredSpatialKeysLocal.map(_._1).countApproxDistinct()
          }
        logger.info(s"Datacube requires approximately ${spatialKeyCount} spatial keys.")


        val retiledMetadata: Option[TileLayerMetadata[SpaceTimeKey]] = DatacubeSupport.optimizeChunkSize(metadata, bufferedPolygons, datacubeParams, spatialKeyCount)
        metadata = retiledMetadata.getOrElse(metadata)

        if (retiledMetadata.isDefined) {
          requiredSpatialKeysLocal = clipToGridWithErrorHandling(polygonsRDD, retiledMetadata.get).groupByKey(workingPartitioner)
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
    val griddedRasterSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] =  featuresRDD(geometricFeatures, metadata, targetCRS, workingPartitioner,keysIfSparse, sc)


    val filteredSources: RDD[(SpatialKey, vector.Feature[Geometry, (RasterSource, Feature)])] = applySpatialMask(datacubeParams, griddedRasterSources,metadata)


    var requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = filteredSources.map(t => (SpaceTimeKey(t._1, TemporalKey(t._2.data._2.nominalDate.toLocalDate.atStartOfDay(ZoneId.of("UTC")))), t._2))

    requiredSpacetimeKeys = applySpaceTimeMask(datacubeParams, requiredSpacetimeKeys,metadata)
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
            //try to detect tiles that are on the edge of the footprint
            val sourcePolygon = source.data._2.geometry.getOrElse(source.data._2.bbox.toPolygon()).reproject(LatLng, targetCRS)
            val sourceExtent = sourcePolygon.extent
            /**-
             * Effect of buffer multiplication factor:
             *  - larger buffer -> shrink source footprint more -> tiles close to edge get discarded faster, this matters for scl_dilation
             */
            val contains = sourcePolygon.contains(extent)

            val sourcePolygonBuffered = sourcePolygon.buffer(-1.5*math.max(extent.width,extent.height))
            val distanceToFootprint =
            if(sourcePolygonBuffered.isEmpty) {
              if(contains) {
                extent.distance(sourcePolygon.getCentroid)
              }else{
                extent.distance(sourcePolygon.getCentroid) + 0.00001 //avoid that distance become zero
              }

            }else{
              extent.distance(sourcePolygonBuffered)
            }



            val distanceBetweenCenters = extent.center.distance(sourceExtent.center)
            ((distanceBetweenCenters,distanceToFootprint,contains), source)
          })
          val smallestCenterDistance = distances.map(_._1._1).min
          val smallestDistanceToFootprint = distances.map(_._1._2).min
          if(smallestDistanceToFootprint > 0) {
            val fullyContained = distances.filter(_._1._3).map(distance_source => (key, distance_source._2))
            if(fullyContained.nonEmpty) {
              fullyContained
            }else{
              return_original
            }
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



  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon], polygons_crs: CRS, zoom: Int, sc: SparkContext, datacubeParams : Option[DataCubeParameters]): MultibandTileLayerRDD[SpaceTimeKey] = {

    val readKeysToRasterSourcesResult: (RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])], TileLayerMetadata[SpaceTimeKey], Option[CloudFilterStrategy], Seq[(RasterSource, Feature)]) = readKeysToRasterSources(from,to, boundingBox, polygons, polygons_crs, zoom, sc, datacubeParams)

    var maskStrategy: Option[CloudFilterStrategy] = readKeysToRasterSourcesResult._3
    val metadata = readKeysToRasterSourcesResult._2
    val requiredSpacetimeKeys: RDD[(SpaceTimeKey, vector.Feature[Geometry, (RasterSource, Feature)])] = readKeysToRasterSourcesResult._1.persist()
    requiredSpacetimeKeys.setName(s"FileLayerProvider_keys_${this.openSearchCollectionId}_${from.toString}_${to.toString}")

    try{

      val spatialBounds = metadata.bounds.get.toSpatial
      val maxKeys = (spatialBounds.maxKey.col - spatialBounds.minKey.col + 1) * (spatialBounds.maxKey.row - spatialBounds.minKey.row + 1)

      val partitioner: Option[SpacePartitioner[SpaceTimeKey]] = {
        if(maxKeys>4) {
          DatacubeSupport.createPartitioner(datacubeParams, requiredSpacetimeKeys.keys, metadata)
        }else{
          //for low number of spatial keys, we can construct sparse partitioner in a cheaper way
          val reduction: Int = datacubeParams.map(_.partitionerIndexReduction).getOrElse(SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION)
          val keys = metadata.keysForGeometry(toPolygon(metadata.extent))
          val dates = readKeysToRasterSourcesResult._4.map(_._2.nominalDate).distinct
          val allKeys: Set[SpaceTimeKey] = for {x <- keys; y <- dates} yield SpaceTimeKey(x, TemporalKey(y))
          val indices = allKeys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = reduction)).toArray.sorted
          Some(SpacePartitioner(metadata.bounds)(SpaceTimeKey.Boundable, ClassTag(classOf[SpaceTimeKey]), new SparseSpaceTimePartitioner(indices, reduction,theKeys = Some(allKeys.toArray))))
        }

      }

      val layoutDefinition = metadata.layout
      val resample = math.abs(layoutDefinition.cellSize.resolution - maxSpatialResolution.resolution) >= 0.0000001 * layoutDefinition.cellSize.resolution
      val reduction = if (resample) 1 else 5
      //resampling is still needed in case bounding boxes are not aligned with pixels
      // https://github.com/Open-EO/openeo-geotrellis-extensions/issues/69
      val theResampleMethod = datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor)

      val regions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = requiredSpacetimeKeys
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

      regions.name = s"FileCollection-${openSearchCollectionId}"

      val theMaskStrategy: CloudFilterStrategy = maskStrategy.getOrElse(NoCloudFilterStrategy)

      //convert to raster region
      val cube=
        if(!datacubeParams.map(_.loadPerProduct).getOrElse(false) || theMaskStrategy != NoCloudFilterStrategy ){
          rasterRegionsToTiles(regions, metadata, retainNoDataTiles, theMaskStrategy, partitioner, datacubeParams)
        }else{
          rasterRegionsToTilesLoadPerProductStrategy(regions, metadata, retainNoDataTiles, NoCloudFilterStrategy, partitioner, datacubeParams)
        }
      logger.info(s"Created cube for ${openSearchCollectionId} with metadata ${cube.metadata} and partitioner ${cube.partitioner}")
      cube
    }finally{
      requiredSpacetimeKeys.unpersist(false)
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

  private def deriveRasterSources(feature: Feature, targetExtent:ProjectedExtent, datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Option[(BandCompositeRasterSource, Feature)] = {
    def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
      Extent(
        extent.xmin,
        extent.ymin,
        math.max(extent.xmax, extent.xmin + cellSize.width),
        math.max(extent.ymax, extent.ymin + cellSize.height),
      )

    val noResampleOnRead = datacubeParams.exists(_.noResampleOnRead)
    val theResolution = targetResolution.getOrElse(maxSpatialResolution)
    val re = RasterExtent(expandToCellSize(targetExtent.extent,theResolution), theResolution)

    val featureExtentInLayout: Option[GridExtent[Long]]=
    if (feature.rasterExtent.isDefined && feature.crs.isDefined) {

      /**
       * Several edge cases to cover:
       *  - if feature extent is whole world, it may be invalid in target crs
       *  - if feature is in utm, target extent may be invalid in feature crs
       *  this is why we take intersection
       */
      val targetExtentInLatLon = targetExtent.reproject(feature.crs.get)
      val featureExtentInLatLon = feature.rasterExtent.get.reproject(feature.crs.get,LatLng)

      val intersection = featureExtentInLatLon.intersection(targetExtentInLatLon).map(_.buffer(1.0)).getOrElse(featureExtentInLatLon)
      val tmp = expandToCellSize(intersection.reproject(LatLng, targetExtent.crs), theResolution)

      val alignedToTargetExtent = re.createAlignedRasterExtent(tmp)
      Some(alignedToTargetExtent.toGridType[Long])
    }else{
      Some(re.toGridType[Long])
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
      if (path.startsWith("/vsicurl/")) path.replaceFirst("/vsicurl/", "")
      else if (path.startsWith("/vsis3/eodata/"))
        path.replaceFirst("/vsis3/eodata/", "https://finder.creodias.eu/files/")
      else if (path.startsWith("/eodata/"))
        path.replaceFirst("/eodata/", "https://zipper.creodias.eu/get-object?path=/")
      else if (path.startsWith("http")) path
      else {
        logger.warn("unexpected path: " + path)
        path
      }
    }

    def rasterSource(dataPath:String, cloudPath:Option[(String,String)], targetCellType:Option[TargetCellType], targetExtent:ProjectedExtent, sentinelXmlAngleBandIndex: Int): RasterSource = {
      if(dataPath.endsWith(".jp2") || dataPath.contains("NETCDF:")) {
        val alignPixels = !dataPath.contains("NETCDF:") //align target pixels does not yet work with CGLS global netcdfs
        val warpOptions = GDALWarpOptions(alignTargetPixels = alignPixels, cellSize = Some(theResolution), targetCRS = Some(targetExtent.crs), resampleMethod = Some(resampleMethod),
          te = featureExtentInLayout.map(_.extent), teCRS = Some(targetExtent.crs)
        )
        if (cloudPath.isDefined) {
          GDALCloudRasterSource(cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = targetCellType)
        } else {
          predefinedExtent = featureExtentInLayout
          GDALRasterSource(GDALPath(dataPath.replace("/vsis3/eodata/", "/vsis3/EODATA/").replace("https", "/vsicurl/https")), options = warpOptions, targetCellType = targetCellType)
        }
      }else if(dataPath.endsWith("MTD_TL.xml")) {
        val targetProjectedExtent = featureExtentInLayout match {
          case None => None
          case Some(featureExtentInLayoutGet) =>
            Some(ProjectedExtent(featureExtentInLayoutGet.extent, targetExtent.crs))
        }
        SentinelXMLMetadataRasterSource.forAngleBand(dataPath, sentinelXmlAngleBandIndex, targetProjectedExtent, Some(theResolution))
      }
      else {
        def alignmentFromDataPath(dataPath: String, projectedExtent: ProjectedExtent): TargetRegion = {
            // When noResampleOnRead is set, we retrieve the actual resolution from the dataPath.
            // Note: This is only supported for S2 dataPaths.
            // E.g. S2A_20190307T105021_31UFT_TOC-B05_20M_V200.tif = 20.0
            val splitPath: Array[String] = dataPath.split("_")
            val tiffResolution = splitPath(splitPath.length - 2).replace("M", "").toDouble
            val tiffCellSize = CellSize(tiffResolution, tiffResolution)
            val tiffRe = RasterExtent(expandToCellSize(projectedExtent.extent, tiffCellSize), tiffCellSize)
            TargetRegion(tiffRe)
        }
        if( feature.crs.isDefined && feature.crs.get != null && feature.crs.get.equals(targetExtent.crs)) {
          // when we don't know the feature (input) CRS, it seems that we assume it is the same as target extent???
          if(experimental) {
            GDALRasterSource(dataPath, options = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(theResolution), resampleMethod=Some(resampleMethod)), targetCellType = targetCellType)
          }else{
            val geotiffPath = GeoTiffPath(dataPath.replace("/vsis3/eodata/","S3://EODATA/"))
            if (noResampleOnRead) {
              val tiffAlignment = alignmentFromDataPath(dataPath, targetExtent)
              val geotiffRasterSource = GeoTiffRasterSource(geotiffPath, targetCellType)
              new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, theResolution)
            } else {
              GeoTiffResampleRasterSource(geotiffPath, alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType, None)
            }
          }
        }else{
          if(experimental) {
            val warpOptions = GDALWarpOptions(alignTargetPixels = false, cellSize = Some(theResolution), targetCRS=Some(targetExtent.crs), resampleMethod = Some(resampleMethod),te = Some(targetExtent.extent))
            GDALRasterSource(dataPath.replace("/vsis3/eodata/","/vsis3/EODATA/").replace("https", "/vsicurl/https"), options = warpOptions, targetCellType = targetCellType)
          }else{
            val geotiffPath = GeoTiffPath(dataPath.replace("/vsis3/eodata/","S3://EODATA/"))
            if (noResampleOnRead) {
              val tiffAlignment = alignmentFromDataPath(dataPath, targetExtent)
              val geotiffRasterSource = GeoTiffReprojectRasterSource(geotiffPath, targetExtent.crs, tiffAlignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType = targetCellType)
              new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, theResolution)
            } else {
              GeoTiffReprojectRasterSource(geotiffPath, targetExtent.crs, alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType = targetCellType)
            }
          }
        }
      }
    }

    val bandNames = openSearchLinkTitles.toList



    def getBandAssetsByBandInfo: Seq[Option[(Link, Int)]] = { // [Some((href, bandIndex))]
      def getBandAsset(bandName: String): Option[(Link, Int)] = { // (href, bandIndex)
        feature.links
          .flatMap(link => link.bandNames match {
            case Some(assetBandNames) =>
              val bandIndex = assetBandNames.indexWhere(_ == bandName)
              if (bandIndex >= 0) {
                convertNetcdfLinksToGDALFormat(link, bandName, bandIndex)
              } else None
            case _ => None
          })
          .headOption
          .orElse {
            logger.warn(s"asset with band name $bandName not found in feature ${feature.id}; inserting NODATA band instead")
            None
          }
      }

      bandNames
        .map(getBandAsset)
    }

    def getBandAssetsByLinkTitle : Seq[Option[(Link, Int)]] = for {
      (title, bandIndex) <- openSearchLinkTitlesWithBandId.toList
      linkWithTitle = feature.links.find(_.title.map(_.toUpperCase) contains title.toUpperCase).orElse {
        logger.warn(s"asset with ID/title $title not found in feature ${feature.id}; inserting NODATA band instead")
        None
      }
    } yield linkWithTitle.map(convertNetcdfLinksToGDALFormat(_,title,bandIndex).get)

    // TODO: pass a strategy to FileLayerProvider instead (incl. one for the PROBA-V workaround)
    val byLinkTitle = !openSearch.isInstanceOf[FixedFeaturesOpenSearchClient]

    val expectedNumberOfBands = openSearchLinkTitlesWithBandId.size

    lazy val cloudPath = for {
      cloudDataPath <- feature.links.find(_.title contains "FineCloudMask_Tile1_Data").map(_.href.toString)
      metadataPath <- feature.links.find(_.title contains "S2_Level-1C_Tile1_Metadata").map(_.href.toString)
    } yield (cloudDataPath, metadataPath)

    val rasterSources: Seq[Option[(RasterSource, Int)]] =
      (if (byLinkTitle) getBandAssetsByLinkTitle else getBandAssetsByBandInfo).map {
        case Some((link, bandIndex)) =>
          val path = deriveFilePath(link.href)

          //special case handling for data that does not declare nodata properly
          val targetCellType = link.title match {
            // An un-used band called "IMG_DATA_Band_SCL_60m_Tile1_Unit" exists, so not specifying the resulution in the if-check.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0)))
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(UShortConstantNoDataCellType))
            case _ => None
          }

          val pixelValueOffset: Double = link.pixelValueOffset.getOrElse(0)
          val targetTargetCellType: Option[TargetCellType] = link.title match {
            // Sentinel 2 bands can have negative values now.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => None
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(ShortConstantNoDataCellType))
            case _ => None
          }

          val rasterSourceRaw = rasterSource(path, cloudPath, targetCellType, targetExtent, sentinelXmlAngleBandIndex = bandIndex)
          val rasterSourceWrapped = ValueOffsetRasterSource.wrapRasterSource(rasterSourceRaw, pixelValueOffset, targetTargetCellType)
          Some((rasterSourceWrapped, bandIndex))
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

        Some((new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetExtent.crs, attributes, predefinedExtent = predefinedExtent), feature))
      } else Some((new MultibandCompositeRasterSource(sources.map { case (rasterSource, bandIndex) => (rasterSource, Seq(bandIndex))}, targetExtent.crs, attributes), feature))
    }
  }

  def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int,datacubeParams : Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty): Seq[(RasterSource,Feature)] = {
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


    val reprojectedBoundingBox: ProjectedExtent = targetBoundingBox(boundingBox, layoutScheme)
    val overlappingRasterSources = (for {
      feature <- overlappingFeatures
    } yield  deriveRasterSources(feature,reprojectedBoundingBox, datacubeParams,targetResolution)).flatMap(_.toList)

    BatchJobMetadataTracker.tracker("").addInputProducts(openSearchCollectionId,overlappingRasterSources.map(_._2.id).asJava)
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

    layerMetadata(projectedExtent, dates.head, dates.last, zoom min maxZoom, FloatConstantNoDataCellType, layoutScheme,
      maxSpatialResolution)
  }

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) = loadMetadata(sc).get

  override def toString: String =
    s"${getClass.getSimpleName}($openSearchCollectionId, ${openSearchLinkTitlesWithBandId.map(_._1).toList.mkString("[", ", ", "]")}, $rootPath)"
}
