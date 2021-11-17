package org.openeo.geotrellis.layers

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchResponses.Feature
import cats.data.NonEmptyList
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import geotrellis.layer.{TemporalKeyExtractor, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.RasterRegion.GridBoundsRasterRegion
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, PaddedTile, Raster, RasterExtent, RasterMetadata, RasterRegion, RasterSource, ResampleMethod, ResampleTarget, SourceName, SourcePath, TargetAlignment, TargetCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.proj4j.proj.TransverseMercatorProjection
import org.openeo.geotrelliscommon.{CloudFilterStrategy, DataCubeParameters, L1CCloudFilterStrategy, MaskTileLoader, NoCloudFilterStrategy, SCLConvolutionFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceOnlyPartitioner, SparseSpaceTimePartitioner}
import org.slf4j.LoggerFactory

import java.io.IOException
import java.net.{URI, URL}
import java.nio.file.{Path, Paths}
import java.time._
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.matching.Regex

class BandCompositeRasterSource(val sourcesList: NonEmptyList[RasterSource], override val crs: CRS, var theAttributes:Map[String,String]=Map.empty)
  extends MosaicRasterSource { // FIXME: don't inherit?

  override val sources: NonEmptyList[RasterSource] = sourcesList
  protected def reprojectedSources: NonEmptyList[RasterSource] = sourcesList map { _.reproject(crs) }

  override def gridExtent: GridExtent[Long] = {
    try {
      sources.head.gridExtent
    }  catch {
      case e: Throwable => throw new IOException(s"Error while reading extent of: ${sources.head.name.toString}",e)
    }

  }
  override def cellType: CellType = sources.map(_.cellType).reduceLeft(_ union _)

  override def attributes: Map[String, String] = theAttributes // TODO: use override val attributes instead
  override def name: SourceName = sources.head.name
  override def bandCount: Int = sources.size

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = bands.map(reprojectedSources.toList)
    val singleBandRasters = selectedSources.par
      .map { _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == selectedSources.size) Some(Raster(MultibandTile(singleBandRasters.map(_.tile).seq), singleBandRasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources = bands.map(reprojectedSources.toList)
    val singleBandRasters = selectedSources.par
      .map { _.read(bounds, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == selectedSources.size) Some(Raster(MultibandTile(singleBandRasters.map(_.tile.convert(cellType)).seq), singleBandRasters.head.extent))
    else None
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

class MultibandCompositeRasterSource(val sourcesListWithBandIds: NonEmptyList[(RasterSource, Seq[Int])], override val crs: CRS, override val attributes: Map[String,String] = Map.empty)
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

  /**
   * Find best CRS, can be location dependent (UTM)
   * @param boundingBox
   * @return
   */
  def bestCRS(boundingBox: ProjectedExtent,layoutScheme:LayoutScheme):CRS = {
    layoutScheme match {
      case scheme: ZoomedLayoutScheme => scheme.crs
      case scheme: FloatingLayoutScheme => boundingBox.crs //TODO determine native CRS based on collection metadata, not bbox?
    }
  }

  def layerMetadata(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, cellType: CellType,
                    layoutScheme:LayoutScheme, maxSpatialResoluton: CellSize) = {

    val worldLayout: LayoutDefinition = getLayout(layoutScheme, boundingBox, zoom, maxSpatialResoluton)

    val reprojectedBoundingBox: ProjectedExtent = targetBoundingBox(boundingBox, layoutScheme)

    val metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, from, to, cellType)
    metadata
  }

  private def targetBoundingBox(boundingBox: ProjectedExtent, layoutScheme: LayoutScheme) = {
    val crs = bestCRS(boundingBox, layoutScheme)
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)
    reprojectedBoundingBox
  }

  def getLayout(layoutScheme: LayoutScheme, boundingBox: ProjectedExtent, zoom: Int, maxSpatialResolution: CellSize) = {
    val LayoutLevel(_, worldLayout) = layoutScheme match {
      case scheme: ZoomedLayoutScheme => scheme.levelForZoom(zoom)
      case scheme: FloatingLayoutScheme => {
        //Giving the layout a deterministic extent simplifies merging of data with spatial partitioner
        val layoutExtent: Extent = {
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
            val extent = boundingBox.extent
            if(extent.width < maxSpatialResolution.width || extent.height < maxSpatialResolution.height) {
              Extent(extent.xmin,extent.ymin,Math.max(extent.xmax,extent.xmin + maxSpatialResolution.width),Math.max(extent.ymax,extent.ymin + maxSpatialResolution.height))
            }else{
              extent
            }
          }
        }

        scheme.levelFor(layoutExtent, maxSpatialResolution)
      }
    }
    worldLayout
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
        if(noResampling)
          LayoutTileSource(rs,metadata.layout,tileKeyTransform)
        else
          rs.tileToLayout(metadata.layout, tileKeyTransform)
      }

    tiledLayoutSourceRDD
  }

  def readMultibandTileLayer(rasterSources: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey], polygons: Array[MultiPolygon], polygons_crs: CRS, sc: SparkContext, cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, useSparsePartitioner: Boolean = true, datacubeParams : Option[DataCubeParameters] = Option.empty) = {
    // The requested polygons dictate which SpatialKeys will be read from the source files/streams.
    var requiredSpatialKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons).map {
      _.reproject(polygons_crs, metadata.crs)
    }.clipToGrid(metadata.layout).groupByKey()

    if(datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject =  datacubeParams.get.maskingCube.get
      maskObject match {
        case theSpatialMask: MultibandTileLayerRDD[SpatialKey] =>
          if(theSpatialMask.metadata.bounds.get._1.isInstanceOf[SpatialKey]) {
            val maskSpatialKeys = theSpatialMask.filter(_._2.band(0).toArray().exists(pixel => pixel == 0)).distinct()
            if(logger.isDebugEnabled) {
              logger.debug(s"Spatial mask reduces the input to: ${maskSpatialKeys.count()} keys.")
            }
            requiredSpatialKeys = requiredSpatialKeys.join(maskSpatialKeys).map(tuple => (tuple._1, tuple._2._1))
          }
        case _ =>
      }
    }

    // Remove all source files that do not intersect with the 'interior' of the requested extent.
    // Note: A normal intersect would also include sources that exactly border the requested extent.
    val filteredSources: RDD[LayoutTileSource[SpaceTimeKey]] = rasterSources.filter({ tiledLayoutSource =>
      tiledLayoutSource.source.extent.interiorIntersects(tiledLayoutSource.layout.extent)
    })

    // The requested sources already contain the requested dates for every tile (if they exist).
    // We can join these dates with the requested spatial keys.

    val partitioner = useSparsePartitioner match {
      case true => {
        val requiredSpacetimeKeys: RDD[SpaceTimeKey] = filteredSources.flatMap(_.keys).map {
          tuple => (tuple.spatialKey, tuple)
        }.rightOuterJoin(requiredSpatialKeys).flatMap(_._2._1.toList)
        // The sparse partitioner will split the final RDD into a single partition for every SpaceTimeKey.
        val reduction: Int = datacubeParams.map(_.partitionerIndexReduction).getOrElse(8)
        val partitionerIndex: PartitionerIndex[SpaceTimeKey] = {
          if(datacubeParams.isDefined && datacubeParams.get.partitionerTemporalResolution!= "ByDay") {
            val indices = requiredSpacetimeKeys.map(SparseSpaceOnlyPartitioner.toIndex(_,indexReduction = reduction)).distinct().collect().sorted
            new SparseSpaceOnlyPartitioner(indices,reduction)
          }else{
            val indices = requiredSpacetimeKeys.map(SparseSpaceTimePartitioner.toIndex(_,indexReduction = reduction)).distinct().collect().sorted
            new SparseSpaceTimePartitioner(indices,reduction)
          }
        }
        Some(SpacePartitioner(metadata.bounds)(SpaceTimeKey.Boundable,
                                               ClassTag(classOf[SpaceTimeKey]), partitionerIndex))
      }
      case false => Option.empty
    }

    // Convert RasterSources to RasterRegions.
    val rasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] =
      filteredSources
        .flatMap { tiledLayoutSource =>
          tiledLayoutSource.keyedRasterRegions()
            //this filter step reduces the 'Shuffle Write' size of this stage, so it already
            .filter({case(key, rasterRegion) => metadata.extent.intersects(key.spatialKey.extent(metadata.layout)) } )
            .map { case (key, rasterRegion) => (key, (rasterRegion, tiledLayoutSource.source.name)) }
        }

    // Only use the regions that correspond with a requested spatial key.
    var requestedRasterRegions: RDD[(SpaceTimeKey, (RasterRegion, SourceName))]  =
      rasterRegions
        .map { tuple => (tuple._1.spatialKey, tuple) }
        //stage boundary, first stage of data loading ends here!
        .rightOuterJoin(requiredSpatialKeys).flatMap { t => t._2._1.toList }

    if(datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject =  datacubeParams.get.maskingCube.get
      maskObject match {
        case spacetimeMask: MultibandTileLayerRDD[SpaceTimeKey] =>
          if(spacetimeMask.metadata.bounds.get._1.isInstanceOf[SpaceTimeKey]) {
            if(logger.isDebugEnabled) {
              logger.debug(s"Spacetime mask is used to reduce input.")
            }
            val theFilteredMask = spacetimeMask.filter(_._2.band(0).toArray().exists(pixel => pixel == 0))
            requestedRasterRegions = requestedRasterRegions.join(theFilteredMask).map((tuple => (tuple._1, tuple._2._1)))
          }
        case _ =>
      }
    }

    rasterRegionsToTiles(requestedRasterRegions, metadata, cloudFilterStrategy, partitioner)
  }

  private def rasterRegionsToTiles(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))], metadata: TileLayerMetadata[SpaceTimeKey], cloudFilterStrategy: CloudFilterStrategy = NoCloudFilterStrategy, partitionerOption: Option[SpacePartitioner[SpaceTimeKey]] = Option.empty) = {
    val partitioner = partitionerOption.getOrElse(SpacePartitioner(metadata.bounds))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(partitioner)
        .flatMapValues { namedRasterRegions => {
          val allRegions = namedRasterRegions.toSeq
          val filteredRegions =
          if(allRegions.size<2 || cloudFilterStrategy == NoCloudFilterStrategy) {
            allRegions
          }else{
            val regionsWithDistance = allRegions.map(r=>{
              val bounds = r._1.asInstanceOf[GridBoundsRasterRegion].bounds
              val rasterBounds = r._1.asInstanceOf[GridBoundsRasterRegion].source.gridExtent
              val minDistanceToTheEdge: Long = Seq(bounds.colMin.abs,bounds.rowMin.abs,Math.abs(rasterBounds.cols - bounds.colMax),Math.abs(rasterBounds.rows - bounds.rowMax)).min
              (minDistanceToTheEdge,r)
            })
            val largestDistanceToTheEdgeOfTheRaster = regionsWithDistance.map(_._1).max
            regionsWithDistance.filter(_._1 == largestDistanceToTheEdgeOfTheRaster).map(_._2)
          }

          filteredRegions
            .flatMap { case (rasterRegion, sourcePath: SourcePath) =>
              val result: Option[(MultibandTile, SourcePath)] = cloudFilterStrategy match {
                case l1cFilterStrategy: L1CCloudFilterStrategy =>
                  if (GDALCloudRasterSource.isRegionFullyClouded(rasterRegion, metadata.crs, metadata.layout, l1cFilterStrategy.bufferInMeters)) {
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
                          val cloudPolygon = MultiPolygon(cloudPolygons).reproject(cloudRasterSource.crs, metadata.crs)
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
                        val expectedTileSize = 456

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

                    override def loadData: Option[MultibandTile] = rasterRegion.raster.map(_.tile)
                  }).map((_, sourcePath))
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
        }
        }.filter { case (_, tile) => !tile.bands.forall(_.isNoDataTile) }

    ContextRDD(tiledRDD, metadata)
  }

  private def tileLayerMetadata(layout: LayoutDefinition, projectedExtent: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, cellType: CellType): TileLayerMetadata[SpaceTimeKey] = {
    val gridBounds = layout.mapTransform.extentToBounds(projectedExtent.extent)

    TileLayerMetadata(
      cellType,
      layout,
      projectedExtent.extent,
      projectedExtent.crs,
      KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
    )
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
                        bandIds: Seq[Seq[Int]] = Seq(), correlationId: String = "", experimental: Boolean=false) extends LayerProvider {

  import FileLayerProvider._

  if(experimental) {
    logger.warn("Experimental features enabled for: " + openSearchCollectionId)
  }

  private val _rootPath = if(rootPath != null) Paths.get(rootPath) else null
  //private val openSearch: OpenSearch = OpenSearch(openSearchEndpoint)

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

  def this(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitle: String, rootPath: String, maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, metadataProperties: Map[String, Any]) =
    this(openSearch, openSearchCollectionId, NonEmptyList.one(openSearchLinkTitle), rootPath, maxSpatialResolution, pathDateExtractor, metadataProperties)

  def this(openSearch: OpenSearchClient, openSearchCollectionId: String, openSearchLinkTitle: String, rootPath: String, maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor) =
    this(openSearch, openSearchCollectionId, NonEmptyList.one(openSearchLinkTitle), rootPath, maxSpatialResolution, pathDateExtractor)

  val maxZoom: Int = layoutScheme match {
    case z: ZoomedLayoutScheme => z.zoom(0, 0, maxSpatialResolution)
    case _ => 14
  }

  private val compositeRasterSource: (NonEmptyList[(RasterSource, Seq[Int])], CRS, Map[String, String]) => BandCompositeRasterSource = {
    (sources, crs, attributes) =>
      if (bandIds.isEmpty) new BandCompositeRasterSource(sources.map(_._1), crs, attributes)
      else new MultibandCompositeRasterSource(sources, crs, attributes)
  }

  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon],polygons_crs: CRS, zoom: Int, sc: SparkContext, datacubeParams : Option[DataCubeParameters]): MultibandTileLayerRDD[SpaceTimeKey] = {

    logger.info(s"Loading ${openSearchCollectionId} with params ${datacubeParams.getOrElse(new DataCubeParameters)} and bands ${openSearchLinkTitles.toList.mkString(";")}")

    var overlappingRasterSources: Seq[RasterSource] = loadRasterSourceRDD(boundingBox, from, to, zoom)
    val commonCellType = overlappingRasterSources.head.cellType
    val metadata = layerMetadata(boundingBox, from, to, zoom min maxZoom, commonCellType, layoutScheme, maxSpatialResolution)

    // Handle maskingStrategyParameters.
    var maskStrategy : Option[CloudFilterStrategy] = None
    if (datacubeParams.isDefined && datacubeParams.get.maskingStrategyParameters != null) {
      val maskParams = datacubeParams.get.maskingStrategyParameters
      val maskMethod = maskParams.getOrDefault("method", "").toString
      if (maskMethod == "mask_scl_dilation") {
        maskStrategy = for {
          (_, sclBandIndex) <- openSearchLinkTitles.zipWithIndex.find {
            case (linkTitle, _) => linkTitle.contains("SCENECLASSIFICATION") || linkTitle.contains("SCL")
          }
        } yield new SCLConvolutionFilterStrategy(sclBandIndex)
      }
      else if (maskMethod == "mask_l1c") {
        overlappingRasterSources = GDALCloudRasterSource.filterRasterSources(overlappingRasterSources, maskParams)
        maskStrategy = Some(new L1CCloudFilterStrategy(GDALCloudRasterSource.getDilationDistance(maskParams.asScala.toMap)))
      }
    }

    val rasterSources: RDD[LayoutTileSource[SpaceTimeKey]] = rasterSourceRDD(overlappingRasterSources, metadata, maxSpatialResolution, openSearchCollectionId)(sc)
    FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc, maskStrategy.getOrElse(NoCloudFilterStrategy), datacubeParams=datacubeParams)
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

  private def deriveRasterSources(feature: Feature, targetExtent:ProjectedExtent): List[(RasterSource, Seq[Int])] = {
    def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
      extent.expandBy(deltaX = (cellSize.width - extent.width) / 2, deltaY = (cellSize.height - extent.height) / 2)

    val re = RasterExtent(expandToCellSize(targetExtent.extent, maxSpatialResolution), maxSpatialResolution).alignTargetPixels
    val alignment = TargetAlignment(re)

    def vsisToHttpsCreo(path: String): String = {
      path.replace("/vsicurl/", "").replace("/vsis3/eodata", "https://finder.creodias.eu/files")
    }

    def rasterSource(dataPath:String, cloudPath:Option[(String,String)], targetCellType:Option[TargetCellType], targetExtent:ProjectedExtent, bands : Seq[Int]): Seq[RasterSource] = {
      if(dataPath.endsWith(".jp2")) {
        val warpOptions = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(maxSpatialResolution), targetCRS=Some(targetExtent.crs))
        if (cloudPath.isDefined) {
          Seq(GDALCloudRasterSource(cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = targetCellType))
        }else{
          Seq(GDALRasterSource(dataPath, options = warpOptions, targetCellType = targetCellType))
        }
      }else if(dataPath.endsWith("MTD_TL.xml")) {
        //TODO EP-3611 parse angles
        SentinelXMLMetadataRasterSource(new URL(vsisToHttpsCreo(dataPath)),bands)
      }
      else {
        if( feature.crs.isEmpty || feature.crs.get.equals(targetExtent.crs)) {
          if(experimental) {
            Seq(GDALRasterSource(dataPath, options = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(maxSpatialResolution)), targetCellType = targetCellType))
          }else{
            Seq(GeoTiffResampleRasterSource(GeoTiffPath(dataPath), alignment, NearestNeighbor, OverviewStrategy.DEFAULT, targetCellType, None))
          }
        }else{
          Seq(GeoTiffReprojectRasterSource(GeoTiffPath(dataPath), targetExtent.crs, alignment, NearestNeighbor, OverviewStrategy.DEFAULT, targetCellType = targetCellType))
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

    rasterSources.flatMap(rs_b => rs_b._1.map(rs => (rs,rs_b._2))).toList
  }

  def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int): Seq[RasterSource] = {
    require(zoom >= 0) // TODO: remove zoom and sc parameters

    val overlappingFeatures = openSearch.getProducts(
      collectionId = openSearchCollectionId,
      (from.toLocalDate, to.toLocalDate), boundingBox,
      attributeValues, correlationId, ""
    )


    val crs = bestCRS(boundingBox,layoutScheme)
    val reprojectedBoundingBox: ProjectedExtent = targetBoundingBox(boundingBox, layoutScheme)
    val overlappingRasterSources = for {
      feature <- overlappingFeatures
      rasterSources = deriveRasterSources(feature,reprojectedBoundingBox)
      if rasterSources.nonEmpty
    } yield compositeRasterSource(NonEmptyList(rasterSources.head, rasterSources.tail), crs, Predef.Map("date"->feature.nominalDate.toString))

    // TODO: these geotiffs overlap a bit so for a bbox near the edge, not one but two or even four geotiffs are taken
    //  into account; it's more efficient to filter out the redundant ones

    if (overlappingRasterSources.isEmpty) throw new IllegalArgumentException(s"Could not find data for your load_collection request with catalog ID ${openSearchCollectionId}. The catalog query had id ${correlationId} and returned ${overlappingFeatures.size} results.")

    overlappingRasterSources

  }

  private def rasterSourcesToTiles(tiledLayoutSourceRDD: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey]) = {
    val rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = tiledLayoutSourceRDD.flatMap { tiledLayoutSource =>
      tiledLayoutSource.keyedRasterRegions() map { case (key, rasterRegion) =>
        (key, (rasterRegion, tiledLayoutSource.source.name))
      }
    }

    rasterRegionsToTiles(rasterRegionRDD, metadata)
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
