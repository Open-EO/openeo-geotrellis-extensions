package org.openeo.geotrellis.layers

import java.net.URL
import java.nio.file.{Path, Paths}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit

import cats.data.NonEmptyList
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import geotrellis.layer.{TemporalKeyExtractor, ZoomedLayoutScheme, _}
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, FloatConstantNoDataCellType, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, PaddedTile, Raster, RasterMetadata, RasterRegion, RasterSource, ResampleMethod, ResampleTarget, SourceName, SourcePath, TargetCellType, UByteUserDefinedNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.proj4j.proj.TransverseMercatorProjection
import org.openeo.geotrellis.layers.OpenSearchResponses.Feature
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner

import scala.util.matching.Regex

class BandCompositeRasterSource(val sourcesList: NonEmptyList[RasterSource], override val crs: CRS, var theAttributes:Map[String,String]=Map.empty)
  extends MosaicRasterSource { // FIXME: don't inherit?

  override val sources: NonEmptyList[RasterSource] = sourcesList map { _.reprojectToGrid(crs, sourcesList.head.gridExtent) }

  override def gridExtent: GridExtent[Long] = sources.head.gridExtent

  override def attributes: Map[String, String] = theAttributes
  override def name: SourceName = sources.head.name
  override def bandCount: Int = sources.size

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val singleBandRasters = sources
      .map { _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == sources.size) Some(Raster(MultibandTile(singleBandRasters.map(_.tile)), singleBandRasters.head.extent))
    else None
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val singleBandRasters = sources
      .map { _.read(bounds, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) } }
      .collect { case Some(raster) => raster }

    if (singleBandRasters.size == sources.size) Some(Raster(MultibandTile(singleBandRasters.map(_.tile.convert(cellType))), singleBandRasters.head.extent))
    else None
  }

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = new BandCompositeRasterSource(
    sources map { _.resample(resampleTarget, method, strategy) }, crs)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new BandCompositeRasterSource(sources map { _.convert(targetCellType) }, crs)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new BandCompositeRasterSource(
      sources map { _.reproject(targetCRS, resampleTarget, method, strategy) },
      crs
    )
}

class MultibandCompositeRasterSource(val sourcesListWithBandIds: NonEmptyList[(RasterSource, Seq[Int])], override val crs: CRS, override val attributes: Map[String,String] = Map.empty)
  extends BandCompositeRasterSource(sourcesListWithBandIds.map(_._1), crs, attributes) {

  override def bandCount: Int = sourcesListWithBandIds.map(_._2.size).toList.sum

  private val sourcesWithBandIds = NonEmptyList.fromListUnsafe(sources.toList.zip(sourcesListWithBandIds.map(_._2).toList))

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
  private[geotrellis] val crs = WebMercator
  private[geotrellis] val layoutScheme = ZoomedLayoutScheme(crs, 256)

  private def extractDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) =>
      ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
  }

  private def fetchExtentFromOpenSearch(openSearch: OscarsOpenSearch, collectionId: String): ProjectedExtent = {
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

    val crs = bestCRS(boundingBox,layoutScheme)

    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)

    val metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, from, to, cellType)
    metadata
  }

  def getLayout(layoutScheme: LayoutScheme, boundingBox: ProjectedExtent, zoom: Int, maxSpatialResolution: CellSize) = {
    val LayoutLevel(_, worldLayout) = layoutScheme match {
      case scheme: ZoomedLayoutScheme => scheme.levelForZoom(zoom)
      case scheme: FloatingLayoutScheme => {
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
        scheme.levelFor(layoutExtent, maxSpatialResolution)
      }
    }
    worldLayout
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
      .build(new CacheLoader[(OscarsOpenSearch, String, Path, PathDateExtractor), Option[(ProjectedExtent, Array[ZonedDateTime])]] {
        override def load(key: (OscarsOpenSearch, String, Path, PathDateExtractor)): Option[(ProjectedExtent, Array[ZonedDateTime])] = {
          val (openSearch, collectionId, start, x) = key

          val bbox = fetchExtentFromOpenSearch(openSearch, collectionId)
          val dates = x.extractDates(start)

          Some(bbox, dates)
        }
      })
}

class FileLayerProvider(openSearchEndpoint: URL, openSearchCollectionId: String, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
                        maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
                        bandIds: Seq[Seq[Int]] = Seq(), correlationId: String = "") extends LayerProvider {

  import FileLayerProvider._

  private val _rootPath = Paths.get(rootPath)
  private val openSearch: OscarsOpenSearch = OscarsOpenSearch(openSearchEndpoint)

  val openSearchLinkTitlesWithBandIds: Seq[(String, Seq[Int])] = openSearchLinkTitles.toList.zipAll(bandIds, "", Seq(0))

  def this(openSearchEndpoint: URL, openSearchCollectionId: String, openSearchLinkTitle: String, rootPath: String, maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor, metadataProperties: Map[String, Any]) =
    this(openSearchEndpoint, openSearchCollectionId, NonEmptyList.one(openSearchLinkTitle), rootPath, maxSpatialResolution, pathDateExtractor, metadataProperties)

  def this(openSearchEndpoint: URL, openSearchCollectionId: String, openSearchLinkTitle: String, rootPath: String, maxSpatialResolution: CellSize, pathDateExtractor: PathDateExtractor) =
    this(openSearchEndpoint, openSearchCollectionId, NonEmptyList.one(openSearchLinkTitle), rootPath, maxSpatialResolution, pathDateExtractor)

  val maxZoom: Int = layoutScheme match {
    case z: ZoomedLayoutScheme => z.zoom(0, 0, maxSpatialResolution)
    case _ => 14
  }

  private val compositeRasterSource: (NonEmptyList[(RasterSource, Seq[Int])], CRS, Map[String, String]) => BandCompositeRasterSource = {
    (sources, crs, attributes) =>
      if (bandIds.isEmpty) new BandCompositeRasterSource(sources.map(_._1), crs, attributes)
      else new MultibandCompositeRasterSource(sources, crs, attributes)
  }

  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon],polygons_crs: CRS, zoom: Int, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val overlappingRasterSources: Seq[RasterSource] = loadRasterSourceRDD(boundingBox, from, to, zoom,sc)
    val commonCellType = overlappingRasterSources.head.cellType

    val metadata = layerMetadata(boundingBox, from, to, zoom min maxZoom, commonCellType, layoutScheme, maxSpatialResolution)

    val requiredKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons).map{_.reproject(polygons_crs,metadata.crs)}.clipToGrid(metadata.layout).groupByKey()

    val rasterSources: RDD[LayoutTileSource[SpaceTimeKey]] = this.rasterSourceRDD(overlappingRasterSources, metadata)(sc)

    val rasterRegionRDD = rasterSources.flatMap { tiledLayoutSource =>
      tiledLayoutSource.keyedRasterRegions() map { case (key, rasterRegion) =>
        (key, (rasterRegion, tiledLayoutSource.source.name))
      }
    }.map{tuple => (tuple._1.spatialKey,tuple)}
    // FIXME: doesn't this equal an inner join?
    val filteredRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = rasterRegionRDD.rightOuterJoin(requiredKeys).flatMap { t=> t._2._1.toList}

    rasterRegionsToTiles(filteredRDD,metadata)
  }

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, zoom: Int = maxZoom, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {

    this.readMultibandTileLayer(from,to,boundingBox,Array(MultiPolygon(boundingBox.extent.toPolygon())),boundingBox.crs,zoom,sc)
  }


  private def deriveFilePath(href: URL): String = href.getProtocol match {
    // as oscars requests now use accessedFrom=MEP, we will normally always get file paths
    case "file" => // e.g. file:/data/MTDA_DEV/CGS_S2_DEV/FAPAR_V2/2020/03/19/S2A_20200319T032531_48SXD_FAPAR_V200/10M/S2A_20200319T032531_48SXD_FAPAR_10M_V200.tif
      href.getPath.replaceFirst("CGS_S2_DEV", "CGS_S2") // temporary workaround?
    case "https" => // e.g. https://oscars-dev.vgt.vito.be/download/FAPAR_V2/2020/03/20/S2B_20200320T102639_33VVF_FAPAR_V200/10M/S2B_20200320T102639_33VVF_FAPAR_10M_V200.tif
      val subPath = href.getPath
        .split("/")
        .drop(4) // the empty string at the front too
        .mkString("/")

      (_rootPath resolve subPath).toString
  }

  private def deriveRasterSources(feature: Feature): List[(RasterSource, Seq[Int])] = {
    for {
      (title, bands) <- openSearchLinkTitlesWithBandIds.toList
      link <- feature.links.find(_.title contains title)
      path = deriveFilePath(link.href)
      targetCellType = if (link.title contains "SCENECLASSIFICATION_20M") Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0))) else None
    } yield (GeoTiffRasterSource(path, targetCellType), bands)
  }

  private def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc:SparkContext): Seq[RasterSource] = {
    require(zoom >= 0)

    val overlappingFeatures = openSearch.getProducts(
      collectionId = openSearchCollectionId,
      from.toLocalDate,
      to.toLocalDate,
      boundingBox,
      correlationId,
      attributeValues
    )


    val crs = bestCRS(boundingBox,layoutScheme)
    val overlappingRasterSources = for {
      feature <- overlappingFeatures
      rasterSources = deriveRasterSources(feature)
      if rasterSources.nonEmpty
    } yield compositeRasterSource(NonEmptyList(rasterSources.head, rasterSources.tail), crs, Predef.Map("date"->feature.nominalDate.toString))

    // TODO: these geotiffs overlap a bit so for a bbox near the edge, not one but two or even four geotiffs are taken
    //  into account; it's more efficient to filter out the redundant ones

    if (overlappingRasterSources.isEmpty) throw new IllegalArgumentException("no fitting raster sources found")

    overlappingRasterSources

  }

  private def rasterSourceRDD(rasterSources: Seq[RasterSource], metadata: TileLayerMetadata[SpaceTimeKey])(implicit sc: SparkContext): RDD[LayoutTileSource[SpaceTimeKey]] = {

    assert(rasterSources.map(_.crs).toSet == Set(metadata.crs))

    val keyExtractor = new TemporalKeyExtractor {
      def getMetadata(rs: RasterMetadata): ZonedDateTime = ZonedDateTime.parse(rs.attributes("date")).truncatedTo(ChronoUnit.DAYS)
    }
    val sources = sc.parallelize(rasterSources,rasterSources.size)

    val tiledLayoutSourceRDD =
      sources.map { rs =>
        val m = keyExtractor.getMetadata(rs)
        val tileKeyTransform: SpatialKey => SpaceTimeKey = { sk => keyExtractor.getKey(m, sk) }
        rs.tileToLayout(metadata.layout, tileKeyTransform)
      }

    tiledLayoutSourceRDD
  }

  private def rasterSourcesToTiles(tiledLayoutSourceRDD: RDD[LayoutTileSource[SpaceTimeKey]], metadata: TileLayerMetadata[SpaceTimeKey]) = {
    val rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))] = tiledLayoutSourceRDD.flatMap { tiledLayoutSource =>
      tiledLayoutSource.keyedRasterRegions() map { case (key, rasterRegion) =>
        (key, (rasterRegion, tiledLayoutSource.source.name))
      }
    }

    rasterRegionsToTiles(rasterRegionRDD, metadata)
  }

  private def rasterRegionsToTiles(rasterRegionRDD: RDD[(SpaceTimeKey, (RasterRegion, SourceName))], metadata: TileLayerMetadata[SpaceTimeKey]) = {
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      rasterRegionRDD
        .groupByKey(SpacePartitioner(metadata.bounds))
        .mapValues { namedRasterRegions =>
          namedRasterRegions.toSeq
            .flatMap { case (rasterRegion, sourcePath: SourcePath) =>
              rasterRegion.raster.map(r => (r.tile, sourcePath))
            }
            .sortWith { case ((leftMultibandTile, leftSourcePath), (rightMultibandTile, rightSourcePath)) =>
              if (leftMultibandTile.band(0).isInstanceOf[PaddedTile] && !rightMultibandTile.band(0).isInstanceOf[PaddedTile]) true
              else if (!leftMultibandTile.band(0).isInstanceOf[PaddedTile] && rightMultibandTile.band(0).isInstanceOf[PaddedTile]) false
              else leftSourcePath.value < rightSourcePath.value
            }
            .map { case (multibandTile, _) => multibandTile }
            .reduce(_ merge _)
        }

      ContextRDD(tiledRDD, metadata)
  }



  override def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])] =
    metadataCache.get((openSearch, openSearchCollectionId, _rootPath, pathDateExtractor))

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
