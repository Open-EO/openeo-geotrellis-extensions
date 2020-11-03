package org.openeo.geotrellis.layers

import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
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
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import org.openeo.geotrellis.layers.OscarsResponses.Feature

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

  private def fetchExtentFromOscars(oscars: Oscars, collectionId: String): ProjectedExtent = {
    val collection = oscars.getCollections()
      .find(_.id == collectionId)
      .getOrElse(throw new IllegalArgumentException(s"unknown OSCARS collection $collectionId"))

    ProjectedExtent(collection.bbox.reproject(LatLng, WebMercator), WebMercator)
  }


  private def maxSpatialResolution(): CellSize = {
    CellSize(10,10)
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

  def layerMetadata(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, cellType: CellType,layoutScheme:LayoutScheme) = {

    val worldLayout: LayoutDefinition = getLayout(layoutScheme, boundingBox, zoom)

    val crs = bestCRS(boundingBox,layoutScheme)

    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)

    val metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, from, to, cellType)
    metadata
  }

  def getLayout(layoutScheme: LayoutScheme, boundingBox: ProjectedExtent, zoom: Int) = {
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
        scheme.levelFor(layoutExtent, maxSpatialResolution())
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
      .build(new CacheLoader[(Oscars, String, Path, FileLayerProvider), Option[(ProjectedExtent, Array[ZonedDateTime])]] {
        override def load(key: (Oscars, String, Path, FileLayerProvider)): Option[(ProjectedExtent, Array[ZonedDateTime])] = {
          val (oscars, collectionId, start, provider) = key

          val bbox = fetchExtentFromOscars(oscars, collectionId)
          val dates = provider.deriveDatesFromDirectoriesOnDisk(start)

          Some(bbox, dates)
        }
      })
}

class FileLayerProvider(oscarsCollectionId: String, oscarsLinkTitles: NonEmptyList[String], rootPath: String,
                        attributeValues: Map[String, Any] = Map(), layoutScheme: LayoutScheme = ZoomedLayoutScheme(WebMercator, 256),
                        bandIds: Seq[Seq[Int]] = Seq(), probaV: Boolean = false, correlationId: String = "") extends LayerProvider {

  import FileLayerProvider._

  protected val oscars: Oscars = if (probaV) Oscars(new URL("https://oscars-dev.vgt.vito.be")) else Oscars()

  val oscarsLinkTitlesWithBandIds: Seq[(String, Seq[Int])] = oscarsLinkTitles.toList.zipAll(bandIds, "", Seq(0))

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String, metadataProperties: Map[String, Any]) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath, metadataProperties)

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath)


  private val _rootPath = Paths.get(rootPath)
  val maxZoom: Int = oscars.getCollections(correlationId)
    .find(_.id == oscarsCollectionId)
    .flatMap(_.resolution)
    .flatMap(r => layoutScheme match {
      case l: ZoomedLayoutScheme => Some(l.zoom(0, 0, CellSize(r, r)))
      case _ => None
    })
    .getOrElse(14)
  protected val compositeRasterSource: (NonEmptyList[(RasterSource, Seq[Int])], CRS, Map[String, String]) => BandCompositeRasterSource = {
    (sources, crs, attributes) =>
      if (bandIds.isEmpty) new BandCompositeRasterSource(sources.map(_._1), crs, attributes)
      else new MultibandCompositeRasterSource(sources, crs, attributes)
  }

  def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, polygons: Array[MultiPolygon],polygons_crs: CRS, zoom: Int, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val overlappingRasterSources: Seq[RasterSource] = loadRasterSourceRDD(boundingBox, from, to, zoom,sc)
    val commonCellType = overlappingRasterSources.head.cellType

    val metadata = layerMetadata(boundingBox, from, to, zoom min maxZoom, commonCellType,layoutScheme)

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


  private def loadRasterSourceRDD(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, sc:SparkContext): Seq[RasterSource] = {
    require(zoom >= 0)

    val overlappingFeatures = oscars.getProducts(
      collectionId = oscarsCollectionId,
      from.toLocalDate,
      to.toLocalDate,
      boundingBox,
      correlationId,
      attributeValues
    )

    def deriveFilePath(href: URL): String = href.getProtocol match {
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

    def deriveRasterSources(feature: Feature): List[(GeoTiffRasterSource, Seq[Int])] = {
      for {
        (title, bands) <- oscarsLinkTitlesWithBandIds.toList
        link <- feature.links.find(_.title contains title)
        path = deriveFilePath(link.href)
        targetCellType = if (link.title contains "SCENECLASSIFICATION_20M") Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0))) else None
      } yield (GeoTiffRasterSource(path, targetCellType), bands)
    }

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
    metadataCache.get((oscars, oscarsCollectionId, _rootPath, this))

  override def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent, zoom: Int = maxZoom, sc: SparkContext): TileLayerRDD[SpaceTimeKey] =
    readMultibandTileLayer(from, to, boundingBox, zoom, sc).withContext { singleBandTiles =>
      singleBandTiles.mapValues { multiBandTile => multiBandTile.band(0) }
    }

  override def readMetadata(zoom: Int, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    val Some((projectedExtent, dates)) = loadMetadata(sc)

    layerMetadata(projectedExtent,dates.head,dates.last,zoom, FloatConstantNoDataCellType,layoutScheme)
  }

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) = loadMetadata(sc).get

  override def toString: String =
    s"${getClass.getSimpleName}($oscarsCollectionId, ${oscarsLinkTitlesWithBandIds.map(_._1).toList.mkString("[", ", ", "]")}, $rootPath)"


  protected def deriveDatesFromDirectoriesOnDisk(start: Path): Array[ZonedDateTime] = {
    import java.util.stream.Stream

    import scala.compat.java8.FunctionConverters._

    val dayDirectoryDepth = if (probaV) 2 else 3
    val fullDateDirs = asJavaPredicate((path: Path) => path.getNameCount == start.getNameCount + dayDirectoryDepth)

    val subDirs = Files.walk(start, dayDirectoryDepth)
      .filter(fullDateDirs)

    val dates = {
      val toDate = if (probaV) {
        ((path: Path) => {
          val relativePath = start.relativize(path)
          ZonedDateTime.of(LocalDate.parse(relativePath.getFileName.toString, DateTimeFormatter.ofPattern("yyyyMMdd")), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
        }).asJava
      } else {
        ((path: Path) => {
          val relativePath = start.relativize(path)
          val Array(year, month, day) = relativePath.toString.split("/")
          ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
        }).asJava
      }

      // compiler needs all kinds of boilerplate for reasons I cannot comprehend
      val dates: Stream[ZonedDateTime] = subDirs
        .sorted()
        .map(toDate)

      val generator = asJavaIntFunction(new Array[ZonedDateTime](_))
      dates.toArray(generator)
    }

    dates
  }
}

/**
 * @deprecated use {@link org.openeo.geotrellis.layers.FileLayerProvider} directly
 */
@Deprecated
class Sentinel1CoherenceFileLayerProvider(oscarsCollectionId: String, oscarsLinkTitles: NonEmptyList[String], rootPath: String, attributeValues: Map[String, Any] = Map())
  extends FileLayerProvider(oscarsCollectionId, oscarsLinkTitles, rootPath, attributeValues) {

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String, metadataProperties: Map[String, Any]) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath, metadataProperties)

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath)

}

/**
 * @deprecated use {@link org.openeo.geotrellis.layers.FileLayerProvider} directly
 */
@Deprecated
class Sentinel2FileLayerProvider(oscarsCollectionId: String, oscarsLinkTitles: NonEmptyList[String], rootPath: String, attributeValues: Map[String, Any] = Map(), layoutScheme:LayoutScheme = ZoomedLayoutScheme(WebMercator, 256))
  extends FileLayerProvider(oscarsCollectionId, oscarsLinkTitles, rootPath, attributeValues, layoutScheme) {

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String, metadataProperties: Map[String, Any]) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath, metadataProperties)

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath)

}

/**
 * @deprecated use {@link org.openeo.geotrellis.layers.FileLayerProvider} directly
 */
@Deprecated
class ProbavFileLayerProvider(oscarsCollectionId: String, oscarsLinkTitles: NonEmptyList[String], rootPath: String,
                              attributeValues: Map[String, Any] = Map(), bandIds: Seq[Seq[Int]] = Seq())
  extends FileLayerProvider(oscarsCollectionId, oscarsLinkTitles, rootPath, attributeValues, bandIds = bandIds) {

  import FileLayerProvider._

  override val compositeRasterSource: (NonEmptyList[(RasterSource, Seq[Int])], CRS,  Map[String, String]) => MultibandCompositeRasterSource =
    (sources, crs, attributes) => new MultibandCompositeRasterSource(sources, crs, attributes)

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String, metadataProperties: Map[String, Any]) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath, metadataProperties)

  def this(oscarsCollectionId: String, oscarsLinkTitle: String, rootPath: String) =
    this(oscarsCollectionId, NonEmptyList.one(oscarsLinkTitle), rootPath, Map[String, Any]())

  override protected val oscars = new Oscars(new URL("https://oscars-dev.vgt.vito.be"))

  override val maxZoom: Int = oscars.getCollections()
    .find(_.id == oscarsCollectionId)
    .flatMap(_.resolution)
    .map(r => layoutScheme.zoom(0, 0, CellSize(r, r)))
    .getOrElse(9)

  override protected def deriveDatesFromDirectoriesOnDisk(start: Path): Array[ZonedDateTime] = {
    import java.util.stream.Stream

    import scala.compat.java8.FunctionConverters._

    val dayDirectoryDepth = 2
    val fullDateDirs = asJavaPredicate((path: Path) => path.getNameCount == start.getNameCount + dayDirectoryDepth)

    val subDirs = Files.walk(start, dayDirectoryDepth)
      .filter(fullDateDirs)

    val dates = {
      val toDate = ((path: Path) => {
        val relativePath = start.relativize(path)
        val dateString = relativePath.getFileName.toString
        val year = dateString.substring(0, 4).toInt
        val month = dateString.substring(4, 6).toInt
        val day = dateString.substring(6, 8).toInt
        ZonedDateTime.of(LocalDate.of(year, month, day), LocalTime.MIDNIGHT, ZoneId.of("UTC"))
      }).asJava

      // compiler needs all kinds of boilerplate for reasons I cannot comprehend
      val dates: Stream[ZonedDateTime] = subDirs
        .sorted()
        .map(toDate)

      val generator = asJavaIntFunction(new Array[ZonedDateTime](_))
      dates.toArray(generator)
    }

    dates
  }
}
