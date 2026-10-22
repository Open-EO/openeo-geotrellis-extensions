package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellType, CropOptions, CroppedTile, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, Raster, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent
import net.jodah.failsafe.event.ExecutionAttemptedEvent
import net.jodah.failsafe.{Failsafe, RetryPolicy}
import org.openeo.geotrellis.GeneralUtils.{cellTypeUnionWithNoData, safeConvert}
import org.openeo.geotrellis.RequestContext
import org.openeo.geotrellis.layers.RasterTileLoader.SOFT_ERROR_MEGAPIXEL_COUNTER
import org.openeo.geotrellis.layers.raster_source.BandSelectionRasterSource
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, ResampledTile}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.AbortedException

import java.io.IOException
import java.time.temporal.ChronoUnit.SECONDS
import java.util.Collections
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._

// TODO: are these attributes typically propagated as RasterSources are transformed? Maybe we should find another way to
//  attach e.g. a date to a RasterSource.
object BandCompositeRasterSource {
  private val logger = LoggerFactory.getLogger(classOf[BandCompositeRasterSource])

  private def retryWithBackoff[R](maxAttempts: Int = 20, onAttemptFailed: Exception => Unit = _ => ())(f: => R): R = {
    val retryPolicy = new RetryPolicy[R]
      .handle(classOf[Exception]) // will otherwise retry Error
      .withMaxAttempts(maxAttempts)
      .withBackoff(1, 16, SECONDS)
      .onFailedAttempt((attempt: ExecutionAttemptedEvent[R]) =>
        onAttemptFailed(attempt.getLastFailure.asInstanceOf[Exception]))

    Failsafe
      .`with`(Collections.singletonList(retryPolicy))
      .get(f _)
  }

  def readBounds(source: RasterSource, bounds: GridBounds[Long], softErrors: Boolean, bands: Seq[Int] = Seq(0)): Option[Raster[MultibandTile]] = {
    try {
      logger.debug(s"reading $bounds from ${source.name}")
      val raster = source.read(bounds, bands)
      logger.debug(s"finished reading $bounds from ${source.name}")
      raster
    } catch {
      case e: AbortedException => throw e
      case e: Exception if softErrors => {

        try{
          val tracker = BatchJobMetadataTracker.tracker("")
          tracker.add(SOFT_ERROR_MEGAPIXEL_COUNTER, bounds.size*bands.length / (1024 * 1024) )
        }catch {
          case e: Exception => logger.warn(s"load_collection: failed to increment soft error counter for ${source.name} - ${e.getMessage}", e)
        }

        logger.warn(s"load_collection: ignoring soft error for ${source.name} - ${e.getMessage}", e)
        None
      }
      case e: Exception => throw new IOException(s"load_collection: Error while reading $bounds from: ${source.name} - ${e.getMessage}", e)
    }
  }
}

/**
 * A multi-band [[RasterSource]] whose output bands are the concatenation of the bands contributed by `sources`:
 * a [[BandSelectionRasterSource]] contributes its selected bands, any other source exactly one (its band 0).
 * FileLayerProvider builds one of these per feature/date, in exactly the band order requested by the user, with
 * missing bands filled in as `NoDataRasterSource` placeholders. Consecutive output bands served by the same physical
 * multi-band dataset are represented by a single [[BandSelectionRasterSource]], so they are transformed and read
 * together, in one call.
 *
 * `groupedBySource` lets RasterTileLoader regroup the bands by physical dataset, to avoid re-opening/re-reading the
 * same dataset once per band.
 *
 * All bands are expected to share the same pixel grid: FileLayerProvider constructs every band of a feature against
 * the same target extent/resolution/CRS (see RasterSourceDefinition), so a single GridBounds/RasterRegion can be
 * shared across all underlying sources.
 */
class BandCompositeRasterSource(override val sources: NonEmptyList[RasterSource],
                                override val crs: CRS,
                                override val attributes: Map[String, String] = Map.empty,
                                val predefinedExtent: Option[GridExtent[Long]] = None,
                                val parallelRead: Boolean = true,
                                val softErrors: Boolean = false,
                                val readFullTile: Boolean = false
                               ) extends MosaicRasterSource { // TODO: don't inherit?

  import BandCompositeRasterSource._

  private val maxRetries = sys.env.getOrElse("GDALREAD_MAXRETRIES", "20").toInt
  private val requestContext = RequestContext.get

  protected def reprojectedSources: NonEmptyList[RasterSource] = sources map {
    _.reproject(crs)
  }

  // (position in `sources`, band within that source) for every output band; never opens the sources
  @transient private lazy val bandLayout: IndexedSeq[(Int, Int)] =
    sources.toList.zipWithIndex.flatMap { case (source, sourceIndex) =>
      val contributedBands = source match {
        case selection: BandSelectionRasterSource => selection.bandCount
        case _ => 1
      }
      (0 until contributedBands).map(band => (sourceIndex, band))
    }.toIndexedSeq

  /**
   * Groups the requested output `bands` per source, in order of first appearance: each entry holds the
   * position of the source in `sources`, the bands to read from it, and their positions in the result.
   */
  private def readPlan(bands: Seq[Int]): Seq[(Int, Seq[Int], Seq[Int])] = {
    val plan = mutable.LinkedHashMap[Int, (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[Int])]()
    bands.zipWithIndex.foreach { case (band, resultPosition) =>
      val (sourceIndex, bandInSource) = bandLayout(band)
      val (bandsInSource, resultPositions) = plan.getOrElseUpdate(sourceIndex, (mutable.ArrayBuffer(), mutable.ArrayBuffer()))
      bandsInSource += bandInSource
      resultPositions += resultPosition
    }
    plan.toSeq.map { case (sourceIndex, (bandsInSource, resultPositions)) => (sourceIndex, bandsInSource.toSeq, resultPositions.toSeq) }
  }

  /** Reprojects `rs` to this composite's CRS, retrying and honouring `softErrors` (`None` if it failed softly). */
  private def reprojectedSource(rs: RasterSource, bandCount: Int): Option[RasterSource] = {
    try Some(retryWithBackoff(maxRetries, (e: Exception) => logger.warn(s"attempt to reproject ${rs.name} to $crs failed", e)) {
      rs.reproject(crs)
    })
    catch {
      case e: AbortedException => throw e
      case e: Exception if softErrors =>
        //without a predefined extent, we don't know the size of the raster yet: count some value so that the soft-errors counter is incremented
        val megapixelsFailed = predefinedExtent.map(_.size * bandCount / (1024 * 1024)).getOrElse(bandCount.toLong)
        BatchJobMetadataTracker.tracker("").add(SOFT_ERROR_MEGAPIXEL_COUNTER, megapixelsFailed)
        logger.warn(s"load_collection: ignoring soft error for ${rs.name} - ${e.getMessage}", e)
        None
      case e: Exception => throw new IOException(s"load_collection: Error while reading: ${rs.name} - ${e.getMessage}", e)
    }
  }

  /**
   * Reads the requested output `bands` with one `read` call per underlying source, returning one single-band
   * raster per requested band, in the requested order, or `None` if any of them could not be read.
   */
  private def readPerSource(bands: Seq[Int], description: => String)
                           (read: (RasterSource, Seq[Int]) => Option[Raster[MultibandTile]]): Option[Seq[Raster[Tile]]] = {
    val plan = readPlan(bands)
    val sourceList = sources.toList
    def readSource(entry: (Int, Seq[Int], Seq[Int])): Option[Seq[(Int, Raster[Tile])]] = {
      val (sourceIndex, bandsInSource, resultPositions) = entry
      reprojectedSource(sourceList(sourceIndex), bandsInSource.size)
        .flatMap(rs => read(rs, bandsInSource))
        .map(raster => resultPositions.zip(raster.tile.bands.map(Raster(_, raster.extent))))
    }
    val readsBySource = if (parallelRead) plan.par.map(readSource).seq else plan.map(readSource)

    if (readsBySource.forall(_.isDefined)) {
      Some(readsBySource.flatMap(_.get).sortBy(_._1).map(_._2))
    } else {
      if (softErrors && readsBySource.forall(_.isEmpty))
        logger.error(s"load_collection: soft errors left zero readable tiles for $description from $name, returning empty result")
      None
    }
  }

  override def gridExtent: GridExtent[Long] = predefinedExtent.getOrElse {
    try {
      sources.head.gridExtent
    } catch {
      case e: Exception => throw new IOException(s"Error while reading extent of: ${sources.head.name.toString}", e)
    }
  }

  override def cellType: CellType = sources.map(_.cellType).reduceLeft((a, b) => cellTypeUnionWithNoData(a, b))

  override def name: SourceName = sources.head.name

  override def bandCount: Int = bandLayout.size

  def readBoundsFullTile(bounds: Traversable[GridBounds[Long]]): Iterator[Raster[MultibandTile]] = {
    var union = bounds.reduce(_ combine _)

    // rastersource contract: do not read negative gridbounds
    union = union.copy(colMin = math.max(union.colMin, 0), rowMin = math.max(union.rowMin, 0))

    val maybeRaster = read(union)
    if (maybeRaster.isEmpty) {
      return Iterator.empty
    }
    val fullRaster = maybeRaster.get
    val mappedBounds = bounds.map(b => b.offset(-union.colMin, -union.rowMin).toGridType[Int])
    return mappedBounds.map(b => fullRaster.crop(b, CropOptions(force = true, clamp = true))).toIterator

  }

  override def readBounds(bounds: Traversable[GridBounds[Long]]): Iterator[Raster[MultibandTile]] = requestContext.apply {
    val union = bounds.reduce(_ combine _)
    val percentageToRead = bounds.map(_.size).sum.toFloat / union.size.toFloat
    if (percentageToRead > 0.5 && readFullTile) {
      logger.debug(s"Special case - percentageToRead: $percentageToRead > 0.5, readFullTile: $readFullTile")
      readBoundsFullTile(bounds)
    } else {
      val sourceList = reprojectedSources.toList
      val rastersByBounds = readPlan(0 until bandCount).flatMap { case (sourceIndex, bandsInSource, resultPositions) =>
        sourceList(sourceIndex).readBounds(bounds, bandsInSource).zipWithIndex.flatMap { case (raster, boundsIndex) =>
          resultPositions.zip(raster.tile.bands).map { case (resultPosition, tile) => (boundsIndex, (resultPosition, Raster(tile, raster.extent))) }
        }
      }.groupBy(_._1)
      rastersByBounds.toSeq.sortBy(_._1).map(_._2).map { rasters =>
        val sortedRasters = rasters.map(_._2).sortBy(_._1).map(_._2)
        Raster(MultibandTile(sortedRasters.map(raster => safeConvert(raster.tile, cellType))), sortedRasters.head.extent)
      }.iterator
    }

  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] =
    readPerSource(bands, extent.toString)(_.read(extent, _)).map { singleBandRasters =>
      Raster(MultibandTile(singleBandRasters.map(raster => safeConvert(raster.tile, cellType))), singleBandRasters.head.extent)
    }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = requestContext.apply {
    def readBoundsAttemptFailed(source: RasterSource)(e: Exception): Unit =
      logger.warn(s"attempt to read $bounds from ${source.name} failed", e)

    val singleBandRasters = readPerSource(bands, bounds.toString) { (rs, bandsInSource) =>
      requestContext.apply {
        retryWithBackoff(maxRetries, readBoundsAttemptFailed(rs)) {
          BandCompositeRasterSource.readBounds(rs, bounds, softErrors, bandsInSource)
        }
      }.map { raster =>
        if (raster.tile.bands.forall(_.cellType == rs.cellType)) raster
        else {
          logger.debug(s"converting tile from ${raster.tile.cellType} to ${rs.cellType}")
          raster.mapTile(_.mapBands((_, band) => safeConvert(band, rs.cellType)))
        }
      }
    }

    try {
      singleBandRasters.map { rasters =>
        val intersection = rasters.map(_.extent).reduce((left, right) => left.intersection(right).get)
        val convertedRasters: Seq[Tile] = rasters.map(_.crop(intersection)).map {
          case Raster(croppedTile: CroppedTile, _) =>
            croppedTile.sourceTile match {
              case tile: ResampledTile => tile.cropAndConvert(croppedTile.gridBounds, cellType)
              case _ => if (croppedTile.cellType != cellType) croppedTile.convert(cellType) else croppedTile
            }
        }
        Raster(MultibandTile(convertedRasters), intersection)
      }
    } catch {
      case e: Exception => throw new IOException(s"Error while reading ${bounds} from: ${name.toString}", e)
    }
  }

  /**
   * Groups the sources by their underlying physical datasource name, preserving the order in which datasources are
   * first encountered. Each group carries the true output band indices (positions in the resulting MultibandTile)
   * of the bands its sources contribute, in order, alongside those sources.
   */
  def groupedBySource: Seq[(SourceName, Seq[Int], NonEmptyList[RasterSource])] = {
    val outputBandsBySource = bandLayout.zipWithIndex.groupBy(_._1._1).map { case (sourceIndex, bands) => sourceIndex -> bands.map(_._2) }
    val order = mutable.LinkedHashMap[SourceName, (mutable.ArrayBuffer[Int], mutable.ArrayBuffer[RasterSource])]()
    sources.toList.zipWithIndex.foreach { case (source, sourceIndex) =>
      val (outputBands, groupSources) = order.getOrElseUpdate(source.name, (mutable.ArrayBuffer(), mutable.ArrayBuffer()))
      outputBands ++= outputBandsBySource(sourceIndex)
      groupSources += source
    }
    order.toSeq.map { case (sourceName, (outputBands, groupSources)) =>
      (sourceName, outputBands.toSeq, NonEmptyList.fromListUnsafe(groupSources.toList))
    }
  }

  // These preserve all settings (attributes, predefinedExtent, readFullTile, ...): e.g. RasterSource.tileToLayout
  // goes through them, and RasterTileLoader relies on the result still being a fully configured composite.
  private def withSources(newSources: NonEmptyList[RasterSource],
                          newCrs: CRS = crs,
                          newPredefinedExtent: Option[GridExtent[Long]] = predefinedExtent): BandCompositeRasterSource =
    new BandCompositeRasterSource(newSources, newCrs, attributes, newPredefinedExtent,
      parallelRead = parallelRead, softErrors = softErrors, readFullTile = readFullTile)

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = withSources(
    reprojectedSources map {
      _.resample(resampleTarget, method, strategy)
    },
    newPredefinedExtent = predefinedExtent.map(resampleTarget(_))
  )

  override def convert(targetCellType: TargetCellType): RasterSource =
    withSources(sources map {
      _.convert(targetCellType)
    })

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    withSources(
      reprojectedSources map {
        _.reproject(targetCRS, resampleTarget, method, strategy)
      },
      newCrs = targetCRS,
      newPredefinedExtent = predefinedExtent.map(extent => resampleTarget(extent.reproject(crs, targetCRS)))
    )

  override def toString: String = s"BandCompositeRasterSource(${sources.toList}, $crs, $gridExtent, $name)"

  /*
   * Value-based equality: instances are used as (shuffle) keys, and the default identity-based hashCode differs for
   * every deserialized copy, so recomputed map tasks (e.g. after executor loss) would route records to different
   * reduce partitions than the original attempt, silently dropping data.
   */
  protected def equalityState: Seq[Any] =
    Seq(getClass, sources.toList, crs, attributes, predefinedExtent, parallelRead, softErrors, readFullTile)

  override def equals(other: Any): Boolean = other match {
    case that: BandCompositeRasterSource => (this eq that) || this.equalityState == that.equalityState
    case _ => false
  }

  // Only uses values with a stable (non-identity) hashCode, consistent with equals.
  override def hashCode(): Int = (getClass.getName, sources.toList.map(_.name), crs).hashCode()

}

