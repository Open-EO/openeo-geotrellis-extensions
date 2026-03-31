package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellType, CropOptions, CroppedTile, GridBounds, GridExtent, MosaicRasterSource, MultibandTile, Raster, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent
import net.jodah.failsafe.event.ExecutionAttemptedEvent
import net.jodah.failsafe.{Failsafe, RetryPolicy}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder
import org.openeo.geotrelliscommon.ResampledTile
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.AbortedException

import java.io.IOException
import java.time.temporal.ChronoUnit.SECONDS
import java.util.Collections
import scala.collection.GenSeq
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
        logger.warn(s"load_collection: ignoring soft error for ${source.name} - ${e.getMessage}", e)
        None
      }
      case e: Exception => throw new IOException(s"load_collection: Error while reading $bounds from: ${source.name} - ${e.getMessage}", e)
    }
  }
}

class BandCompositeRasterSource(override val sources: NonEmptyList[RasterSource],
                                override val crs: CRS,
                                override val attributes: Map[String, String] = Map.empty,
                                val predefinedExtent: Option[GridExtent[Long]] = None,
                                parallelRead: Boolean = true,
                                softErrors: Boolean = false,
                                readFullTile: Boolean = false
                               ) extends MosaicRasterSource { // TODO: don't inherit?

  import BandCompositeRasterSource._

  private val maxRetries = sys.env.getOrElse("GDALREAD_MAXRETRIES", "20").toInt

  protected def reprojectedSources: NonEmptyList[RasterSource] = sources map {
    _.reproject(crs)
  }

  protected def reprojectedSources(bands: Seq[Int]): Seq[RasterSource] = {
    def reprojectRasterSourceAttemptFailed(source: RasterSource)(e: Exception): Unit =
      logger.warn(s"attempt to reproject ${source.name} to $crs failed", e)

    val selectedBands = bands.map(sources.toList)
    selectedBands flatMap { rs =>
      try Some(retryWithBackoff(maxRetries, reprojectRasterSourceAttemptFailed(rs))(rs.reproject(crs)))
      catch {

        case e: AbortedException => throw e
        case e: Exception if softErrors => {
          logger.warn(s"load_collection: ignoring soft error for ${rs.name} - ${e.getMessage}", e)
          None
        }
        case e: Exception => throw new IOException(s"load_collection: Error while reading: ${rs.name} - ${e.getMessage}", e)
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

  override def cellType: CellType = sources.map(_.cellType).reduceLeft((a, b) => OpenEOProcessScriptBuilder.cellTypeUnion(a, b))

  override def name: SourceName = sources.head.name

  override def bandCount: Int = sources.size

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

  override def readBounds(bounds: Traversable[GridBounds[Long]]): Iterator[Raster[MultibandTile]] = {
    val union = bounds.reduce(_ combine _)
    val percentageToRead = bounds.map(_.size).sum.toFloat / union.size.toFloat
    if (percentageToRead > 0.5 && readFullTile) {
      logger.debug(s"Special case - percentageToRead: $percentageToRead > 0.5, readFullTile: $readFullTile")
      readBoundsFullTile(bounds)
    } else {
      val rastersByBounds = reprojectedSources.zipWithIndex.toList.flatMap(s => {
        s._1.readBounds(bounds).zipWithIndex.map(raster_int => ((raster_int._2, (s._2, raster_int._1))))
      }).groupBy(_._1)
      rastersByBounds.toSeq.sortBy(_._1).map(_._2).map((rasters) => {
        val sortedRasters = rasters.toList.sortBy(_._2._1).map(_._2._2)
        Raster(MultibandTile(sortedRasters.map(_.tile.band(0).convert(cellType))), sortedRasters.head.extent)
      }).toIterator
    }

  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val selectedSources: scala.collection.Seq[RasterSource] = reprojectedSources(bands)

    val singleBandRasters = {
      if (parallelRead) {
        selectedSources.par
          .map {
            _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) }
          }
          .collect { case Some(raster) => raster }

      } else {
        selectedSources
          .map {
            _.read(extent, Seq(0)) map { case Raster(multibandTile, extent) => Raster(multibandTile.band(0), extent) }
          }
          .collect { case Some(raster) => raster }

      }
    }.iterator.to(Seq)

    if (singleBandRasters.size == selectedSources.size)
      Some(Raster(MultibandTile(singleBandRasters.map(_.tile.convert(cellType))), singleBandRasters.head.extent))
    else None
  }


  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val sources = reprojectedSources(bands)
    val selectedSources: IterableOnce[RasterSource] =
      if (parallelRead) {
        sources.par
      } else {
        sources
      }

    def readBoundsAttemptFailed(source: RasterSource)(e: Exception): Unit =
      logger.warn(s"attempt to read $bounds from ${source.name} failed", e)

    val singleBandRasters: Seq[Raster[Tile]] = selectedSources
      .iterator.map(rs => retryWithBackoff(maxRetries, readBoundsAttemptFailed(rs)) {
        BandCompositeRasterSource.readBounds(rs, bounds, softErrors).map(_.mapTile(_.band(0)))
      })
      .collect { case Some(raster) => raster }.toSeq

    try {
      if (singleBandRasters.isEmpty) {
        None
      } else {
        val intersection = singleBandRasters.map(_.extent).reduce((left, right) => left.intersection(right).get)
        val croppedRasters = singleBandRasters.map(_.crop(intersection))
        if (singleBandRasters.size == selectedSources.size) {
          val convertedRasters: Seq[Tile] = croppedRasters.map {
            case Raster(croppedTile: CroppedTile, extent) =>
              croppedTile.sourceTile match {
                case tile: ResampledTile => tile.cropAndConvert(croppedTile.gridBounds, cellType)
                case _ => if (croppedTile.cellType != cellType) croppedTile.convert(cellType) else croppedTile
              }
          }.toSeq
          Some(Raster(MultibandTile(convertedRasters), intersection))
        }
        else None
      }
    } catch {
      case e: Exception => throw new IOException(s"Error while reading ${bounds} from: ${sources.head.name.toString}", e)
    }
  }

  override def resample(
                         resampleTarget: ResampleTarget,
                         method: ResampleMethod,
                         strategy: OverviewStrategy
                       ): RasterSource = new BandCompositeRasterSource(
    reprojectedSources map {
      _.resample(resampleTarget, method, strategy)
    }, crs, parallelRead = parallelRead,
    softErrors = softErrors)

  override def convert(targetCellType: TargetCellType): RasterSource =
    new BandCompositeRasterSource(reprojectedSources map {
      _.convert(targetCellType)
    }, crs,
      parallelRead = parallelRead, softErrors = softErrors)

  override def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    new BandCompositeRasterSource(reprojectedSources map {
      _.reproject(targetCRS, resampleTarget, method, strategy)
    },
      crs, parallelRead = parallelRead, softErrors = softErrors)
}

