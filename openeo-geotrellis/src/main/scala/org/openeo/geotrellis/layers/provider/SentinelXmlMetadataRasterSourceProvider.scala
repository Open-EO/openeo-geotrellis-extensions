package org.openeo.geotrellis.layers.provider

import geotrellis.raster.{CellSize, FloatConstantNoDataCellType, GridExtent, RasterSource}
import geotrellis.vector.ProjectedExtent
import org.openeo.geotrellis.layers.raster_source.SentinelAngleFields.Angle
import org.openeo.geotrellis.layers.raster_source.SentinelAngleRasterSource.{TileAngles, tileAngles, viewReferenceBandId}
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.DetectorMaskFormat
import org.openeo.geotrellis.layers.raster_source.{AngleOutput, IndexedRasterSource, NoDataRasterSource, SentinelAngleRasterSource, SentinelXMLMetadataRasterSource}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.core.exception.{AbortedException, SdkException}

import java.io.IOException

object SentinelXmlMetadataRasterSourceProvider extends SentinelXmlMetadataRasterSourceProvider {
  /**
   * The per-pixel outputs by band index (`granule_metadata##N`). Indices 0 to 3 are the constant tile means:
   * sun azimuth, sun zenith, and B05 viewing azimuth and zenith.
   */
  val perPixelOutputs: Map[Int, AngleOutput] = Map(
    4 -> AngleOutput(Angle.Azimuth, None),
    5 -> AngleOutput(Angle.Zenith, None),
    6 -> AngleOutput(Angle.Azimuth, Some(viewReferenceBandId)),
    7 -> AngleOutput(Angle.Zenith, Some(viewReferenceBandId)),
  )

  /** Requested names for all eight outputs, including the four tile means. */
  val metadataBandNames: Set[String] = (0 to 7).map(index => s"granule_metadata##$index").toSet
}

class SentinelXmlMetadataRasterSourceProvider extends RasterSourceProvider {

  import SentinelXmlMetadataRasterSourceProvider.perPixelOutputs

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[SentinelXmlMetadataRasterSourceProvider])

  /** Loader injections for ordinary metadata I/O failure tests, without network access or global state. */
  private[layers] def loadTileAngles(xmlPath: String): TileAngles = tileAngles(xmlPath)

  private[layers] def loadConstantAngleBand(xmlPath: String,
                                            bandIndex: Int,
                                            targetProjectedExtent: Option[ProjectedExtent],
                                            cellSize: Option[CellSize]): SentinelXMLMetadataRasterSource =
    SentinelXMLMetadataRasterSource.forAngleBand(xmlPath, bandIndex, targetProjectedExtent, cellSize)

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    definition.dataPath.endsWith("MTD_TL.xml")
  }

  /**
   * Returns the angle band `definition.bandIndex`, already selected. Returns null for a soft error, which
   * `FileLayerProvider` turns into NoData.
   */
  override def rasterSource(definition: RasterSourceDefinition): RasterSource = definition.bandIndex match {
    case index if index >= 0 && index <= 3 => constantAngleBand(definition)
    case index if perPixelOutputs.contains(index) => perPixelAngleBand(definition, perPixelOutputs(index))
    case index =>
      throw new IllegalArgumentException(s"unsupported angle band index $index for ${definition.dataPath}, expected 0 to 7")
  }

  private def constantAngleBand(definition: RasterSourceDefinition): RasterSource = {
    val targetProjectedExtent = definition.featureExtentInLayout match {
      case None => None
      case Some(featureExtentInLayoutGet) =>
        Some(ProjectedExtent(featureExtentInLayoutGet.extent, definition.targetExtent.crs))
    }

    try {
      IndexedRasterSource(
        loadConstantAngleBand(
          definition.dataPath,
          definition.bandIndex,
          targetProjectedExtent,
          Some(definition.theResolution),
        ),
        0)
    } catch {
      case e: AbortedException => throw e
      case e@(_: IOException | _: IllegalArgumentException | _: SdkException) if definition.softErrors =>
        logger.warn(s"Ignoring soft error $e", e)
        null // will be turned into a NoDataRasterSource further on
    }
  }

  /**
   * A per-pixel output only depends on its own part of the tile metadata: the sun grid, or the viewing grids and
   * detector mask of the reference band. If the metadata cannot be read or its part is missing or invalid, this is
   * a soft error if soft errors are allowed, and a failure otherwise. GML detector masks are not supported and give
   * NoData.
   */
  private def perPixelAngleBand(definition: RasterSourceDefinition, output: AngleOutput): RasterSource = {
    val path = definition.dataPath
    val tile = loadTileAnglesOrSoftError(path, definition.softErrors).getOrElse(return null)

    val (crs, gridExtent) = definition.featureExtentInLayout match {
      case Some(extentInLayout) => (definition.targetExtent.crs, GridExtent[Long](extentInLayout.extent, definition.theResolution))
      case None => (tile.metadata.crs, GridExtent[Long](tile.metadata.extent, definition.theResolution))
    }

    def unavailable(problem: String): RasterSource = {
      val message = s"${output.id} is unavailable for $path: $problem"
      if (!definition.softErrors) throw new IllegalArgumentException(message)
      logger.warn(s"Ignoring soft error: $message")
      null
    }

    def perPixelSource: RasterSource =
      IndexedRasterSource(SentinelAngleRasterSource(path, output, crs, gridExtent)(), 0)

    output.viewBandId match {
      case None => tile.metadata.sunGrid match {
        case Left(problem) => unavailable(problem)
        case Right(_) => perPixelSource
      }
      case Some(bandId) => tile.metadata.viewing(bandId) match {
        case Left(problem) => unavailable(problem)
        case Right(viewing) if viewing.detectorMask.format == DetectorMaskFormat.Gml =>
          tile.warnOnce("gml", s"$path: per-pixel viewing angles are NoData, because GML detector masks " +
            s"(processing baseline ${tile.metadata.processingBaseline.getOrElse("unknown")}) are not supported")
          IndexedRasterSource(new NoDataRasterSource(FloatConstantNoDataCellType, gridExtent, crs), 0)
        case Right(_) => perPixelSource
      }
    }
  }

  private def loadTileAnglesOrSoftError(path: String, softErrors: Boolean): Option[TileAngles] =
    try Some(loadTileAngles(path))
    catch {
      case e: AbortedException => throw e
      case e@(_: IOException | _: IllegalArgumentException | _: SdkException) if softErrors =>
        logger.warn(s"Ignoring soft error $e", e)
        None
    }
}
