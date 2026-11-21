package org.openeo.sar.raster

import com.github.benmanes.caffeine.cache.Caffeine
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleTarget, StringName, TargetCellType}
import geotrellis.vector.Extent
import org.openeo.sar.{SceneContext, SceneSpec, TerrainCorrectionProcessor}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

/** A [[RasterSource]] whose pixels are produced by SAR terrain correction
 *  (calibration + range-Doppler orthorectification).
 *
 *  Band layout is controlled by [[SarProcessingConfig]] stored in the [[SceneContext]].
 *  See [[org.openeo.sar.backend.TerrainCorrectionBackend]] for the full band index table.
 *  Default layout (sigma0, no angle bands, no shadow/layover mask):
 *    0 .. nPols-1 → backscatter (sigma0 or gamma0_RTC) per polarisation
 *    nPols        → validity mask (1.0 valid, 0.0 outside swath)
 *   [nPols+..]    → ellipsoidal incidence angle in degrees (optional, config.ellipsoidIncidenceAngle)
 *   [nPols+..]    → local terrain incidence angle in degrees (optional, config.localIncidenceAngle)
 *   [nPols+..]    → shadow/layover mask (optional, config.shadowLayoverMask; 0=ok, 1=layover, 2=shadow)
 *
 *  All expensive state (orbit, LUTs, open RasterSources) lives in the
 *  [[SceneContext]] which is built once and shared across all reads.
 *  The [[TerrainCorrectionProcessor]] is injected so that the caller can
 *  choose backend (native / ONNX), DEM and geoid sources. */
final class S1GrdRasterSource(
  val sceneSpec: SceneSpec,
  val processor: TerrainCorrectionProcessor,
  override val gridExtent: GridExtent[Long],
  override val crs: CRS,
  override val name: StringName,
  override val targetCellType: Option[TargetCellType] = None
) extends RasterSource {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  // Opened on first read, in the JVM that reads (the executor); never serialized.
  @transient private lazy val sceneContext: SceneContext = S1GrdRasterSource.scene(processor, sceneSpec)

  override def metadata: RasterMetadata = this

  override def bandCount: Int = sceneSpec.config.bandCount(sceneSpec.polarisations.size)

  override def cellType: CellType =
    targetCellType.map(_.cellType).getOrElse(FloatConstantNoDataCellType)

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty

  override def attributesForBand(band: Int): Map[String, String] = Map.empty

  // ---- reprojection / resample -----------------------------------------------

  override protected def reprojection(targetCRS: CRS,
                                      resampleTarget: ResampleTarget,
                                      method: ResampleMethod,
                                      strategy: OverviewStrategy): RasterSource = {
    val newGridExtent = resampleTarget(gridExtent.reproject(crs, targetCRS))
    new S1GrdRasterSource(
      sceneSpec.copy(crs = targetCRS, cellSize = newGridExtent.cellSize),
      processor, newGridExtent, targetCRS, name, targetCellType)
  }

  override def resample(resampleTarget: ResampleTarget,
                        method: ResampleMethod,
                        strategy: OverviewStrategy): RasterSource = {
    val newGridExtent = resampleTarget(gridExtent)
    new S1GrdRasterSource(
      sceneSpec.copy(cellSize = newGridExtent.cellSize),
      processor, newGridExtent, crs, name, targetCellType)
  }

  /**
   * Fake override of convert: STAC metadata sets GRD to uint16, but the sar_backscatter process always results in float.
   *
   */
  override def convert(targetCellType: TargetCellType): RasterSource =
    this

  // ---- reads -----------------------------------------------------------------

  /** Efficient multi-extent read: shares per-pixel geometry work via the
   *  shared [[SceneContext]] (no repeated XML parsing or RasterSource opens). */
  override def readExtents(extents: Traversable[Extent],
                           bands: Seq[Int]): Iterator[Raster[MultibandTile]] =
    processor.readExtents(sceneContext, extents, bands)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    extent.intersection(gridExtent.extent) match {
      case None =>
        logger.debug(s"sar_backscatter: requested extent $extent does not intersect extent ${gridExtent.extent} of item ${sceneSpec.stacItemUrl}")
        None
      case Some(clipped) =>
        readExtents(List(clipped), bands).nextOption()
    }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent = gridExtent.extentFor(bounds, clamp = true)
    read(extent, bands)
  }
}

object S1GrdRasterSource {
  private val logger = LoggerFactory.getLogger(getClass)

  // String keys: Scala case objects are not guaranteed to stay singletons after Kryo deserialization.
  private final case class Key(processorId: String, stacItemUrl: String, pols: Seq[String], config: String,
                               demSearchExtent: Option[Extent])

  // Executor data is grouped by product (= scene), so a handful of entries suffices; the cache only bridges
  // separately deserialized copies of a source within a task.
  private val scenes = Caffeine.newBuilder()
    .maximumSize(4)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build[Key, SceneContext]()

  private[sar] def scene(processor: TerrainCorrectionProcessor, spec: SceneSpec): SceneContext = {
    val key = Key(processor.instanceId, spec.stacItemUrl.toString, spec.polarisations.map(_.code),
      spec.config.toString, spec.demSearchExtentWgs84)
    val opened = scenes.get(key, (_: Key) => {
      logger.info(s"sar_backscatter: Opening ${spec.stacItemUrl}, pols=${spec.polarisations.map(_.code).mkString(",")}, " +
        s"crs=${spec.crs}, cellSize=${spec.cellSize}, coefficient=${spec.config.normalization}")
      processor.openScene(spec.stacItemUrl, spec.cellSize, spec.crs, spec.polarisations, spec.config, spec.demSearchExtentWgs84)
    })
    // cellSize/crs are not part of the key: they do not affect what openScene parses.
    opened.copy(cellSize = spec.cellSize, crs = spec.crs)
  }
}
