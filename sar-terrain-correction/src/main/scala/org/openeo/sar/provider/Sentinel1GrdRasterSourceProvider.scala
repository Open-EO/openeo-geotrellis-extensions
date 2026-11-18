package org.openeo.sar.provider

import cats.data.NonEmptyList
import com.github.benmanes.caffeine.cache.Caffeine
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, GridExtent, RasterSource, StringName}
import geotrellis.vector.Extent
import org.openeo.geotrellis.layers.provider.{RasterSourceDefinition, RasterSourceProvider}
import org.openeo.geotrellis.layers.raster_source.IndexedRasterSource
import org.openeo.sar._
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.metadata.Polarisation
import org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider.{PolPattern, polarisationBandIndex, polarisationOfBandName, requestedPolarisation}
import org.openeo.sar.raster.S1GrdRasterSource
import org.slf4j.LoggerFactory

import java.net.URI
import java.util.concurrent.TimeUnit

/** Recognises Sentinel-1 GRD measurement TIFF or SAFE paths and returns an
 *  [[S1GrdRasterSource]] that performs sigma0 calibration and terrain
 *  correction on-the-fly.
 *
 *  Scene-level state ([[SceneContext]]) is expensive (XML parsing, RasterSource
 *  construction).  We cache it in a Caffeine cache keyed by the SAFE root URI
 *  so that different calls for the same scene / different polarisations share
 *  the same context without redundant I/O.
 *
 *  @param processor              pre-configured [[TerrainCorrectionProcessor]].
 *                                Callers can inject any backend, DEM and geoid.
 *  @param sceneContextCacheSize  max number of scenes to keep in the cache.
 * */
class Sentinel1GrdRasterSourceProvider(
  val processor: TerrainCorrectionProcessor,
  val processingConfig: SarProcessingConfig = SarProcessingConfig.default,
  val sceneContextCacheSize: Int = 16
) extends RasterSourceProvider {

  private val logger = LoggerFactory.getLogger(getClass)

  // Cache SceneContext keyed by SAFE-root URI + CRS + CellSize to avoid
  // re-parsing annotation XML / re-opening RasterSources for every band.
  private val sceneCache = Caffeine.newBuilder()
    .maximumSize(sceneContextCacheSize)
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .build[SceneCacheKey, SceneContext]()

  // ---- RasterSourceProvider contract ----------------------------------------

  override def canProcess(definition: RasterSourceDefinition): Boolean = {
    val path = definition.dataPath
    logger.debug(s"sar_backscatter: checking if Sentinel1GrdRasterSourceProvider can process $path")
    isS1GrdMeasurementTiff(path) || isS1SafePath(path) || isS1GrdStacItem(definition)
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val (scene, safeRoot, crs, cellSize) = openScene(definition, requestedPolarisation(definition).toSeq)
    val ge = GridExtent[Long](definition.targetExtent.extent, cellSize)
    // The scene source carries all polarisations: select the one this definition refers to.
    val bandIndex = polarisationBandIndex(definition, scene.polarisations, safeRoot)

    IndexedRasterSource(new S1GrdRasterSource(scene, processor, ge, crs, StringName(safeRoot.toString)), bandIndex)
  }



  /** Serves all requested polarisation bands of one feature from a single [[S1GrdRasterSource]]: the
   *  scene (orbit/LUTs/RasterSources) is opened once and shared, and terrain correction for all
   *  polarisations is computed together in one pass, instead of once per band as `rasterSource` would. */
  override def multibandRasterSource(definitions: NonEmptyList[RasterSourceDefinition]): Option[(RasterSource, Seq[Int])] = {
    // All definitions belong to the same feature/target extent (see FileLayerProvider), so scene state
    // (and hence the polarisation ordering used to open it) can be derived from any single one of them.
    val (scene, safeRoot, crs, cellSize) = openScene(definitions.head, definitions.toList.flatMap(requestedPolarisation))
    val ge = GridExtent[Long](definitions.head.targetExtent.extent, cellSize)
    val source: RasterSource = new S1GrdRasterSource(scene, processor, ge, crs, StringName(safeRoot.toString))

    val bandIndices = definitions.toList.map { definition =>
      polarisationBandIndex(definition, scene.polarisations, safeRoot)
    }

    Some((source, bandIndices))
  }

  /** Opens (or retrieves from cache) the [[SceneContext]] backing `definition`, alongside the SAFE
   *  root/CRS/cellSize used to key it. Shared by `rasterSource` and `multibandRasterSource` so that a
   *  scene opened for one band is reused for the others (both via the cache, and, in the latter case,
   *  by construction - one open serves every requested polarisation). */
  private def openScene(definition: RasterSourceDefinition,
                        requestedPolarisations: Seq[Polarisation]): (SceneContext, URI, CRS, CellSize) = {
    val crs      = definition.targetExtent.crs
    val cellSize = definition.theResolution

    val safeRoot: URI = deriveSafeRoot(definition)
    val stacItemUrl: Option[URI] = definition.feature.selfUrl

    // Per-request `sar_backscatter` arguments (openEO process graph), if any, override the
    // provider-level default processing config.
    val effectiveProcessingConfig: SarProcessingConfig =
      definition.datacubeParams
        .flatMap(_.extraProcessingParameters)
        .collect { case sarArgs: SarBackscatterParameters => sarArgs }
        .map(_.toSarProcessingConfig(processingConfig))
        .getOrElse(processingConfig)
        .copy(softErrors = definition.softErrors)

    // All scenes of a load share the target extent: one DEM catalog search serves all of them. The scene keeps
    // the DEM items found for this extent, so the extent is part of the cache key.
    val demSearchExtent = definition.targetExtent.reproject(LatLng)
    val cacheKey = SceneCacheKey(safeRoot, crs, cellSize, effectiveProcessingConfig, demSearchExtent)

    val scene = sceneCache.get(cacheKey, (_: SceneCacheKey) => {
      val pols = derivePolarisations(definition, requestedPolarisations)
      logger.info(s"sar_backscatter: Opening $safeRoot, pols=${pols.map(_.code).mkString(",")}, crs=$crs, cellSize=$cellSize, " +
        s"coefficient=${effectiveProcessingConfig.normalization}")

      stacItemUrl match {
        case Some(url) =>
          processor.openScene(url, cellSize, crs, pols, effectiveProcessingConfig, Some(demSearchExtent))
        case None =>
          openSceneFromSafeRoot(safeRoot, pols, cellSize, crs)
      }
    })

    (scene, safeRoot, crs, cellSize)
  }

  // ---- helpers ---------------------------------------------------------------

  private val S1MeasurementPattern =
    """(?i).*/measurement/s1[a-z0-9_-]+-(vv|vh|hh|hv)-[a-z0-9_-]+\.tiff?$""".r

  private val S1GrdPattern =
    """(?i).*(S1[AB]_IW_GRD|S1[A-Z]_IW_GRDH|S1[A-Z]_EW_GRDM|s1[a-z0-9]_iw_grd[a-z]?).*\.SAFE/?$""".r

  private def isS1GrdMeasurementTiff(path: String): Boolean =
    S1MeasurementPattern.matches(path)

  private def isS1SafePath(path: String): Boolean =
    S1GrdPattern.matches(path)

  private def isS1GrdStacItem(definition: RasterSourceDefinition): Boolean = {
    val collectionId = definition.feature.collectionId
    if (collectionId == null) return false
    val cid = collectionId.toLowerCase
    val isSentinel1 = cid.contains("sentinel-1") || cid.contains("sentinel1")
    // Only raw GRD items carry calibration and noise annotations; processed products (e.g. sentinel-1-rtc) do not.
    val titles = definition.feature.links.flatMap(_.title.map(_.toLowerCase))
    isSentinel1 &&
    titles.exists(_.startsWith("schema-calibration")) &&
    titles.exists(_.startsWith("schema-noise"))
  }

  /** Derive the SAFE root URI from the measurement TIFF path:
   *  `.../S1X_IW_GRDH_..._.SAFE/measurement/s1x-....tiff`
   *  → `.../S1X_IW_GRDH_..._.SAFE` */
  private def deriveSafeRoot(definition: RasterSourceDefinition): URI = {
    val path = definition.dataPath
    val safeIdx = path.toLowerCase.indexOf(".safe")
    if (safeIdx >= 0) {
      // strip everything after .SAFE (including the .SAFE suffix itself to get the folder)
      URI.create(path.substring(0, safeIdx + 5)) // +5 = length of ".SAFE"
    } else {
      // Path is already the SAFE root or at least a stable scene identifier
      URI.create(path)
    }
  }

  /** Collect all polarisations present in the feature links: from measurement hrefs, otherwise from the
   *  links' band metadata (e.g. a SAFE-root asset), otherwise the polarisations requested by the caller. */
  private def derivePolarisations(definition: RasterSourceDefinition,
                                  requestedPolarisations: Seq[Polarisation]): Seq[Polarisation] = {
    val fromHrefs = definition.feature.links
      .flatMap(l => PolPattern.findFirstMatchIn(l.href.toString).map(_.group(1).toUpperCase))
      .distinct
    lazy val fromBandNames = definition.feature.links.toSeq
      .flatMap(_.bandNames.getOrElse(Seq.empty))
      .flatMap(polarisationOfBandName)
      .distinct

    if (fromHrefs.nonEmpty) fromHrefs.sorted.map(Polarisation.parse)
    else if (fromBandNames.nonEmpty) fromBandNames.sortBy(_.code)
    else if (requestedPolarisations.nonEmpty) requestedPolarisations.distinct.sortBy(_.code)
    else Seq(Polarisation.VV)
  }

  /** Open a scene when no STAC item URL is available.
   *  Currently requires a STAC item URL — throws otherwise. */
  private def openSceneFromSafeRoot(safeRoot: URI,
                                    pols: Seq[Polarisation],
                                    cellSize: CellSize,
                                    crs: CRS): SceneContext = {
    throw new UnsupportedOperationException(
      s"sar_backscatter: cannot open SAR scene from SAFE root without a STAC item URL. " +
      s"Ensure the OpenSearch feature has a selfUrl pointing to the STAC item. " +
      s"SAFE root: $safeRoot")
  }
}

object Sentinel1GrdRasterSourceProvider {
  private[provider] val PolPattern = """(?i)-grd-(vv|vh|hh|hv)-""".r

  private val BandNamePolPattern = """(?i)(?<![a-z])(vv|vh|hh|hv)(?![a-z])""".r

  private[provider] def polarisationOfBandName(bandName: String): Option[Polarisation] =
    Option(bandName).flatMap(BandNamePolPattern.findFirstMatchIn).map(m => Polarisation.parse(m.group(1)))

  /** The polarisation a definition refers to: from a measurement path (`-grd-vv-`), otherwise - e.g. for a
   *  SAFE-root asset - from its band metadata: the requested band name, or the asset's own name for band
   *  `bandIndex`. */
  private[provider] def requestedPolarisation(dataPath: String, bandName: String,
                                              assetBandNames: Option[Seq[String]], bandIndex: Int): Option[Polarisation] =
    PolPattern.findFirstMatchIn(dataPath).map(m => Polarisation.parse(m.group(1)))
      .orElse(polarisationOfBandName(bandName))
      .orElse(assetBandNames.flatMap(_.lift(bandIndex)).flatMap(polarisationOfBandName))

  private def requestedPolarisation(definition: RasterSourceDefinition): Option[Polarisation] =
    requestedPolarisation(definition.dataPath, definition.bandName, definition.link.bandNames, definition.bandIndex)

  /** Index of the band within a scene source (one band per polarisation, in `scenePolarisations` order)
   *  that holds the requested polarisation. Fails rather than guessing, so that a request for one
   *  polarisation can never silently be served another polarisation's data. */
  private[provider] def polarisationBandIndex(requested: Option[Polarisation], description: => String,
                                              scenePolarisations: Seq[Polarisation], safeRoot: URI): Int = {
    val pol = requested.getOrElse(throw new IllegalArgumentException(
      s"sar_backscatter: cannot determine the polarisation of $description (scene $safeRoot): neither the path nor the band metadata identify one"))
    val index = scenePolarisations.indexOf(pol)
    if (index < 0) throw new IllegalStateException(
      s"sar_backscatter: polarisation ${pol.code} of $description is not available in scene $safeRoot " +
        s"(pols=${scenePolarisations.map(_.code).mkString(",")})")
    index
  }

  private def polarisationBandIndex(definition: RasterSourceDefinition, scenePolarisations: Seq[Polarisation], safeRoot: URI): Int =
    polarisationBandIndex(requestedPolarisation(definition), s"${definition.dataPath} (band ${definition.bandName})",
      scenePolarisations, safeRoot)

  /** Convenience factory: NativeBackend with the given geoid; the DEM follows the `elevation_model` of the
   *  processing config (see [[org.openeo.sar.dem.ElevationModel]]). */
  def withGeoid(
    geoidTiffUri: Option[URI] = None,
    config: SarProcessingConfig = SarProcessingConfig.default
  ): Sentinel1GrdRasterSourceProvider = {
    val processor = new TerrainCorrectionProcessor(
      new NativeBackend,
      geoidTiffUri.map(TerrainCorrectionProcessor.geoidFromTiff)
    )
    new Sentinel1GrdRasterSourceProvider(processor, config)
  }
}

/** Default no-arg service provider loaded via [[java.util.ServiceLoader]].
 *  Uses [[NativeBackend]]; the DEM is selected per request with the `elevation_model` argument of
 *  `sar_backscatter` (e.g. `COPERNICUS_30`, or a STAC collection URL), no DEM if absent.
 *
 *  Environment variable overrides:
 *    S1_GEOID_TIFF_URI      – URI of a geoid undulation GeoTIFF (mandatory for terrain correction)
 *    S1_NORMALIZATION       – "sigma0" (default) or "gamma0rtc"
 *    S1_SHADOW_LAYOVER_MASK – "true" to add shadow/layover band */
class DefaultSentinel1GrdRasterSourceProvider
  extends Sentinel1GrdRasterSourceProvider(
    processor = {
      val geoidUri = Option(System.getenv("S1_GEOID_TIFF_URI")).map(URI.create)
      Sentinel1GrdRasterSourceProvider.withGeoid(geoidUri).processor
    },
    processingConfig = {
      val norm = Option(System.getenv("S1_NORMALIZATION")).map(_.toLowerCase) match {
        case Some("gamma0rtc") => BackscatterNormalization.Gamma0RTC
        case _                 => BackscatterNormalization.Sigma0
      }
      val shadow = Option(System.getenv("S1_SHADOW_LAYOVER_MASK")).exists(_.equalsIgnoreCase("true"))
      SarProcessingConfig(norm, shadow)
    }
  )
private final case class SceneCacheKey(safeRoot: URI, crs: CRS, cellSize: CellSize, config: SarProcessingConfig,
                                          demSearchExtent: Extent)
