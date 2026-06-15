package org.openeo.sar.provider

import com.github.benmanes.caffeine.cache.Caffeine
import geotrellis.proj4.CRS
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.{CellSize, GridExtent, RasterSource, StringName}
import geotrellis.vector.Extent
import org.openeo.geotrellis.layers.provider.{RasterSourceDefinition, RasterSourceProvider}
import org.openeo.sar.backend.nativ.NativeBackend
import org.openeo.sar.metadata.Polarisation
import org.openeo.sar.raster.S1GrdRasterSource
import org.openeo.sar.{BackscatterNormalization, SarProcessingConfig, SceneContext, TerrainCorrectionProcessor}
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
    isS1GrdMeasurementTiff(path) || isS1SafePath(path) || isS1GrdStacItem(definition)
  }

  override def rasterSource(definition: RasterSourceDefinition): RasterSource = {
    val crs      = definition.targetExtent.crs
    val cellSize = definition.theResolution

    val safeRoot: URI = deriveSafeRoot(definition)
    val stacItemUrl: Option[URI] = definition.feature.selfUrl

    val cacheKey = SceneCacheKey(safeRoot, crs, cellSize, processingConfig)

    val scene = sceneCache.get(cacheKey, (_: SceneCacheKey) => {
      val pols = derivePolarisations(definition)
      logger.info(s"Opening SAR scene $safeRoot, pols=${pols.map(_.code).mkString(",")}, crs=$crs, cellSize=$cellSize")

      stacItemUrl match {
        case Some(url) =>
          processor.openScene(url, cellSize, crs, pols, processingConfig)
        case None =>
          openSceneFromSafeRoot(safeRoot, pols, cellSize, crs)
      }
    })

    val ge = GridExtent[Long](definition.targetExtent.extent, cellSize)

    new S1GrdRasterSource(scene, processor, ge, crs, StringName(safeRoot.toString))
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
    val cid = definition.feature.collectionId.toLowerCase
    (cid.contains("sentinel-1") || cid.contains("sentinel1")) &&
    (cid.contains("grd") || definition.dataPath.toLowerCase.contains("grd"))
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

  private val PolPattern = """(?i)-grd-(vv|vh|hh|hv)-""".r

  /** Collect all polarisations present in the feature links. */
  private def derivePolarisations(definition: RasterSourceDefinition): Seq[Polarisation] = {
    val pols = definition.feature.links
      .flatMap(l => PolPattern.findFirstMatchIn(l.href.toString).map(_.group(1).toUpperCase))
      .distinct

    if (pols.isEmpty) {
      // Fall back to the polarisation in the current link
      PolPattern.findFirstMatchIn(definition.dataPath)
        .map(m => Seq(Polarisation.parse(m.group(1).toUpperCase)))
        .getOrElse(Seq(Polarisation.VV))
    } else {
      pols.sorted.map(Polarisation.parse)
    }
  }

  /** Open a scene when no STAC item URL is available.
   *  Currently requires a STAC item URL — throws otherwise. */
  private def openSceneFromSafeRoot(safeRoot: URI,
                                    pols: Seq[Polarisation],
                                    cellSize: CellSize,
                                    crs: CRS): SceneContext = {
    throw new UnsupportedOperationException(
      s"Cannot open SAR scene from SAFE root without a STAC item URL. " +
      s"Ensure the OpenSearch feature has a selfUrl pointing to the STAC item. " +
      s"SAFE root: $safeRoot")
  }
}

object Sentinel1GrdRasterSourceProvider {
  /** Convenience factory: NativeBackend + Copernicus DEM (COG via HTTPS). */
  def withCopernicusDem(
    geoidTiffUri: Option[URI] = None,
    config: SarProcessingConfig = SarProcessingConfig.default
  ): Sentinel1GrdRasterSourceProvider = {
    val demFactory: Extent => RasterSource = extent => {
      val lon0 = math.floor(extent.xmin).toInt
      val lat0 = math.floor(extent.ymin).toInt
      val lon1 = math.ceil(extent.xmax).toInt
      val lat1 = math.ceil(extent.ymax).toInt
      val urls = for {
        lat <- lat0 until lat1
        lon <- lon0 until lon1
        latStr = f"${if (lat >= 0) "N" else "S"}${math.abs(lat)}%02d"
        lonStr = f"${if (lon >= 0) "E" else "W"}${math.abs(lon)}%03d"
        url = s"s3://eodata/auxdata/CopDEM_COG/copernicus-dem-30m/Copernicus_DSM_COG_10_${latStr}_00_${lonStr}_00_DEM/Copernicus_DSM_COG_10_${latStr}_00_${lonStr}_00_DEM.tif"
      } yield url
      urls.map(GeoTiffRasterSource(_)).head
    }

    val processor = geoidTiffUri match {
      case Some(g) => TerrainCorrectionProcessor.withDemAndGeoid(new NativeBackend, demFactory, g)
      case None    => new TerrainCorrectionProcessor(new NativeBackend, demFactory)
    }
    new Sentinel1GrdRasterSourceProvider(processor, config)
  }
}

/** Default no-arg service provider loaded via [[java.util.ServiceLoader]].
 *  Uses [[NativeBackend]] and Copernicus 30 m DEM tiles from CDSE S3.
 *
 *  Environment variable overrides:
 *    S1_GEOID_TIFF_URI      – URI of a geoid undulation GeoTIFF (optional)
 *    S1_NORMALIZATION       – "sigma0" (default) or "gamma0rtc"
 *    S1_SHADOW_LAYOVER_MASK – "true" to add shadow/layover band */
class DefaultSentinel1GrdRasterSourceProvider
  extends Sentinel1GrdRasterSourceProvider(
    processor = {
      val geoidUri = Option(System.getenv("S1_GEOID_TIFF_URI")).map(URI.create)
      Sentinel1GrdRasterSourceProvider.withCopernicusDem(geoidUri).processor
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
private final case class SceneCacheKey(safeRoot: URI, crs: CRS, cellSize: CellSize, config: SarProcessingConfig)
