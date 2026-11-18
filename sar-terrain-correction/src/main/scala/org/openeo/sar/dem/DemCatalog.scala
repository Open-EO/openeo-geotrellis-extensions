package org.openeo.sar.dem

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.backends.STACClient
import org.slf4j.LoggerFactory

import java.net.{URI, URL}
import java.util.concurrent.TimeUnit

/** One DEM raster: its WGS84 extent and the href of the asset to read. Small and serializable, so that
 *  the (driver-side) catalog lookup can be shipped to executors, which filter it per tile. */
final case class DemItem(bboxWgs84: Extent, href: String)

/** Lists DEM rasters. [[items]] is meant to be called on the driver, once for the overall area of a request. */
trait DemCatalog extends Serializable {
  def items(searchExtentWgs84: Seq[Extent]): Seq[DemItem]
}

/** No elevation model: terrain is assumed to be at 0 m. */
object NoDemCatalog extends DemCatalog {
  override def items(searchExtentWgs84: Seq[Extent]): Seq[DemItem] = Seq.empty
}

/** A fixed list of items, regardless of the search extent. */
final case class StaticDemCatalog(allItems: Seq[DemItem]) extends DemCatalog {
  override def items(searchExtentWgs84: Seq[Extent]): Seq[DemItem] =
    allItems.filter(item => searchExtentWgs84.exists(_.intersects(item.bboxWgs84)))
}

/** DEM items of a STAC API collection, found with an item search.
 *
 *  @param endpoint     STAC API root, e.g. `https://stac.dataspace.copernicus.eu/v1`
 *  @param collectionId collection with the DEM items
 *  @param assetKey     asset of each item holding the DEM raster
 */
final class StacDemCatalog(endpoint: URL, collectionId: String, assetKey: String) extends DemCatalog {
  import StacDemCatalog._

  /** @param searchExtentWgs84 parts of the search area; each within [-180, 180] longitude (see [[DemGeometry]]) */
  override def items(searchExtentWgs84: Seq[Extent]): Seq[DemItem] =
    cache.get((endpoint.toString, collectionId, assetKey, searchExtentWgs84), _ => search(searchExtentWgs84))

  private def search(parts: Seq[Extent]): Seq[DemItem] = {
    logger.info(s"Searching DEM items of collection $collectionId at $endpoint for $parts")
    val client = new STACClient(endpoint, s3URLS = true)

    val found = parts.flatMap { part =>
      client.getProducts(collectionId, dateRange = None, bbox = ProjectedExtent(part, LatLng),
        attributeValues = Map.empty, correlationId = "", processingLevel = "")
    }

    found.map { feature =>
      val href = feature.links.find(_.title.contains(assetKey)).map(_.href.toString).getOrElse(
        throw new IllegalStateException(
          s"DEM item ${feature.id} of collection $collectionId has no asset '$assetKey'; " +
            s"available: ${feature.links.flatMap(_.title).mkString(", ")}"))
      DemItem(feature.bbox, href)
    }.distinct
  }

  override def toString: String = s"StacDemCatalog($endpoint, $collectionId, $assetKey)"
}

object StacDemCatalog {
  private val logger = LoggerFactory.getLogger(classOf[StacDemCatalog])

  // Scenes of one request share the same search extent, so the search happens once per request.
  private val cache: Cache[(String, String, String, Seq[Extent]), Seq[DemItem]] = Caffeine.newBuilder()
    .maximumSize(32)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .build()
}

/** Maps the `elevation_model` argument of `sar_backscatter` to a [[DemCatalog]]. */
object ElevationModel {
  val Copernicus30Alias = "COPERNICUS_30"
  val Copernicus30Url = "https://stac.dataspace.copernicus.eu/v1/collections/cop-dem-glo-30-dged-cog"

  private val CollectionUrl = """^(https?://.+?)/collections/([^/?#]+)/?$""".r

  /** `None`: no DEM. A STAC collection URL or the `COPERNICUS_30` alias: that collection. Otherwise: error. */
  def catalog(elevationModel: Option[String], assetKey: String): DemCatalog = elevationModel match {
    case None => NoDemCatalog
    case Some(model) =>
      val url = if (model.trim.equalsIgnoreCase(Copernicus30Alias)) Copernicus30Url else model.trim
      url match {
        case CollectionUrl(endpoint, collectionId) => new StacDemCatalog(new URI(endpoint).toURL, collectionId, assetKey)
        case _ => throw new IllegalArgumentException(
          s"Unsupported elevation_model '$model': expected a STAC collection URL " +
            s"(https://<host>/<root>/collections/<id>) or '$Copernicus30Alias'")
      }
  }
}

object DemGeometry {

  /** Buffers the box and splits it in parts within [-180, 180] longitude and [-90, 90] latitude. A box crossing
   *  the antimeridian is expressed either beyond ±180 or, as in a STAC bbox, with xmin > xmax. */
  def splitAtAntimeridian(xmin: Double, ymin: Double, xmax: Double, ymax: Double, buffer: Double = 0.0): Seq[Extent] = {
    val y0 = math.max(-90.0, ymin - buffer)
    val y1 = math.min(90.0, ymax + buffer)

    def part(x0: Double, x1: Double) = Extent(x0, y0, x1, y1)

    if (xmin > xmax) Seq(part(xmin - buffer, 180.0), part(-180.0, xmax + buffer)).map(clampLon)
    else {
      val x0 = xmin - buffer
      val x1 = xmax + buffer
      if (x1 - x0 >= 360.0) Seq(part(-180.0, 180.0))
      else if (x1 > 180.0) Seq(part(math.max(x0, -180.0), 180.0), part(-180.0, x1 - 360.0))
      else if (x0 < -180.0) Seq(part(-180.0, x1), part(x0 + 360.0, 180.0))
      else Seq(part(x0, x1))
    }
  }

  def splitAtAntimeridian(extent: Extent, buffer: Double): Seq[Extent] =
    splitAtAntimeridian(extent.xmin, extent.ymin, extent.xmax, extent.ymax, buffer)

  private def clampLon(e: Extent): Extent =
    Extent(math.max(e.xmin, -180.0), e.ymin, math.min(e.xmax, 180.0), e.ymax)
}
