package org.openeo.geotrellis.stac

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.vector.Extent
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.jdk.CollectionConverters._

/**
 * A STAC 1.1 item, as written next to the data.
 *
 * @param datetime      explicit `properties.datetime`; if null it is derived from `startDatetime`/`endDatetime`
 * @param bbox          in `crs`
 * @param startDatetime start of the temporal extent (ISO instant), can be null
 * @param endDatetime   end of the temporal extent (ISO instant), can be null
 */
case class Item(id: String, datetime: String, bbox: Extent, assets: util.Map[String, Asset], crs: CRS, // TODO: order assets by key?
                startDatetime: String = null, endDatetime: String = null) {

  Item.warnIfIncompleteTemporalExtent(this)

  def bbox4326: Extent = if (crs == LatLng) bbox else bbox.reproject(crs, LatLng)

  private def hasTemporalRange: Boolean =
    startDatetime != null && endDatetime != null && startDatetime != endDatetime

  /** Null if there is no date, or if the dates are described by `start_datetime`/`end_datetime`. */
  def effectiveDatetime: String = {
    if (datetime != null) datetime
    else if (hasTemporalRange) null
    else if (startDatetime != null) startDatetime
    else endDatetime
  }

  def properties: util.Map[String, Any] = {
    val result = new util.LinkedHashMap[String, Any]()
    if (hasTemporalRange) {
      result.put("datetime", effectiveDatetime)
      result.put("start_datetime", startDatetime)
      result.put("end_datetime", endDatetime)
    } else {
      Option(effectiveDatetime).foreach(result.put("datetime", _))
    }
    result
  }

  def geometry: util.Map[String, Any] = {
    // Reproject the corners rather than taking the envelope: in EPSG:4326 the rectangle is a slightly rotated quadrilateral.
    val toLatLng = Transform(crs, LatLng)
    // counter-clockwise exterior ring, as RFC 7946 (GeoJSON) recommends
    val corners = Seq((bbox.xmin, bbox.ymin), (bbox.xmax, bbox.ymin), (bbox.xmax, bbox.ymax), (bbox.xmin, bbox.ymax))
      .map { case (x, y) => toLatLng(x, y) }
    val ring = corners :+ corners.head
    val coordinates = ring.map { case (lon, lat) => util.Arrays.asList(lon, lat) }.asJava

    val geometry = new util.LinkedHashMap[String, Any]()
    geometry.put("type", "Polygon")
    geometry.put("coordinates", util.Arrays.asList(coordinates))
    geometry
  }

  def toStacMap: util.Map[String, Any] = {
    val b = bbox4326
    val itemGeometry = geometry
    val itemBbox = util.Arrays.asList(b.xmin, b.ymin, b.xmax, b.ymax)
    val itemProperties = properties

    val stacAssets = new util.LinkedHashMap[String, Any]()
    assets.forEach { (key, asset) =>
      val assetMap = asset.toStacMap
      Seq("datetime", "start_datetime", "end_datetime").foreach { field =>
        Option(itemProperties.get(field)).foreach(value => assetMap.put(field, value))
      }
      assetMap.put("geometry", itemGeometry)
      assetMap.put("bbox", itemBbox)
      stacAssets.put(key, assetMap)
    }

    val result = new util.LinkedHashMap[String, Any]()
    result.put("type", "Feature")
    result.put("stac_version", "1.1.0")
    result.put("id", id)
    result.put("geometry", itemGeometry)
    result.put("bbox", itemBbox)
    if (!itemProperties.isEmpty) result.put("properties", itemProperties)
    result.put("links", util.Collections.emptyList[util.Map[String, Any]]())
    result.put("assets", stacAssets)
    result
  }

  def toJson: String = Item.mapper.writeValueAsString(toStacMap)
}

object Item {
  private val mapper = new ObjectMapper()
  private val logger: Logger = LoggerFactory.getLogger(classOf[Item])

  private def warnIfIncompleteTemporalExtent(item: Item): Unit = {
    if (item.datetime == null && (item.startDatetime == null || item.endDatetime == null)) {
      logger.warn(s"STAC item ${item.id} has no complete temporal extent (start_datetime=${item.startDatetime}, " +
        s"end_datetime=${item.endDatetime}): the item is not valid STAC. " +
        "The driver should call setTemporalExtent(start, end) with both ends on the output options.")
    }
  }
}
