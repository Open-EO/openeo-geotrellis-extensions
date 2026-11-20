package org.openeo.geotrellis.stac

import geotrellis.raster.CellType
import org.openeo.geotrellis.stac

import java.util

/**
 * A STAC asset.
 *
 * @param href       location of the asset
 * @param mediaType  STAC `type` of the asset
 * @param metadata   as produced by `GeneralUtils.setupAssetMetadata`
 */
case class Asset(href: String, cellType: CellType, mediaType: String, metadata: util.Map[String, Any] = null,
                 roles: util.List[String] = new util.ArrayList[String](util.Arrays.asList("data"))) {
  def dataType: String = stac.dataType(cellType)
  def nodata: java.lang.Number = stac.nodata(cellType)

  def toStacMap: util.Map[String, Any] = {
    val result = new util.LinkedHashMap[String, Any]()
    result.put("href", href)
    result.put("type", mediaType)
    result.put("roles", roles)
    result.put("data_type", dataType)

    nodata match {
      case null =>
      case n if n.doubleValue.isNaN => result.put("nodata", "nan") // not representable as a JSON number
      case n => result.put("nodata", n)
    }

    if (metadata != null) {
      metadata.forEach { (key, value) =>
        if (key != "time") result.put(key, value)
      }
    }

    result
  }
}
