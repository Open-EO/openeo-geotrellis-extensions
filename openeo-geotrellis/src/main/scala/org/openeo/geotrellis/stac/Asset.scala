package org.openeo.geotrellis.stac

import geotrellis.proj4.CRS
import geotrellis.raster.CellType
import geotrellis.vector.Extent
import org.openeo.geotrellis.GeneralUtils.fixBboxLargerThanWorld
import org.openeo.geotrellis.stac

import java.util

/**
 * A STAC asset.
 *
 * @param href           location of the asset
 * @param mediaType      STAC `type` of the asset
 * @param bandNames      names of the bands in the asset, emitted as `bands`
 * @param bandStatistics statistics per band name, added to the matching entry of `bands`
 * @param bbox           extent in `crs`, emitted as `proj:bbox`; projection fields are only emitted if set
 * @param crs            emitted as `proj:epsg`
 * @param shape          (rows, cols), emitted as `proj:shape`
 */
case class Asset(href: String, cellType: CellType, mediaType: String,
                 bandNames: Seq[String] = Seq.empty,
                 bandStatistics: collection.Map[String, util.HashMap[String, Any]] = Map.empty,
                 bbox: Extent = null, crs: CRS = null, shape: Array[Int] = null,
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

    if (bandNames.nonEmpty) {
      val bands = new util.ArrayList[util.HashMap[String, Any]]()
      bandNames.foreach { name =>
        val band = new util.HashMap[String, Any]()
        band.put("name", name)
        bandStatistics.get(name).foreach(stats => band.put("statistics", stats))
        bands.add(band)
      }
      result.put("bands", bands)
      result.put("raster:bands", bands) // duplicate of `bands` for consumers of the pre-1.1 raster extension
    }

    if (bbox != null && crs != null) {
      val cropped = fixBboxLargerThanWorld(bbox, crs)
      result.put("proj:bbox", Array(cropped.xmin, cropped.ymin, cropped.xmax, cropped.ymax))
      crs.epsgCode.foreach(epsg => result.put("proj:epsg", epsg))
    }
    if (shape != null) result.put("proj:shape", shape)

    result
  }
}
