package org.openeo.geotrellis.tile

import geotrellis.proj4.LatLng
import geotrellis.proj4.util.UTM
import geotrellis.raster.{CellSize, GridBounds, GridExtent}
import geotrellis.vector._
import mil.nga.geopackage.{GeoPackage, GeoPackageManager}
import org.locationtech.jts.geom.Envelope

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object TileGrid {

  def getOverlappingFeaturesFromTileGrid[K](tileGrid: String, extent: ProjectedExtent) = {
    val geoPackage = getGeoPackage(tileGrid)

    val features = ListBuffer[(String, Extent)]()

    try {
      val extentLatLng = extent.reproject(LatLng)

      val resultSet = geoPackage.getFeatureDao(geoPackage.getFeatureTables.get(0))
        .query(s"ST_MaxX(geom)>=${extentLatLng.xmin} AND ST_MinX(geom)<=${extentLatLng.xmax} AND ST_MaxY(geom)>=${extentLatLng.ymin} AND ST_MinY(geom)<=${extentLatLng.ymax}")

      try while (resultSet.moveToNext) {
        val row = resultSet.getRow()
        val name = row.getValue("name").toString

        val points = row.getGeometry.getGeometry.asInstanceOf[mil.nga.sf.Polygon].getExteriorRing.getPoints
        val bbox = new Envelope()
        for (p <- points.asScala) {
          val reprojected = Point(p.getX(), p.getY).reproject(LatLng, extent.crs)
          bbox.expandToInclude(reprojected.getX,reprojected.getY)

        }
        val tileExtent = Extent(bbox)
        features.append((name, tileExtent))
      }
      finally resultSet.close()
    } finally geoPackage.close()

    features.toList
  }

  private def getGeoPackage(tileGrid: String): GeoPackage = {
    //TODO: can we move to artifactory?
    val basePath = "/data/MEP/tilegrids"
    val path =
      if (tileGrid.contains("degree"))
        s"$basePath/wgs84-1degree.gpkg"
      else if (tileGrid.contains("100km"))
        s"$basePath/100km.gpkg"
      else if (tileGrid.contains("20km"))
        s"$basePath/20km.gpkg"
      else
        s"$basePath/10km.gpkg"

    GeoPackageManager.open(new File(path))
  }

  def computeFeaturesForTileGrid(tileGrid: String, extent: ProjectedExtent) = {
    if (tileGrid.contains("degree")) {
      computeFeaturesForDegreeGrid(extent).toList
    } else {
      computeFeaturesForMetersGrid(tileGrid, extent).toList
    }
  }

  private def computeFeaturesForMetersGrid(tileGrid: String, extent: ProjectedExtent) = {
    val extentLatLng = extent.reproject(LatLng)
    val utmZone = UTM.getZone(extentLatLng.xmin, extentLatLng.ymin)
    val utmCrs = UTM.getZoneCrs(extentLatLng.xmin, extentLatLng.ymin)

    val extentUtm = extent.reproject(utmCrs)

    val cellSize =
      if (tileGrid.contains("100km"))
        100000
      else if (tileGrid.contains("20km"))
        20000
      else
        10000

    val gridExtent = getGridExtent(extentUtm, cellSize)

    def featureForColRow(col: Int, row: Int) = {
      val e = gridExtent.extentFor(GridBounds(col, row, col ,row))
      val mgrsId = MGRS.getUTM2MGRSSqId(e.center.getX, e.center.getY, utmZone.left.getOrElse(utmZone.right.get), isNorth = utmZone.isLeft)

      (s"${mgrsId}_${col}_$row", e.reproject(utmCrs, extent.crs))
    }

    val bounds = gridExtent.gridBoundsFor(extentUtm)

    for (c <- bounds.colMin to bounds.colMax;
         r <- bounds.rowMin to bounds.rowMax)
    yield featureForColRow(c, r)
  }

  private def getGridExtent(extent: Extent, cellSize: Int) = {
    val newXMin = math.floor(extent.xmin / 1e5) * 1e5
    val newYMin = math.floor(extent.ymin / 1e5) * 1e5
    val newXMax = math.ceil(extent.xmax / 1e5) * 1e5
    val newYMax = math.ceil(extent.ymax / 1e5) * 1e5

    GridExtent[Int](Extent(newXMin, newYMin, newXMax, newYMax), CellSize(cellSize, cellSize))
  }

  private def computeFeaturesForDegreeGrid(extent: ProjectedExtent) = {
    val extentLatLng = extent.reproject(LatLng)

    def featureForXY(x: Int, y: Int) = {
      val e = Extent(x, y, x + 1, y + 1)
      val latName = if (y < 0) f"S${math.abs(y)}%02d" else f"N$y%02d"
      val lonName = if (x < 0) f"W${math.abs(x)}%03d" else f"E$x%03d"

      (s"$latName$lonName", e.reproject(LatLng, extent.crs))
    }

    for (x <- math.floor(extentLatLng.xmin).toInt until math.ceil(extentLatLng.xmax).toInt;
         y <- math.floor(extentLatLng.ymin).toInt until math.ceil(extentLatLng.ymax).toInt)
    yield featureForXY(x, y)
  }

  object MGRS {
    // false northing used for south hemishere
    val FALSE_NORTHING_NORTH = 0
    val FALSE_NORTHING_SOUTH = 10000000

    // false easting at central meridian
    val FALSE_EASTING = 500000

    // bounds of the latitude UTM zones
    // 1118414,2011067,2902985,3793920,4683699,5572242,6459564,7345773,8231064,9115702,
    val BASE_NORTHING = List(-8881586, -7988933, -7097015, -6206080, -5316301, -4427758, -3540436, -2654227, -1768936, -884298,
      0, 884297, 1768935, 2654226, 3540435, 4427757, 5316300, 6206079, 7097014, 7988932, 9328093)

    // number to character conversion (allowed zone characters)
    val N2C = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    val NCN = 20
    val NCE = 24

    def getUTM2MGRSSqId(east: Double, north: Double, utmz: Int, isNorth: Boolean) = {
      if (utmz < 1 || utmz > 60) {
        throw new IllegalArgumentException(s"Invalid UTM zone: $utmz")
      }

      // substract false northing and easting and floor to meters
      val newEast = math.floor(east - FALSE_EASTING).toInt
      val newNorth = math.floor(if (isNorth) north - FALSE_NORTHING_NORTH else north - FALSE_NORTHING_SOUTH).toInt

      // get the UTM lateral zone
      var utmLatZone = ""
      breakable {
        for ((latBandBorder, idx) <- BASE_NORTHING.drop(1).zipWithIndex) {
          if (newNorth < latBandBorder) {
            utmLatZone = N2C(idx + 2).toString
            break
          }
        }
      }

      // MGRS letter easting offset of the central meridian
      val offCmr = 4 + ((utmz - 1) % 3) * 8

      // MGRS letter northing offset to equator
      val offEqt = ((utmz - 1) % 2) * 5

      // 100 km square easting/northing coordinates
      val eTmp = math.floor(newEast * 1e-5).toInt
      val nTmp = math.floor(newNorth * 1e-5).toInt

      // 100km square letter symbols
      val cn100km = N2C((nTmp + offEqt) % NCN)
      val ce100km = N2C((eTmp + offCmr) % NCE)

      f"$utmz%02d$utmLatZone$ce100km$cn100km"
    }
  }
}
