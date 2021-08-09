package org.openeo.geotrellissentinelhub

import geotrellis.proj4.CRS
import geotrellis.vector._

import java.util
import scala.collection.mutable.ArrayBuffer

trait SparseMultiPolygonSimplification {
  def simplify(multiPolygons: Array[MultiPolygon], multiPolygonsCrs: CRS): (MultiPolygon, CRS)
}

// flattens n MultiPolygons into 1 by taking their polygon exteriors
// drawback: can result in big GeoJSON to send over the wire
object PolygonExteriorSparseMultiPolygonSimplification extends SparseMultiPolygonSimplification {
  override def simplify(multiPolygons: Array[MultiPolygon], multiPolygonsCrs: CRS): (MultiPolygon, CRS) = {
    val polygonExteriors = for {
      multiPolygon <- multiPolygons
      polygon <- multiPolygon.polygons
    } yield Polygon(polygon.getExteriorRing)

    (MultiPolygon(polygonExteriors), multiPolygonsCrs)
  }
}

// flattens n MultiPolygons into 1 by improving on PolygonExteriorSparseMultiPolygonSimplification and translating
// MultiPolygons' exteriors into fewer, courser polygons
class LocalGridSparseMultiPolygonSimplification(gridCrs: CRS, gridTileSize: Double) extends SparseMultiPolygonSimplification {
  override def simplify(multiPolygons: Array[MultiPolygon], multiPolygonsCrs: CRS): (MultiPolygon, CRS) = {
    val (flattenedMultiPolygon, flattenedMultiPolygonCrs) =
      PolygonExteriorSparseMultiPolygonSimplification.simplify(multiPolygons, multiPolygonsCrs)

    val reprojectedMultiPolygon = flattenedMultiPolygon.reproject(flattenedMultiPolygonCrs, gridCrs)
    val gridExtent = reprojectedMultiPolygon.extent

    val gridTiles = collectGridTiles(gridExtent, gridTileSize)

    val overlappingGridTiles = ArrayBuffer[Polygon]()

    for (polygon <- reprojectedMultiPolygon.polygons if !gridTiles.isEmpty) {
      val it = gridTiles.iterator()

      while (it.hasNext) {
        val gridTile = it.next()

        if (polygon intersects gridTile) {
          overlappingGridTiles += gridTile
          it.remove()
        }
      }
    }

    (MultiPolygon(overlappingGridTiles), gridCrs)
  }

  private def collectGridTiles(extent: Extent, tileSize: Double): util.List[Polygon] = {
    val gridTiles = new util.ArrayList[Polygon]()

    for {
      upperLeftY <- extent.ymax until extent.ymin by -tileSize
      upperLeftX <- extent.xmin until extent.xmax by tileSize
    } gridTiles.add(Extent(upperLeftX, upperLeftY - tileSize, upperLeftX + tileSize, upperLeftY).toPolygon())

    gridTiles
  }
}
