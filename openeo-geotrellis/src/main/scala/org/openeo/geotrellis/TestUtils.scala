package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrelliscommon.{DataCubeParameters, OpenEORasterCube, OpenEORasterCubeMetadata}

import java.time.ZonedDateTime

object TestUtils {

  /**
   * Aims to be structured like a 10m Sentinel-2 data cube. But with fake data, for faster testing.
   */
  def buildPlainSpatioTemporalDataCube(approximatePE: ProjectedExtent,
                                      resolutionInMeters: Double,
                                      parameters: DataCubeParameters = new DataCubeParameters): OpenEORasterCube[SpaceTimeKey] = {
    var projectedExtent = safeReproject(approximatePE, CRS.fromEpsgCode(32631)) // UTM 31N. TODO: Get UTM from PE
    val tilePixelSize = parameters.tileSize
    val resolution = resolutionInMeters * tilePixelSize
    projectedExtent = ProjectedExtent(
      Extent(
        math.floor(projectedExtent.extent.xmin / resolution) * resolution,
        math.floor(projectedExtent.extent.ymin / resolution) * resolution,
        math.ceil(projectedExtent.extent.xmax / resolution) * resolution,
        math.ceil(projectedExtent.extent.ymax / resolution) * resolution
      ),
      projectedExtent.crs
    )
    val horizontalTiles = (projectedExtent.extent.width / resolution).toInt
    val verticalTiles = (projectedExtent.extent.height / resolution).toInt
    val tileLayout = new TileLayout(horizontalTiles, verticalTiles, tilePixelSize, tilePixelSize)

    val mbTiles = for {
      i <- 0 until horizontalTiles
      j <- 0 until verticalTiles
    } yield {
      // TODO: use LatLon coordinates as values
      val rasterTileLongitude = ShortArrayTile.apply((for {
        _ <- 0 until tilePixelSize
        pixel_j <- 0 until tilePixelSize
      } yield (pixel_j * 1.0 + j).toShort).toArray, tilePixelSize, tilePixelSize)

      val rasterTileLatitude = ShortArrayTile.apply((for {
        pixel_i <- 0 until tilePixelSize
        _ <- 0 until tilePixelSize
      } yield (pixel_i * 1.0 + i).toShort).toArray, tilePixelSize, tilePixelSize)

      val mbt1 = ArrayMultibandTile(Array(rasterTileLongitude, rasterTileLatitude)).asInstanceOf[MultibandTile]
      mbt1
    }

    assert(mbTiles.length == horizontalTiles * verticalTiles)
    val dateTime = ZonedDateTime.parse("2019-01-01T00:00:00Z")
    val cellType: CellType = ShortConstantNoDataCellType

    val layout = LayoutDefinition(projectedExtent.extent, tileLayout)
    val metadata = TileLayerMetadata(
      cellType,
      layout,
      projectedExtent.extent,
      projectedExtent.crs,
      bounds = {
        val GridBounds(colMin, rowMin, colMax, rowMax) = layout.mapTransform(projectedExtent.extent)
        KeyBounds(
          SpaceTimeKey(colMin, rowMin, dateTime),
          SpaceTimeKey(colMax, rowMax, dateTime)
        )
      }
    )

    val re = RasterExtent(
      extent = projectedExtent.extent,
      cols = tileLayout.layoutCols,
      rows = tileLayout.layoutRows
    )

    val tmsTiles = re.gridBoundsFor(projectedExtent.extent).coordsIter.zip(mbTiles.toIterator).map {
      case ((col, row), tile) => (SpaceTimeKey(col, row, dateTime), tile)
    }
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    val parallel = sc.parallelize(tmsTiles.toSeq, 1000).partitionBy(SpacePartitioner(metadata.bounds))
    new OpenEORasterCube[SpaceTimeKey](
      parallel,
      metadata,
      new OpenEORasterCubeMetadata(Seq("band1", "band2"))
    )
  }

}
