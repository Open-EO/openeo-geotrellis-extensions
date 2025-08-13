package org.openeo.geotrellis

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrelliscommon.{DataCubeParameters, DatacubeSupport, OpenEORasterCube, OpenEORasterCubeMetadata}

import java.time.ZonedDateTime

object TestUtils {

  /**
   * Aims to be structured like a 10m Sentinel-2 data cube. But with fake data, for faster testing.
   */
  def buildPlainSpatioTemporalDataCube(approximatePE: ProjectedExtent,
                                       resolutionInMeters: Double,
                                       datacubeParams: DataCubeParameters = new DataCubeParameters): OpenEORasterCube[SpaceTimeKey] = {
    var projectedExtent = safeReproject(approximatePE, CRS.fromEpsgCode(32631)) // UTM 31N. TODO: Get UTM from PE
    val tilePixelSize = datacubeParams.tileSize
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
    val layoutDef = LayoutDefinition(projectedExtent.extent, tileLayout)
    val dateTime = ZonedDateTime.parse("2019-01-01T00:00:00Z")  //  TODO: Make this configurable
    val gridBounds = layoutDef.mapTransform(projectedExtent.extent)
    val metadata = TileLayerMetadata(
      ShortConstantNoDataCellType,
      layoutDef,
      projectedExtent.extent,
      projectedExtent.crs,
      KeyBounds(
        SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, dateTime),
        SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, dateTime)
      )
    )

    implicit val sc: SparkContext = SparkContext.getOrCreate()
    // TODO: Find a way to partition correctly from the beginning, to avoid an extra stage:
    val keysRDD = sc.parallelize(gridBounds.coordsIter.toSeq, 100).map {
      case (col, row) => SpaceTimeKey(col, row, dateTime)
    }

    val p = DatacubeSupport.createPartitioner(Some(datacubeParams), keysRDD, metadata).get

    val rdd = keysRDD
      .map {
        case SpaceTimeKey(col, row, dateTime) => (SpaceTimeKey(col, row, dateTime), {
          // TODO: use LatLon coordinates as values
          val rasterTileLongitude = ShortArrayTile.apply((for {
            _ <- 0 until tilePixelSize
            pixel_j <- 0 until tilePixelSize
          } yield (pixel_j * 1.0 + col).toShort).toArray, tilePixelSize, tilePixelSize)

          val rasterTileLatitude = ShortArrayTile.apply((for {
            pixel_i <- 0 until tilePixelSize
            _ <- 0 until tilePixelSize
          } yield (pixel_i * 1.0 + row).toShort).toArray, tilePixelSize, tilePixelSize)

          val mbt1 = ArrayMultibandTile(Array(rasterTileLongitude, rasterTileLatitude)).asInstanceOf[MultibandTile]
          mbt1
        })
      }.partitionBy(p)


    new OpenEORasterCube[SpaceTimeKey](
      rdd,
      metadata,
      new OpenEORasterCubeMetadata(Seq("band1", "band2"))
    )
  }

}
