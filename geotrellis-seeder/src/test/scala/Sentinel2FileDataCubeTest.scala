import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.Test

class Sentinel2FileDataCubeTest {

  @Test
  def test(): Unit = {
    val geoTiff10m = "/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B02_10M_V102.tif"
    val geoTiff20m = "/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B05_20M_V102.tif"
    val geoTiff60m = "/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B01_60M_V102.tif"

    val bandGeoTiffs = Seq(geoTiff10m, geoTiff20m, geoTiff60m)
    val bbox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    implicit val sc = SparkUtils.createLocalSparkContext("local[*]", getClass.getSimpleName)

    try {
      val rasterSourceRDD = tileLayerRdd(bandGeoTiffs, bbox)
      val tileCount = rasterSourceRDD.count()

      println(s"got $tileCount tiles")

      val Raster(multibandTile, extent) = rasterSourceRDD
          .crop(bbox.reproject(rasterSourceRDD.metadata.crs))
          .stitch()

      MultibandGeoTiff(multibandTile, extent, rasterSourceRDD.metadata.crs).write("/tmp/stitched.geotiff")
    } finally {
      sc.stop()
    }
  }

  private def tileLayerRdd(geoTiffBandUris: Iterable[String], bbox: ProjectedExtent)(implicit sc: SparkContext): MultibandTileLayerRDD[SpatialKey] = {
    val bands = geoTiffBandUris.map(GeoTiffRasterSource(_))

    bands
      .map(band => band.cellSize)
      .foreach(println)

    val maxResolutionBand = bands.minBy(_.cellSize.resolution)

    val reprojectedBbox = bbox.reproject(maxResolutionBand.crs)

    val layout = FloatingLayoutScheme(tileSize = 256).levelFor(maxResolutionBand.extent, maxResolutionBand.cellSize).layout
    val overlappingKeys = layout.mapTransform.keysForGeometry(reprojectedBbox.toPolygon())

    val tiles = bands
      .map { band  => band.tileToLayout(layout) }
      .flatMap { layoutTileSource => layoutTileSource.readAll(overlappingKeys.toIterator).map { case (key, multibandTile) => (key, multibandTile.band(0)) } }
      .groupBy { case (key, _) => key }
      .map { case (key, tilesPerKey) =>
        val tiles = tilesPerKey.map { case (_, tile) => tile }
        (key, MultibandTile(tiles))
      }
      .toSeq

    val tilesRdd = sc.parallelize(tiles, tiles.size)

    val metadata = TileLayerMetadata[SpatialKey](
      maxResolutionBand.cellType,
      layout,
      reprojectedBbox,
      maxResolutionBand.crs,
      KeyBounds(layout.mapTransform(reprojectedBbox))
    )

    ContextRDD(tilesRdd, metadata)
  }
}
