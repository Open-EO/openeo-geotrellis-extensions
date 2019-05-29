import geotrellis.contrib.vlm.ReadingSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.contrib.vlm.spark.RasterSourceRDD
import geotrellis.proj4.LatLng
import geotrellis.raster.{GridExtent, MultibandTile, Raster}
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, SinglebandGeoTiff}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.junit.Test

class Sentinel2FileDataCubeTest {

  @Test
  def test(): Unit = {
    val geoTiff10m = GeoTiffRasterSource("/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B02_10M_V102.tif")
    val geoTiff20m = GeoTiffRasterSource("/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B05_20M_V102.tif")
    val geoTiff60m = GeoTiffRasterSource("/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B01_60M_V102.tif")

    val bands = Seq(geoTiff10m, geoTiff20m, geoTiff60m)

    bands
      .map(band => band.cellSize)
      .foreach(println)

    val maxResolutionBand = bands.minBy(_.cellSize.resolution)

    val upsampledBands = bands
      .map { band =>
        if (band.cellSize == maxResolutionBand.cellSize) band else band.resampleToGrid(maxResolutionBand.rasterExtent)
      }

    val bbox = Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206).reproject(LatLng, maxResolutionBand.crs)

    /*val singleGeotiffs = for {
      (upsampledBand, i) <- upsampledBands.zipWithIndex
      Some(Raster(tile, extent)) = upsampledBand.read(bbox)
    } yield (SinglebandGeoTiff(tile.band(0), extent, upsampledBand.crs), i)

    singleGeotiffs.foreach { case (geotiff, i) => geotiff.write(s"/tmp/${i}_upsampled.geotiff") }

    val multibandTile = MultibandTile(singleGeotiffs.map { case (geotiff, _) => geotiff.tile })
    MultibandGeoTiff(multibandTile, singleGeotiffs.head._1.extent, maxResolutionBand.crs).write("/tmp/multi_upsampled.geotiff")*/

    val gridExtent = GridExtent(maxResolutionBand.extent, maxResolutionBand.cellSize)
    val layout = LayoutDefinition(gridExtent, 256)

    implicit val sc = SparkUtils.createLocalSparkContext("local[1]", getClass.getSimpleName)

    try {
      /*val rasterSourceRDD = RasterSourceRDD(upsampledBands, layout)
        .crop(bbox)*/
      val readingSources = upsampledBands.zipWithIndex
        .map { case (upsampledBand, i) => ReadingSource(upsampledBand, i) }

      val rasterSourceRDD = RasterSourceRDD.read(readingSources, layout)
        .crop(bbox)

      val Raster(multibandTile, extent) = rasterSourceRDD.stitch()
      MultibandGeoTiff(multibandTile, extent, rasterSourceRDD.metadata.crs).write("/tmp/stitched.geotiff")
    } finally {
      sc.stop()
    }
  }
}
