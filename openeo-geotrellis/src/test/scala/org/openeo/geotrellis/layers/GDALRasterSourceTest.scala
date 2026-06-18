package org.openeo.geotrellis.layers

import com.azavea.gdal.GDALWarp
import geotrellis.raster.{GridBounds, MultibandTile, Raster, Tile}
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.spark.util.SparkUtils
import org.apache.spark.SparkContext
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.condition.EnabledIf
import org.openeo.geotrellis.layers.GDALRasterSourceTest.sc

object GDALRasterSourceTest {
  private implicit var sc: SparkContext = _

  @AfterAll
  def reinitGDAL(): Unit = {
    GDALWarp.deinit()
    GDALWarp.init(20)
  }

}

@EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasGdalInstalled")
class GDALRasterSourceTest {


  @BeforeEach
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

  @AfterEach
  def tearDownSpark(): Unit = {
    sc.stop()
  }

  @Test
  def checkRasterSource2(): Unit = {
    val rasterSource = GDALRasterSource("NETCDF:/home/dsamaey/data/MTDA/BIOPAR/BioPar_SWI1km_V1_Global/2018/20180801/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1.nc:SSF", GDALWarpOptions(outputFormat = Some("VRT"), ovr = Some(OverviewStrategy.DEFAULT)))
    val tile: MultibandTile = rasterSource.read().get.tile
    val band: Tile = tile.bands(0)
    var topLeft = (Int.MaxValue, Int.MaxValue)
    var bottomRight = (Int.MinValue, Int.MinValue)
    band.foreachIntVisitor(( (col: Int, row: Int, value: Int) => {
      if (value >= 0) {
        if (col < topLeft._1 && row < topLeft._2) {
          topLeft = (col, row)
        }
        if (col > bottomRight._1 && row > bottomRight._2) {
          bottomRight = (col, row)
        }
      }
    }))
    println(topLeft, bottomRight)
    val str = tile.band(0).toArray().groupBy(identity).mapValues(_.length).toSeq.sortBy(-_._2).toString()
    println(str)

    // GridBounds(337,337,464,464)
    // bands Seq(0)

    // GDALWarpOptions(-of VRT -ovr AUTO)

  }


  @Test
  def checkRasterSource(): Unit = {
    val gdalVersion = GDALWarp.get_version_info("VERSION_NUM")
    println(s"GDAL version: $gdalVersion")
    val rasterSource = GDALRasterSource("NETCDF:/data/MTDA/BIOPAR/BioPar_SWI1km_V1_Global/2018/20180801/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1.nc:SSF", GDALWarpOptions(outputFormat = Some("VRT"), ovr = Some(OverviewStrategy.DEFAULT)))
    val gridBounds: GridBounds[Long] = GridBounds(4104, 92, 6592, 3377)
    val raster = rasterSource.read(gridBounds, Seq(0)).get
    println(raster.toString)
    val tile: MultibandTile = raster.tile
    val str = tile.band(0).toArray().groupBy(identity).mapValues(_.length).toSeq.sortBy(_._1).toString()
    println(str)
    assertEquals("Raster(ArrayMultibandTile(2489,3286,1,uint8ud255),Extent(25.64285714285714, 41.83928571428572, 47.86607142857142, 71.17857142857143))", raster.toString)
    assertEquals("List((-2147483648,3947105), (0,3339), (1,4227883), (2,323), (3,204))", str)
  }

  @Test
  def readRasterSource(): Unit = {

    //    val r = GDALRasterSource("NETCDF:/data/MTDA/BIOPAR/BioPar_SWI1km_V1_Global/2018/20180919/c_gls_SWI1km_201809191200_CEURO_SCATSAR_V1.0.1/c_gls_SWI1km_201809191200_CEURO_SCATSAR_V1.0.1.nc:SSF", GDALWarpOptions(-of VRT -r near -tr 0.00297619047619 0.00297619047619 -t_srs +proj=longlat +datum=WGS84 +no_defs  -ovr AUTO -te 4.098214285711869 33.99702380952397 30.955357142850428 52.06249999999727 -te_srs +proj=longlat +datum=WGS84 +no_defs ))


    val rasterSource: GDALRasterSource = GDALRasterSource("NETCDF:/home/dsamaey/data/MTDA/BIOPAR/BioPar_SWI1km_V1_Global/2018/20180921/c_gls_SWI1km_201809211200_CEURO_SCATSAR_V1.0.1/c_gls_SWI1km_201809211200_CEURO_SCATSAR_V1.0.1.nc:SSF")
    //    val bounds: GridBounds[Long] = GridBounds(337, 337, 464, 464)
    val bounds: GridBounds[Long] = GridBounds(0, 0, 127, 127)
    val bands = Seq(0)
    //    val value: Option[Raster[MultibandTile]] = rasterSource.read(bounds, bands)
    //      assert(value.isDefined)

    // GridBounds(337,337,464,464)
    // bands Seq(0)
    // NETCDF:"/data/MTDA/BIOPAR/BioPar_SWI1km_V1_Global/2018/20180801/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1/c_gls_SWI1km_201808011200_CEURO_SCATSAR_V1.0.1.nc":SSF
    // GDALWarpOptions(-of VRT -ovr AUTO)
    // targetCEllType None

    // GDALWarpOptions(-of VRT -r near -tr 0.00297619047619 0.00297619047619 -t_srs +proj=longlat +datum=WGS84 +no_defs  -ovr AUTO -te 4.098214285711869 33.99702380952397 30.955357142850428 52.06249999999727 -te_srs +proj=longlat +datum=WGS84 +no_defs )

    val option = rasterSource.read()
    println(option)
    val tile: MultibandTile = option.get.tile
    val str = tile.band(0).toArray().groupBy(identity).mapValues(_.length).toSeq.sortBy(-_._2).toString()
    println(str)
  }
}
