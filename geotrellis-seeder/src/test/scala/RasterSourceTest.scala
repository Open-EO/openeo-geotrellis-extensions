import java.time.LocalDate

import be.vito.eodata.catalog.CatalogClient
import geotrellis.contrib.vlm.gdal.{GDALRasterSource, GDALReprojectRasterSource}
import geotrellis.contrib.vlm.geotiff.{GeoTiffRasterSource, GeoTiffReprojectRasterSource}
import geotrellis.contrib.vlm.{GlobalLayout, RasterSource}
import geotrellis.proj4.WebMercator
import geotrellis.raster.render.Png
import geotrellis.raster.{CellSize, UByteConstantNoDataCellType}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.CRSWorldExtent
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.junit.{Ignore, Test}
import org.openeo.geotrellisvlm.ColorMapParser

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class RasterSourceTest {

  @Test
  @Ignore
  def testReprojection(): Unit = {
    val globalLayout = GlobalLayout(256, 14, 0.1)
    val (layout, _) = globalLayout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))
    
    val catalog = new CatalogClient
    val date = LocalDate.of(2019, 2, 4)
    val products = catalog.getProducts("CGS_S2_FAPAR", date, date, "GEOTIFF")
    val source = products.asScala.flatMap(_.getFiles.asScala).filter(_.getBands.contains("FAPAR")).map(_.getFilename.getPath).filter(_.contains("S2B_20190204T103229Z_33UUA_FAPAR_10M_V102")).head

    val geo = GeoTiffRasterSource(source)
    val gdal = GDALRasterSource(source)
 
    assertEquals(geo.extent, gdal.extent)
    
    val reprGeo = GeoTiffReprojectRasterSource(source, WebMercator)
    val reprGdal = GDALReprojectRasterSource(source, WebMercator)
    
    assertNotEquals(reprGeo.extent, reprGdal.extent)

    val colorMap = ColorMapParser.parse(getClass.getResourceAsStream("colorMaps/styles_ColorTable_NDVI_V2.sld"))
    val keys = Array(SpatialKey(8777, 5246), SpatialKey(8777, 5245), SpatialKey(8776, 5246), SpatialKey(8776, 5245))

    def renderPng(source: RasterSource): Png = {
      source.tileToLayout(layout).readAll(keys.iterator).map(s => (s._1, s._2.band(0).convert(UByteConstantNoDataCellType))).toList.stitch().renderPng(colorMap)
    }
    
    val pngGeo = renderPng(reprGeo)
    val pngGdal = renderPng(reprGdal)
    
    pngGeo.write(System.getProperty("user.home") + "/geo.png")
    pngGdal.write(System.getProperty("user.home") + "/gdal.png")
    
    assert(!(pngGeo.bytes sameElements pngGdal.bytes))
  }
}
