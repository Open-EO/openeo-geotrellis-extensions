package org.openeo.geotrellis.file

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.openeo.geotrellis.{OpenEOProcessScriptBuilder, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import scala.collection.JavaConverters._

object FileRDDFactoryTest {
  private var sc: SparkContext = _

  @BeforeAll
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[FileRDDFactoryTest].getName)

  @AfterAll
  def tearDownSpark(): Unit = sc.stop()
}

class FileRDDFactoryTest extends RasterMatchers {

  @Test
  def loadSpatialFeatureJsonRDDTest(): Unit = {
    val client = OpenSearchClient.apply(
      "https://catalogue.dataspace.copernicus.eu/resto", false, "", new java.util.ArrayList[String](), ""
    )
    val attributeValues = Map[String, Any](("processingLevel", "LEVEL1"), ("productType", "IW_GRDH_1S-COG")).asJava
    val file_rdd_factory = new FileRDDFactory(client, "Sentinel1", attributeValues, "unknown-job", CellSize(10, 10))
    val from_date = "2022-12-01T00:00:00+00:00"
    val to_date = "2022-12-30T00:00:00+00:00"
    val projected_polygons = ProjectedPolygons.fromExtent(Extent(11.23, 46.9, 11.45, 47), "EPSG:4326")
    val polygons_32632 = projected_polygons.extent.reproject(CRS.fromEpsgCode(32632))
    val projected_polygons_32632 = ProjectedPolygons.fromExtent(polygons_32632, "EPSG:32632")
    val dataCubeParameters = new DataCubeParameters()

    val min = "2022-12-05T00:00:00+00:00".asInstanceOf[Object] // selected date
    val max = "2022-12-06T00:00:00+00:00".asInstanceOf[Object]
    val excludeMax = true.asInstanceOf[Object]

    val builder = new OpenEOProcessScriptBuilder();
    val args = Map[String, Object](("min", min), ("max", max), ("exclude_max", excludeMax)).asJava
    builder.expressionStart("date_between", args);
    builder.argumentStart("x");
    builder.fromParameter("value");
    builder.argumentEnd();
    builder.expressionEnd("date_between", args);

    dataCubeParameters.timeDimensionFilter = Some(builder)

    val res = file_rdd_factory.loadSpatialFeatureJsonRDD(projected_polygons_32632, from_date, to_date, 0, 512, dataCubeParameters)
    val res_array = res._1.collect().toArray
    res_array.foreach(x => assert(x.asInstanceOf[String].contains("2022-12-05"))) // only selected date should be here
  }
}
