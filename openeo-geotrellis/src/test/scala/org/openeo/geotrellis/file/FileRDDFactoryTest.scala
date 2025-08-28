package org.openeo.geotrellis.file

import cats.syntax.either._
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.CellSize
import geotrellis.raster.testkit.RasterMatchers
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.io.wkt.WKT
import geotrellis.vector.{Extent, Geometry, MultiPolygon, Polygon, ReprojectGeometry}
import io.circe.{Json, parser}
import io.circe.parser.{decode => circeDecode}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.api.java.JavaRDD
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.openeo.geotrellis.{OpenEOProcessScriptBuilder, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.{Instant, LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.ZoneOffset.UTC
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
    val client = OpenSearchClient(new URL("https://catalogue.dataspace.copernicus.eu/resto"))
    val attributeValues = Map[String, Any](("processingLevel", "LEVEL1"), ("productType", "IW_GRDH_1S-COG")).asJava
    val file_rdd_factory = new FileRDDFactory(client, "Sentinel1", attributeValues, "unknown-job", CellSize(10, 10))
    val from_date = "2022-12-01T00:00:00+00:00"
    val to_date = "2022-12-30T00:00:00+00:00"
    val projected_polygons = ProjectedPolygons.fromExtent(Extent(10.8, 46.0, 11.45, 46.5), "EPSG:4326")
    val polygons_32632 = projected_polygons.extent.reproject(CRS.fromEpsgCode(32632))
    val projected_polygons_32632 = ProjectedPolygons.fromExtent(polygons_32632, "EPSG:32632")
    val dataCubeParameters = new DataCubeParameters()

    val min = "2022-12-05T00:00:00+00:00".asInstanceOf[Object] // selected date
    val max = "2022-12-06T00:00:00+00:00".asInstanceOf[Object]
    val excludeMax = true.asInstanceOf[Object]

    val builder = new OpenEOProcessScriptBuilder()
    val args = Map[String, Object](("min", min), ("max", max), ("exclude_max", excludeMax)).asJava
    builder.expressionStart("date_between", args)
    builder.argumentStart("x")
    builder.fromParameter("value")
    builder.argumentEnd()
    builder.expressionEnd("date_between", args)

    dataCubeParameters.timeDimensionFilter = Some(builder)

    val res: (JavaRDD[String], TileLayerMetadata[SpaceTimeKey], Partitioner) = file_rdd_factory.loadSpatialFeatureJsonRDD(projected_polygons_32632, from_date, to_date, 0, 512, dataCubeParameters)
    val res_array = res._1.collect().asScala

    val productBounds: Geometry = WKT.read("MULTIPOLYGON (((13.877172 44.690407, 14.278967 46.191269, 10.879353 46.599377, 10.566596 45.099113, 13.877172 44.690407)))")
    val productBoundsUTM = productBounds.reproject(LatLng, CRS.fromEpsgCode(32632))
    res_array.foreach(x => assert(x.contains("2022-12-05")))
    res_array.foreach {jsonString => {
      val decoded = parser.parse(jsonString).getOrElse(Json.Null)

      val extent = decoded.hcursor.downField("key_extent")
      val xmin: Double = extent.downField("xmin").as[Double].toOption.get
      val ymin = extent.downField("ymin").as[Double].toOption.get
      val xmax = extent.downField("xmax").as[Double].toOption.get
      val ymax = extent.downField("ymax").as[Double].toOption.get


      assert(productBoundsUTM.intersects(new Extent(xmin,ymin,xmax,ymax)))
    }}// only selected date should be here
  }

  @ParameterizedTest
  @ValueSource(strings = Array("2022-06-18T00:00:00+00:00", "2022-06-19T00:00:00+00:00"))
  def testUpperTemporalBound(until_datetime: String): Unit = {
    // TODO: replace with FixedFeaturesOpenSearchClient?
    val openSearchClient = OpenSearchClient(new URL("https://catalogue.dataspace.copernicus.eu/resto"))
    val attributeValues = Map[String, Any]("productType" -> "SY_2_SYN___").asJava
    val fileRddFactory = new FileRDDFactory(
      openSearchClient,
      openSearchCollectionId = "Sentinel3",
      attributeValues,
      correlationId = "unknown-job",
      maxSpatialResolution = CellSize(1.0/112/3,  1.0/112/3)
    )

    val from = ZonedDateTime.parse("2022-06-18T00:00:00+00:00", ISO_OFFSET_DATE_TIME)
    val until = ZonedDateTime.parse(until_datetime, ISO_OFFSET_DATE_TIME)

    val (javaRdd, _,_) = fileRddFactory.loadSpatialFeatureJsonRDD(
      ProjectedPolygons.fromExtent(Extent(399960.0, 1590240.0, 509760.0, 1700040.0), crs = "EPSG:32628")
        .reproject(LatLng),
      from_datetime = ISO_OFFSET_DATE_TIME format from,
      until_datetime = ISO_OFFSET_DATE_TIME format until,
      zoom = 0
    )

    val keyInstants = javaRdd.rdd
      .flatMap(json => circeDecode[Json](json).valueOr(throw _).asObject)
      .flatMap(_("key"))
      .flatMap(_.asObject)
      .flatMap(_("instant"))
      .flatMap(_.asNumber)
      .flatMap(_.toLong)
      .map(Instant.ofEpochMilli)
      .collect()

    assertTrue(keyInstants.nonEmpty)
    assertTrue(keyInstants.forall { instant =>
      LocalDate.ofInstant(instant, UTC) == from.toLocalDate
    })
  }
}
