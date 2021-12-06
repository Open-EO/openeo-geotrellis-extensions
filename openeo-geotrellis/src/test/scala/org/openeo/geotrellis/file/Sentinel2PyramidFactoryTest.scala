package org.openeo.geotrellis.file

import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.{CellSize, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import org.openeo.geotrelliscommon.DataCubeParameters

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Collections.{emptyMap, singletonList}
import scala.collection.mutable.ListBuffer

object Sentinel2PyramidFactoryTest {
    private var sc: SparkContext = _

    @BeforeClass
    def setupSpark(): Unit = {
        val sparkConf = new SparkConf()
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rdd.compress","true")

        sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
    }

    @AfterClass
    def tearDownSpark(): Unit = sc.stop()
}

class Sentinel2PyramidFactoryTest {

    @Test
    def testDemLayer(): Unit = {
        val localFromDate = LocalDate.of(2010, 1, 1)
        val localToDate = LocalDate.of(2012, 1, 1)
        val ZonedFromDate = ZonedDateTime.of(localFromDate, MIDNIGHT, UTC)
        val zonedToDate = ZonedDateTime.of(localToDate, MIDNIGHT, UTC)

        val extent = Extent(4.0,51.0,4.5,51.5)
        val srs = "EPSG:4326"
        val projected_polygons_native_crs = ProjectedPolygons.fromExtent(extent, srs)
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
        val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate
        val correlation_id = ""

        val factory = new Sentinel2PyramidFactory(
            openSearchEndpoint = "http://oscars-dev.vgt.vito.be/",
            openSearchCollectionId = "urn:eop:VITO:COP_DEM_GLO_30M_COG",
            openSearchLinkTitles = singletonList("DEM"),
            rootPath = "/data/MTDA/DEM/COP_DEM_30M_COG",
            maxSpatialResolution = CellSize(0.002777777777777778, 0.002777777777777778)
        )

        val metadata_properties = emptyMap[String, Any]()
        val datacubeParams = new DataCubeParameters()
        datacubeParams.tileSize = 256
        datacubeParams.layoutScheme = "FloatingLayoutScheme"
        val baseLayer = factory.datacube_seq(
            projected_polygons_native_crs,
            from_date, to_date, metadata_properties, correlation_id, datacubeParams
        ).maxBy { case (zoom, _) => zoom }._2

        // Compare actual with reference tile.
        val actualTiffs = baseLayer.toSpatial().toGeoTiffs(Tags.empty,GeoTiffOptions(DeflateCompression)).collect().toList.map(t => t._2)
        assert(actualTiffs.length == 1)
        //actualTiffs.head.write("/tmp/tile0_0.tiff", true)

        val resourcePath = "org/openeo/geotrellis/file/testDemLayer/tile0_0.tiff"
        val refFile = Thread.currentThread().getContextClassLoader.getResource(resourcePath)
        val refTiff = GeoTiff.readMultiband(refFile.getPath)
        assertArrayEquals(refTiff.toByteArray, actualTiffs.head.toByteArray)
    }

    @Test
    def testStatsFromPyramid(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2019, 10, 11)
        val spatialLayer = createLayerForDate(bbox, localDate)

        checkStatsResult("testStatsFromPyramid", bbox, spatialLayer)
    }

    @Test
    def testStatsFromNativeUTM(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2019, 10, 11)
        val spatialLayer = createLayerForDate(bbox, localDate,pyramid = false)

        checkStatsResult("testStatsFromNativeUTM", bbox, spatialLayer)
    }

    private def checkStatsResult(context: String, bbox: ProjectedExtent, spatialLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
        // spatialLayer.writeGeoTiff(s"/tmp/Sentinel2PyramidFactory_cropped_openeo_$context.tif", bbox)

        val polygon = bbox.reproject(spatialLayer.metadata.crs).toPolygon()

        val singleBandMean = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor) match {
            case Summary(values) => values.head.mean
        }

        val qgisZonalStaticsPluginResult = 4.510951226022072
        assertEquals(qgisZonalStaticsPluginResult, singleBandMean, 0.005)
    }


    private def createLayerForDate(bbox: ProjectedExtent, localDate: LocalDate, pyramid:Boolean=true) = {
        val date = ZonedDateTime.of(localDate, MIDNIGHT, UTC)

        val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
        val to_date = from_date

        val baseLayer =
            if(pyramid) {
                sceneClassificationV200PyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date,
                    metadata_properties = emptyMap[String, Any]())
                  .maxBy { case (zoom, _) => zoom }._2
            }else{
                sceneClassificationV200PyramidFactory.datacube(Array(MultiPolygon(bbox.extent.toPolygon())), bbox.crs,
                    from_date, to_date, correlationId = "")
            }

        val spatialLayer = baseLayer
          .toSpatial(date)
          .cache()
        spatialLayer
    }

    private def sceneClassificationV200PyramidFactory = new Sentinel2PyramidFactory(
        openSearchEndpoint = "https://services.terrascope.be/catalogue",
        openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
        openSearchLinkTitles = singletonList("SCENECLASSIFICATION_20M"),
        rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
        maxSpatialResolution = CellSize(10, 10)
    )
}
