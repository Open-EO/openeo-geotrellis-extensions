package org.openeo.geotrellis.file

import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffOptions, Tags}
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
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Collections
import java.util.Collections.{emptyMap, singletonList}

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

    //@Test
    def testS2INCDLayer(): Unit = {
        val boundingBox: ProjectedExtent = ProjectedExtent(Extent(-5.0, 37.0, -4.0, 38.0), LatLng)
        var utmCrs : CRS = null
        val utmBoundingBox = {
            val center = boundingBox.extent.center
            utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
            ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
        }

        val localFromDate = LocalDate.of(2020, 12, 27)
        val localToDate = LocalDate.of(2020, 12, 28)
        val ZonedFromDate = ZonedDateTime.of(localFromDate, MIDNIGHT, UTC)
        val zonedToDate = ZonedDateTime.of(localToDate, MIDNIGHT, UTC)

        val projected_polygons_native_crs = ProjectedPolygons.fromExtent(utmBoundingBox.extent, utmBoundingBox.crs.toString())
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
        val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate
        val correlation_id = ""
        val openSearchEndpoint = "https://resto.c-scale.zcu.cz"
        val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = true)
        val factory = new PyramidFactory(
            openSearchClient,
            openSearchCollectionId = "S2",
            openSearchLinkTitles = singletonList("B02"),
            rootPath = null,
            maxSpatialResolution = CellSize(10, 10) // TODO: cube:dimensions has stepsize 10 but B01 has gsd 60m.
        )

        val metadata_properties = emptyMap[String, Any]()
        val datacubeParams = new DataCubeParameters()
        datacubeParams.tileSize = 256
        datacubeParams.layoutScheme = "FloatingLayoutScheme"
        val baseLayer = factory.datacube_seq(
            projected_polygons_native_crs,
            from_date, to_date, metadata_properties, correlation_id, datacubeParams
        ).maxBy { case (zoom, _) => zoom }._2

        val actualTiffs = baseLayer.toSpatial().toGeoTiffs(Tags.empty,GeoTiffOptions(DeflateCompression)).collect().toList.map(t => t._2)
        assert(actualTiffs.length == 1)
        actualTiffs.head.write("s2incd_01.tiff", true)
    }

    @Test
    def testSingleGeotiff():Unit = {
        val bandNames = Collections.singletonList("band")
        val client = OpenSearchClient("https://s3-eu-west-1.amazonaws.com/download.agisoft.com/gtg/us_nga_egm96_15.tif",false,null,bandNames,globClientType = "globspatialonly")
        val factory = new PyramidFactory(client,"",bandNames,null,CellSize(0.25,0.25))

        val localFromDate = LocalDate.of(2010, 1, 1)
        val localToDate = LocalDate.of(2015, 1, 1)
        val ZonedFromDate = ZonedDateTime.of(localFromDate, MIDNIGHT, UTC)
        val zonedToDate = ZonedDateTime.of(localToDate, MIDNIGHT, UTC)

        val extent = Extent(-4.0411, 37.969, -3.9911, 38.1197)
        val srs = "EPSG:4326"
        val projected_polygons_native_crs = ProjectedPolygons.fromExtent(extent, srs)
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
        val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate

        val cube: Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = factory.datacube_seq(projected_polygons_native_crs,from_date,to_date,Collections.emptyMap(),"")
        cube.head._2.toSpatial().writeGeoTiff("geoid.tif")

    }

    @Test
    def testDemLayer(): Unit = {
        val localFromDate = LocalDate.of(2010, 1, 1)
        val localToDate = LocalDate.of(2014, 1, 1)
        val ZonedFromDate = ZonedDateTime.of(localFromDate, MIDNIGHT, UTC)
        val zonedToDate = ZonedDateTime.of(localToDate, MIDNIGHT, UTC)

        val extent = Extent(4.0,51.0,4.5,51.5)
        val srs = "EPSG:4326"
        val projected_polygons_native_crs = ProjectedPolygons.fromExtent(extent, srs)
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
        val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate
        val correlation_id = ""

        val openSearchEndpoint = "https://services.terrascope.be/catalogue"
        val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = false)
        val factory = new PyramidFactory(
            openSearchClient,
            openSearchCollectionId = "urn:eop:VITO:COP_DEM_GLO_30M_COG",
            openSearchLinkTitles = singletonList("DEM"),
            rootPath = "/data/MTDA/DEM/COP_DEM_30M_COG",
            maxSpatialResolution = CellSize(0.002777777777777778, 0.002777777777777778)
        )

        val metadata_properties = emptyMap[String, Any]()
        val datacubeParams = new DataCubeParameters()
        datacubeParams.tileSize = 256
        datacubeParams.globalExtent = Some(ProjectedExtent(extent,LatLng))
        datacubeParams.layoutScheme = "FloatingLayoutScheme"
        val baseLayer = factory.datacube_seq(
            projected_polygons_native_crs,
            from_date, to_date, metadata_properties, correlation_id, datacubeParams
        ).maxBy { case (zoom, _) => zoom }._2

        // Compare actual with reference tile.
        //saveRDDTemporal(baseLayer,"/tmp")

        val dates = baseLayer.keys.map(_.time).distinct().collect()

        val actualTiffs = baseLayer.toSpatial(dates.apply(0)).toGeoTiffs(Tags.empty,GeoTiffOptions(DeflateCompression)).collect().toList.map(t => t._2)
        println(dates.mkString("Array(", ", ", ")"))
        assertEquals(1, actualTiffs.length )
        //actualTiffs.head.write("/tmp/tile0_0.tiff", true)

        val resourcePath = "org/openeo/geotrellis/file/testDemLayer/tile0_0.tiff"
        val refFile = Thread.currentThread().getContextClassLoader.getResource(resourcePath)
        val refTiff = GeoTiff.readMultiband(refFile.getPath)
        assertArrayEquals(refTiff.raster.tile.band(0).toArrayDouble(), actualTiffs.head.raster.tile.band(0).toArrayDouble(),0.1)
    }

    @Test
    def testStatsFromPyramid(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2022, 8, 1)
        val spatialLayer = createLayerForDate(bbox, localDate)

        checkStatsResult("testStatsFromPyramid", bbox, spatialLayer)
    }

    @Test
    def testStatsFromNativeUTM(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2022, 8, 1)
        val spatialLayer = createLayerForDate(bbox, localDate,pyramid = false)

        checkStatsResult("testStatsFromNativeUTM", bbox, spatialLayer)
    }

    private def checkStatsResult(context: String, bbox: ProjectedExtent, spatialLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
        // spatialLayer.writeGeoTiff(s"/tmp/Sentinel2PyramidFactory_cropped_openeo_$context.tif", bbox)

        val polygon = bbox.reproject(spatialLayer.metadata.crs).toPolygon()

        val singleBandMean = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor) match {
            case Summary(values) => values.head.mean
        }

        val qgisZonalStaticsPluginResult = 8.874901011878574
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

    private def sceneClassificationV200PyramidFactory = {
        val openSearchEndpoint = "https://services.terrascope.be/catalogue"
        val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = true)
        new PyramidFactory(
            openSearchClient,
            openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
            openSearchLinkTitles = singletonList("SCENECLASSIFICATION_20M"),
            rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
            maxSpatialResolution = CellSize(10, 10)
        )
    }
}
