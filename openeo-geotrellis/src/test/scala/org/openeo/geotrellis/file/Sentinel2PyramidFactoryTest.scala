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
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.condition.EnabledIf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test}
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.geotiff.saveRDD
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.FeatureCollection
import org.openeo.opensearch.backends.GeotiffNoDateSearchClient

import java.net.URL
import java.nio.file.{Files, Path}
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Collections
import java.util.Collections.{emptyMap, singletonList}
import scala.io.Source


object Sentinel2PyramidFactoryTest {
    private var sc: SparkContext = _

    @BeforeAll
    def setupSpark(): Unit = {
        val sparkConf = new SparkConf()
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rdd.compress","true")

        sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
    }

    @AfterAll
    def tearDownSpark(): Unit = sc.stop()
}

class Sentinel2PyramidFactoryTest {

    @Disabled
    @Test
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
        val client = OpenSearchClient("https://download.agisoft.com/gtg/us_nga_egm96_15.tif",false,null,bandNames, clientType = "globspatialonly")
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
    def testDemDirEdgeError(): Unit = {
        val bandNames = Collections.singletonList("DEM")
        val tiffUrl = getClass.getResource("/org/openeo/geotrellis/geotiff/pretrain_train_adjecent_rectangles_DEM.tiff")
        val client = new GeotiffNoDateSearchClient(tiffUrl.getPath, bandNames)
        val factory = new PyramidFactory(client, "", bandNames, null, CellSize(0.0002777777777777778, 0.0002777777777777778))

        val localFromDate = LocalDate.of(1970, 1, 1)
        val localToDate = LocalDate.of(2070, 1, 1)
        val ZonedFromDate = ZonedDateTime.of(localFromDate, MIDNIGHT, UTC)
        val zonedToDate = ZonedDateTime.of(localToDate, MIDNIGHT, UTC)
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
        val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate

        val url = getClass.getResource("/org/openeo/geotrellis/geotiff/pretrain_train_adjecent_rectangles.geojson")
        import scala.util.Using
        val geoJson = Using(Source.fromURL(url)) { source => source.getLines.mkString("\n") }.get
        val h = geoJson.parseGeoJson[MultiPolygon]()
        val polygons = ProjectedPolygons(Array(h), LatLng)
        val dcp = new DataCubeParameters()
        dcp.globalExtent = Some(ProjectedExtent(Extent(-1.6147222333333389, 48.636527788888884, -0.13166667777778684, 49.638750011111114), LatLng))
        factory.datacube_seq(polygons, from_date, to_date, Collections.emptyMap(), "", dcp)
    }

    @Test
    def testPixelShift(): Unit = {
        val p = Path.of(f"tmp/testPixelShift/")
        Files.createDirectories(p)
        val bandNames = Collections.singletonList("SCF")
        val tiffUrl = getClass.getResource("/org/openeo/geotrellis/pixel_shift/SCF_2023012.tif") // compressed version
        val client = new GeotiffNoDateSearchClient(tiffUrl.getPath, bandNames)
        val factory = new PyramidFactory(client, "", bandNames, null, CellSize(500, 500))

        val fromDate = ZonedDateTime.of(LocalDate.of(1970, 1, 1), MIDNIGHT, UTC)
        val toDate = ZonedDateTime.of(LocalDate.of(2070, 1, 1), MIDNIGHT, UTC)
        val fromDateStr = DateTimeFormatter.ISO_OFFSET_DATE_TIME format fromDate
        val toDateStr = DateTimeFormatter.ISO_OFFSET_DATE_TIME format toDate

        val projectedExtent = ProjectedExtent(Extent(631800.0, 5167700.0, 655800.0, 5184200.0), CRS.fromName("EPSG:32632"))
        val polygons = ProjectedPolygons.fromExtent(projectedExtent.extent, projectedExtent.crs.toString())
        val dcp = new DataCubeParameters()
        dcp.globalExtent = Some(projectedExtent) // The offset in the extent will help align the output pixels with the source tiffs
        val baseLayer = factory.datacube_seq(polygons, fromDateStr, toDateStr, Collections.emptyMap(), "", dcp).maxBy { case (zoom, _) => zoom }._2
        val baseLayerSpatial = baseLayer.toSpatial()
        val actualTiffs = baseLayerSpatial.toGeoTiffs(Tags.empty, GeoTiffOptions(DeflateCompression)).collect().toList.map(t => t._2)

        // Values where a multiple of 500 before. Now it should take into account the global extent offset:
        assertEquals(Extent(631800.0, 5152200.0, 663800.0, 5184200.0), actualTiffs.head.extent)
        saveRDD(baseLayerSpatial, 1, "tmp/testPixelShift/testPixelShift.tiff")
    }

    @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
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

    @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
    @Test
    def testStatsFromPyramid(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2024, 8, 2)
        val spatialLayer = createLayerForDate(bbox, localDate)

        checkStatsResult("testStatsFromPyramid", bbox, spatialLayer)
    }

    @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
    @Test
    def testStatsFromNativeUTM(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2024, 8, 2)
        val spatialLayer = createLayerForDate(bbox, localDate,pyramid = false)

        checkStatsResult("testStatsFromNativeUTM", bbox, spatialLayer)
    }

    private def checkStatsResult(context: String, bbox: ProjectedExtent, spatialLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
        // spatialLayer.writeGeoTiff(s"/tmp/Sentinel2PyramidFactory_cropped_openeo_$context.tif", bbox)

        val polygon = bbox.reproject(spatialLayer.metadata.crs).toPolygon()

        val singleBandMean = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor) match {
            case Summary(values) => values.head.mean
        }

        val qgisZonalStaticsPluginResult = 8.982515316307564
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
        val openSearchClient = new FixedFeaturesOpenSearchClient

        FeatureCollection.parse(
            """{
              |    "@context": "http://schemas.opengis.net/os-geojson/1.0/os-geojson.jsonld",
              |    "type": "FeatureCollection",
              |    "id": "https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=1.3380835%2C47.0517205%2C1.3969783%2C47.0885245&sortKeys=title&startIndex=1&accessedFrom=MEP&clientId=&start=2024-08-02T00%3A00%3A00Z&end=2024-08-02T23%3A59%3A59.999999999Z",
              |    "totalResults": 1,
              |    "startIndex": 1,
              |    "itemsPerPage": 1,
              |    "queries": {
              |        "request": [
              |            {
              |    		"geo:box": "1.3380835,47.0517205,1.3969783,47.0885245",
              |    		"referrer:accessedFrom": "MEP",
              |    		"startIndex": 1,
              |    		"sru:sortKeys": "title,,1,0,highValue",
              |    		"time:start": "2024-08-02T00:00:00Z",
              |    		"time:end": "2024-08-02T23:59:59Z"
              |            }
              |        ]
              |    },
              |    "properties": {
              |        "title": "Product Search result",
              |        "subtitle": "Number of results: 1",
              |        "creator": "VITO OpenSearch Service",
              |        "authors": [
              |            {
              |                "type": "Agent",
              |                "name": "VITO"
              |            }
              |        ],
              |        "updated": "2026-02-18T16:55:58Z",
              |        "lang": "en",
              |        "links": {
              |	        "last": [
              |                {
              |                    "href": "https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=1.3380835%2C47.0517205%2C1.3969783%2C47.0885245&sortKeys=title&accessedFrom=MEP&clientId=&start=2024-08-02T00%3A00%3A00Z&end=2024-08-02T23%3A59%3A59.999999999Z&startIndex=1",
              |                    "type": "application/geo+json",
              |                    "title": "last"
              |                }
              |	        ],
              |	        "first": [
              |                {
              |                    "href": "https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=1.3380835%2C47.0517205%2C1.3969783%2C47.0885245&sortKeys=title&accessedFrom=MEP&clientId=&start=2024-08-02T00%3A00%3A00Z&end=2024-08-02T23%3A59%3A59.999999999Z&startIndex=1",
              |                    "type": "application/geo+json",
              |                    "title": "first"
              |                }
              |	        ],
              |	        "search": [
              |                {
              |                    "href": "https://services.terrascope.be/catalogue/description.geojson?collection=urn:eop:VITO:TERRASCOPE_S2_TOC_V2?accessedFrom=MEP",
              |                    "type": "application/geo+json",
              |                    "title": "search"
              |                }
              |	        ],
              |            "profiles": [
              |                {
              |                    "href": "http://www.opengis.net/spec/owc-geojson/1.0/req/core"
              |                },
              |                {
              |                    "href": "http://www.opengis.net/spec/os-geojson/1.0/req/core"
              |                }
              |            ]
              |        }
              |    },
              |    "features": [
              |        {
              |            "type": "Feature",
              |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20240802T104619_31TCN_TOC_V220",
              |            "geometry": {"type":"Polygon","coordinates":[[[0.3551042,47.2791119],[0.3772804,46.8356437],[1.8166485,46.8595831],[1.794355,47.8473712],[0.5549573,47.8264309],[0.5145704,47.717178],[0.4612082,47.5707009],[0.4079634,47.4242586],[0.3551042,47.2791119]]]},
              |            "bbox": [0.3551042,46.8356437,1.8166485,47.8473712],
              |            "properties":
              |            	{"date":"2024-08-02T10:46:19.024Z","updated":"2026-02-08T11:06:57.550Z","available":"2026-02-08T11:06:59Z","published":"2026-02-08T11:06:59Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","title":"S2B_20240802T104619_31TCN_TOC_V220","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20240802T104619_31TCN_TOC_V220","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitNumber":38687,"relativeOrbitNumber":51,"beginningDateTime":"2024-08-02T10:46:19.024Z","endingDateTime":"2024-08-02T10:46:19.024Z","tileId":"31TCN"}}],"productInformation":{"cloudCover":74.068,"productType":"TOC","availabilityTime":"2026-02-08T11:06:59Z","productVersion":"V220","processingCenter":"VITO","processingDate":"2026-02-08T11:06:57.550Z"},"links":{"previews":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC_QUICKLOOK_V220.tif","type":"image/tiff","length":556399,"category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2024-08-02&BBOX=39530.01872255278,5915288.082122198,202228.38597036424,6081500.298637985&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC_V220.xml","type":"application/vnd.iso.19139+xml","length":41247,"title":"Inspire metadata"}],"related":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_AOT_60M_V220.tif","type":"image/tiff","length":363267,"title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_CLOUDMASK_20M_V220.tif","type":"image/tiff","length":221989,"title":"CLOUDMASK_20M","bandNames":["CLOUDMASK_20M"],"category":"CLOUD"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_RAA_60M_V220.tif","type":"image/tiff","length":417335,"title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_SCENECLASSIFICATION_20M_V220.tif","type":"image/tiff","length":1945781,"title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_SZA_60M_V220.tif","type":"image/tiff","length":88351,"title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_VZA_60M_V220.tif","type":"image/tiff","length":194229,"title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_WVP_60M_V220.tif","type":"image/tiff","length":1085385,"title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B01_60M_V220.tif","type":"image/tiff","length":3210589,"title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B02_10M_V220.tif","type":"image/tiff","length":98703213,"title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B03_10M_V220.tif","type":"image/tiff","length":99163533,"title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B04_10M_V220.tif","type":"image/tiff","length":101210785,"title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B05_20M_V220.tif","type":"image/tiff","length":28211713,"title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B06_20M_V220.tif","type":"image/tiff","length":28270125,"title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B07_20M_V220.tif","type":"image/tiff","length":28403039,"title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B08_10M_V220.tif","type":"image/tiff","length":97574377,"title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B11_20M_V220.tif","type":"image/tiff","length":28459759,"title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B12_20M_V220.tif","type":"image/tiff","length":28087121,"title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2024/08/02/S2B_20240802T104619_31TCN_TOC_V220/S2B_20240802T104619_31TCN_TOC-B8A_20M_V220.tif","type":"image/tiff","length":28418447,"title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}]}}
              |         }
              |    ]
              |  }""".stripMargin).features.foreach(feature => openSearchClient.addFeature(feature))

        new PyramidFactory(
            openSearchClient,
            openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
            openSearchLinkTitles = singletonList("SCENECLASSIFICATION_20M"),
            rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
            maxSpatialResolution = CellSize(10, 10)
        )
    }
}
