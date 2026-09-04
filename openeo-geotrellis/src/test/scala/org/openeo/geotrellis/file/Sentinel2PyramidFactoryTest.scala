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
import org.openeo.opensearch.OpenSearchResponses.{FeatureCollection, STACFeatureCollection}
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

        val client = new FixedFeaturesOpenSearchClient
        FeatureCollection.parse(
        """{
          |    "features": [
          |        {
          |            "type": "Feature",
          |            "id": "urn:eop:VITO:COP_DEM_GLO_30M_COG:Copernicus_DSM_COG_10_N50_00_E004_00_DEM",
          |            "geometry": {"coordinates":[[[4.0,50.0],[5.0,50.0],[5.0,51.0],[4.0,51.0],[4.0,50.0]]],"type":"Polygon"},
          |            "bbox": [4.0,50.0,5.0,51.0],
          |            "properties":
          |            	{"date":"2013-05-22T17:26:43Z","identifier":"urn:eop:VITO:COP_DEM_GLO_30M_COG:Copernicus_DSM_COG_10_N50_00_E004_00_DEM","available":"2021-04-15T14:11:00Z","parentIdentifier":"urn:eop:VITO:COP_DEM_GLO_30M_COG","productInformation":{"processingCenter":"VITO","referenceSystemIdentifier":"https://www.opengis.net/def/crs/EPSG/0/4326","processingDate":"2021-04-21T16:30:48Z","productType":"DEM_GLO_30M","availabilityTime":"2021-04-15T14:11:00Z"},"links":{"related":[],"data":[{"length":31721015,"href":"file:///data/MTDA/DEM/COPERNICUS-DEM-30/Copernicus_DSM_COG_10_N50_00_E004_00_DEM/Copernicus_DSM_COG_10_N50_00_E004_00_DEM.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"DEM","bandNames":["DEM"]}],"previews":[],"alternates":[]},"published":"2021-04-15T14:11:00Z","title":"Copernicus_DSM_COG_10_N50_00_E004_00_DEM","updated":"2019-10-27T00:00:00Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"N50E004","beginningDateTime":"2011-03-27T17:33:08Z","endingDateTime":"2013-05-22T17:26:43Z"}}],"status":"ARCHIVED","additionalAttributes":{"verticalReferenceSystemIdentifier":"https://www.opengis.net/def/crs/EPSG/0/3855","resolution":30.0}}
          |         }
          |        ,{
          |            "type": "Feature",
          |            "id": "urn:eop:VITO:COP_DEM_GLO_30M_COG:Copernicus_DSM_COG_10_N51_00_E004_00_DEM",
          |            "geometry": {"coordinates":[[[4.0,51.0],[5.0,51.0],[5.0,52.0],[4.0,52.0],[4.0,51.0]]],"type":"Polygon"},
          |            "bbox": [4.0,51.0,5.0,52.0],
          |            "properties":
          |            	{"date":"2013-01-10T17:27:20Z","identifier":"urn:eop:VITO:COP_DEM_GLO_30M_COG:Copernicus_DSM_COG_10_N51_00_E004_00_DEM","available":"2021-04-15T14:11:01Z","parentIdentifier":"urn:eop:VITO:COP_DEM_GLO_30M_COG","productInformation":{"processingCenter":"VITO","referenceSystemIdentifier":"https://www.opengis.net/def/crs/EPSG/0/4326","processingDate":"2021-04-21T16:30:49Z","productType":"DEM_GLO_30M","availabilityTime":"2021-04-15T14:11:01Z"},"links":{"related":[],"data":[{"length":34273012,"href":"file:///data/MTDA/DEM/COPERNICUS-DEM-30/Copernicus_DSM_COG_10_N51_00_E004_00_DEM/Copernicus_DSM_COG_10_N51_00_E004_00_DEM.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/4326","type":"image/tiff","title":"DEM","bandNames":["DEM"]}],"previews":[],"alternates":[]},"published":"2021-04-15T14:11:01Z","title":"Copernicus_DSM_COG_10_N51_00_E004_00_DEM","updated":"2019-10-27T00:00:00Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"N51E004","beginningDateTime":"2011-03-22T17:26:27Z","endingDateTime":"2013-01-10T17:27:20Z"}}],"status":"ARCHIVED","additionalAttributes":{"verticalReferenceSystemIdentifier":"https://www.opengis.net/def/crs/EPSG/0/3855","resolution":30.0}}
          |         }
          |    ]
          |  }""".stripMargin
        ).features.foreach(feature => client.addFeature(feature))

        val openSearchClient = OpenSearchClient(new URL(openSearchEndpoint), isUTM = false)
        val factory = new PyramidFactory(
            client,
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
        val localDate = LocalDate.of(2026, 8, 7)
        val spatialLayer = createLayerForDate(bbox, localDate)

        checkStatsResult("testStatsFromPyramid", bbox, spatialLayer)
    }

    @EnabledIf("org.openeo.geotrelliscommon.TestConditions#hasMTDAData")
    @Test
    def testStatsFromNativeUTM(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val localDate = LocalDate.of(2026, 8, 7)
        val spatialLayer = createLayerForDate(bbox, localDate, pyramid = false)

        checkStatsResult("testStatsFromNativeUTM", bbox, spatialLayer)
    }

    private def checkStatsResult(context: String, bbox: ProjectedExtent, spatialLayer: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
        // spatialLayer.writeGeoTiff(s"/tmp/Sentinel2PyramidFactory_cropped_openeo_$context.tif", bbox)

        val polygon = bbox.reproject(spatialLayer.metadata.crs).toPolygon()

        val singleBandMean = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor) match {
            case Summary(values) => values.head.mean
        }

        val qgisZonalStaticsPluginResult = 4.699450065992081
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

        STACFeatureCollection.parse(
            """{
              |  "type": "FeatureCollection",
              |  "features": [
              |    {
              |      "type": "Feature",
              |      "stac_version": "1.1.0",
              |      "stac_extensions": [
              |        "https://stac-extensions.github.io/eo/v2.0.0/schema.json",
              |        "https://stac-extensions.github.io/sat/v1.0.0/schema.json",
              |        "https://stac-extensions.github.io/product/v0.1.0/schema.json",
              |        "https://stac-extensions.github.io/mgrs/v1.0.0/schema.json",
              |        "https://stac-extensions.github.io/grid/v1.1.0/schema.json",
              |        "https://stac-extensions.github.io/processing/v1.2.0/schema.json",
              |        "https://stac-extensions.github.io/file/v2.1.0/schema.json",
              |        "https://stac-extensions.github.io/raster/v2.0.0/schema.json",
              |        "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
              |        "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
              |        "https://stac-extensions.github.io/authentication/v1.1.0/schema.json"
              |      ],
              |      "id": "S2C_20260807T104621_31TCN_TOC_V220",
              |      "collection": "terrascope-s2-toc-v2",
              |      "geometry": {
              |        "type": "Polygon",
              |        "coordinates": [
              |          [
              |            [0.353496071341766, 47.3112713918985],
              |            [0.377280362167835, 46.8356437141635],
              |            [1.81664846691532, 46.8595830838483],
              |            [1.79435498889108, 47.8473711523488],
              |            [0.5412353563803, 47.8261990808026],
              |            [0.506279748806763, 47.7314940568723],
              |            [0.452957836887773, 47.5850291507813],
              |            [0.399727939547736, 47.4386396658516],
              |            [0.353496071341766, 47.3112713918985]
              |          ]
              |        ]
              |      },
              |      "bbox": [0.353496071341766, 46.8356437141635, 1.81664846691532, 47.8473711523488],
              |      "properties": {
              |        "title": "S2C_20260807T104621_31TCN_TOC_V220",
              |        "datetime": "2026-08-07T10:46:21.025000Z",
              |        "created": "2026-08-07T23:03:55.618954Z",
              |        "updated": "2026-08-07T23:03:56.042877Z",
              |        "start_datetime": "2026-08-07T10:46:21.025000Z",
              |        "end_datetime": "2026-08-07T10:46:21.025000Z",
              |        "providers": [
              |          {
              |            "name": "ESA",
              |            "roles": [
              |              "producer"
              |            ],
              |            "url": "https://earth.esa.int/"
              |          },
              |          {
              |            "name": "VITO",
              |            "roles": [
              |              "processor",
              |              "host"
              |            ],
              |            "url": "https://terrascope.be/"
              |          }
              |        ],
              |        "platform": "sentinel-2c",
              |        "instruments": [
              |          "msi"
              |        ],
              |        "constellation": "sentinel-2",
              |        "gsd": 10,
              |        "eo:cloud_cover": 0,
              |        "sat:absolute_orbit": 10031,
              |        "sat:relative_orbit": 51,
              |        "sat:orbit_state": "descending",
              |        "product:type": "TOC",
              |        "mgrs:utm_zone": 31,
              |        "mgrs:latitude_band": "T",
              |        "mgrs:grid_square": "CN",
              |        "grid:code": "MGRS-31TCN",
              |        "processing:facility": "VITO",
              |        "processing:datetime": "2026-08-07T23:03:51.167Z",
              |        "processing:version": "220",
              |        "image_refining": true,
              |        "proj:code": "EPSG:32631",
              |        "proj:geometry": {
              |          "type": "Polygon",
              |          "coordinates": [
              |            [
              |              [300000, 5190240],
              |              [409800, 5190240],
              |              [409800, 5300040],
              |              [300000, 5300040],
              |              [300000, 5190240]
              |            ]
              |          ]
              |        },
              |        "proj:bbox": [300000, 5190240, 409800, 5300040],
              |        "processing:software": {
              |          "sentinel2_terrascope": "1.1.0"
              |        },
              |        "auth:schemes": {
              |          "oidc": {
              |            "type": "openIdConnect",
              |            "description": "Authenticate with Terrascope OpenID Connect",
              |            "openIdConnectUrl": "https://sso.terrascope.be/auth/realms/terrascope/.well-known/openid-configuration"
              |          }
              |        }
              |      },
              |      "links": [
              |        {
              |          "rel": "self",
              |          "type": "application/geo+json",
              |          "href": "https://stac.terrascope.be/collections/terrascope-s2-toc-v2/items/S2C_20260807T104621_31TCN_TOC_V220"
              |        },
              |        {
              |          "rel": "parent",
              |          "type": "application/json",
              |          "href": "https://stac.terrascope.be/collections/terrascope-s2-toc-v2"
              |        },
              |        {
              |          "rel": "collection",
              |          "type": "application/json",
              |          "href": "https://stac.terrascope.be/collections/terrascope-s2-toc-v2"
              |        },
              |        {
              |          "rel": "root",
              |          "type": "application/json",
              |          "href": "https://stac.terrascope.be/"
              |        }
              |      ],
              |      "assets": {
              |        "AOT": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_AOT_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "AOT",
              |          "description": "Aerosol Optical Thickness at native 60M resolution",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 219373,
              |          "updated": "2026-08-07T23:03:10.790734Z",
              |          "data_type": "uint16",
              |          "raster:scale": 0.001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "AOT"
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_AOT_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "CLOUDMASK": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_CLOUDMASK_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "CLOUDMASK",
              |          "roles": [
              |            "data-mask",
              |            "cloud"
              |          ],
              |          "file:size": 53013,
              |          "updated": "2026-08-07T23:03:10.804733Z",
              |          "data_type": "uint8",
              |          "raster:scale": 1,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 255,
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_CLOUDMASK_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "RAA": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_RAA_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "RAA",
              |          "description": "Relative Azimuth Angle at native 60M resolution",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 411617,
              |          "updated": "2026-08-07T23:02:59.153954Z",
              |          "data_type": "uint16",
              |          "raster:scale": 0.01,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 65535,
              |          "unit": "deg",
              |          "bands": [
              |            {
              |              "name": "RAA"
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_RAA_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "SCL": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_SCENECLASSIFICATION_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "SCL",
              |          "description": "Scene classification generated by Sen2Cor",
              |          "roles": [
              |            "data-mask"
              |          ],
              |          "file:size": 2072801,
              |          "updated": "2026-08-07T23:03:10.725760Z",
              |          "data_type": "uint8",
              |          "raster:scale": 1,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "bands": [
              |            {
              |              "name": "SCL"
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_SCENECLASSIFICATION_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "SZA": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_SZA_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "SZA",
              |          "description": "Sun Zenith Angle at native 60M resolution",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 88207,
              |          "updated": "2026-08-07T23:02:58.984964Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.01,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "deg",
              |          "bands": [
              |            {
              |              "name": "SZA"
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_SZA_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B01": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B01_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B01",
              |          "description": "S2-MSI Band01, 60M resolution, Top Of Canopy Reflectance at 443nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 5230427,
              |          "updated": "2026-08-07T23:02:59.317943Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B01",
              |              "eo:common_name": "coastal",
              |              "eo:center_wavelength": 0.443,
              |              "eo:full_width_half_max": 0.027
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B01_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B02": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B02_10M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B02",
              |          "description": "S2-MSI Band02, 10M resolution, Top Of Canopy Reflectance at 490nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 184757429,
              |          "updated": "2026-08-07T23:03:01.218825Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B02",
              |              "eo:common_name": "blue",
              |              "eo:center_wavelength": 0.49,
              |              "eo:full_width_half_max": 0.098
              |            }
              |          ],
              |          "proj:shape": [10980, 10980],
              |          "proj:transform": [10, 0, 300000, 0, -10, 5300040, 0, 0, 1],
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B02_10M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B03": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B03_10M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B03",
              |          "description": "S2-MSI Band03, 10M resolution, Top Of Canopy Reflectance at 560nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 189789967,
              |          "updated": "2026-08-07T23:03:03.187704Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B03",
              |              "eo:common_name": "green",
              |              "eo:center_wavelength": 0.56,
              |              "eo:full_width_half_max": 0.045
              |            }
              |          ],
              |          "proj:shape": [10980, 10980],
              |          "proj:transform": [10, 0, 300000, 0, -10, 5300040, 0, 0, 1],
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B03_10M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B04": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B04_10M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B04",
              |          "description": "S2-MSI Band04, 10M resolution, Top Of Canopy Reflectance at 665nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 193855833,
              |          "updated": "2026-08-07T23:03:05.186080Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B04",
              |              "eo:common_name": "red",
              |              "eo:center_wavelength": 0.665,
              |              "eo:full_width_half_max": 0.038
              |            }
              |          ],
              |          "proj:shape": [10980, 10980],
              |          "proj:transform": [10, 0, 300000, 0, -10, 5300040, 0, 0, 1],
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B04_10M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B05": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B05_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B05",
              |          "description": "S2-MSI Band05, 20M resolution, Top Of Canopy Reflectance at 705nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 50819093,
              |          "updated": "2026-08-07T23:03:05.748045Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B05",
              |              "eo:common_name": "rededge",
              |              "eo:center_wavelength": 0.704,
              |              "eo:full_width_half_max": 0.019
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B05_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B06": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B06_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B06",
              |          "description": "S2-MSI Band06, 20M resolution, Top Of Canopy Reflectance at 740nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 50259285,
              |          "updated": "2026-08-07T23:03:06.455002Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B06",
              |              "eo:common_name": "rededge",
              |              "eo:center_wavelength": 0.74,
              |              "eo:full_width_half_max": 0.018
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B06_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B07": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B07_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B07",
              |          "description": "S2-MSI Band07, 20M resolution, Top Of Canopy Reflectance at 783nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 50676081,
              |          "updated": "2026-08-07T23:03:07.004968Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B07",
              |              "eo:common_name": "rededge",
              |              "eo:center_wavelength": 0.783,
              |              "eo:full_width_half_max": 0.028
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B07_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B08": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B08_10M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B08",
              |          "description": "S2-MSI Band08, 10M resolution, Top Of Canopy Reflectance at 842nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 194614789,
              |          "updated": "2026-08-07T23:03:09.027842Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B08",
              |              "eo:common_name": "nir",
              |              "eo:center_wavelength": 0.842,
              |              "eo:full_width_half_max": 0.145
              |            }
              |          ],
              |          "proj:shape": [10980, 10980],
              |          "proj:transform": [10, 0, 300000, 0, -10, 5300040, 0, 0, 1],
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B08_10M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B11": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B11_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B11",
              |          "description": "S2-MSI Band11, 20M resolution, Top Of Canopy Reflectance at 1610mm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 51775915,
              |          "updated": "2026-08-07T23:03:10.138775Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B11",
              |              "eo:common_name": "swir16",
              |              "eo:center_wavelength": 1.61,
              |              "eo:full_width_half_max": 0.143
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B11_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B12": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B12_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B12",
              |          "description": "S2-MSI Band12, 20M resolution, Top Of Canopy Reflectance at 2190nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 50618787,
              |          "updated": "2026-08-07T23:03:10.675741Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B12",
              |              "eo:common_name": "swir22",
              |              "eo:center_wavelength": 2.19,
              |              "eo:full_width_half_max": 0.242
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B12_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "B8A": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B8A_20M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "B8A",
              |          "description": "S2-MSI Band8A, 20M resolution, Top Of Canopy Reflectance at 865nm",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 50867217,
              |          "updated": "2026-08-07T23:03:09.586810Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.0001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "B8A",
              |              "eo:common_name": "nir08",
              |              "eo:center_wavelength": 0.865,
              |              "eo:full_width_half_max": 0.033
              |            }
              |          ],
              |          "proj:shape": [5490, 5490],
              |          "proj:transform": [20, 0, 300000, 0, -20, 5300040, 0, 0, 1],
              |          "gsd": 20,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC-B8A_20M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "QUICKLOOK": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC_QUICKLOOK_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "QUICKLOOK",
              |          "roles": [
              |            "thumbnail"
              |          ],
              |          "file:size": 1295041,
              |          "updated": "2026-08-07T23:03:10.823732Z",
              |          "data_type": "uint8",
              |          "raster:scale": 1,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 255,
              |          "proj:shape": [686, 686],
              |          "proj:transform": [160.058309037901, 0, 300000, 0, -160.058309037901, 5300040, 0, 0, 1],
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC_QUICKLOOK_V220.tif"
              |            }
              |          }
              |        },
              |        "metadata": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC_V220.xml",
              |          "type": "application/xml",
              |          "title": "metadata",
              |          "description": "INSPIRE metadata",
              |          "roles": [
              |            "metadata"
              |          ],
              |          "file:size": 41245,
              |          "updated": "2026-08-07T23:03:10.845731Z",
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_TOC_V220.xml"
              |            }
              |          }
              |        },
              |        "VZA": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_VZA_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "VZA",
              |          "description": "View Zenith Angle at native 60M resolution",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 186129,
              |          "updated": "2026-08-07T23:02:59.100956Z",
              |          "data_type": "int16",
              |          "raster:scale": 0.01,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "deg",
              |          "bands": [
              |            {
              |              "name": "VZA"
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_VZA_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "WVP": {
              |          "href": "https://services.terrascope.be/download/Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_WVP_60M_V220.tif",
              |          "type": "image/tiff; application=geotiff; profile=cloud-optimized",
              |          "title": "WVP",
              |          "description": "Column Water Vapor at native 60M resolution",
              |          "roles": [
              |            "data"
              |          ],
              |          "file:size": 4814125,
              |          "updated": "2026-08-07T23:03:10.786734Z",
              |          "data_type": "uint16",
              |          "raster:scale": 0.001,
              |          "raster:offset": 0,
              |          "raster:sampling": "area",
              |          "nodata": 32767,
              |          "unit": "-",
              |          "bands": [
              |            {
              |              "name": "WVP"
              |            }
              |          ],
              |          "proj:shape": [1830, 1830],
              |          "proj:transform": [60, 0, 300000, 0, -60, 5300040, 0, 0, 1],
              |          "gsd": 60,
              |          "alternate": {
              |            "local": {
              |              "href": "file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2026/08/07/S2C_20260807T104621_31TCN_TOC_V220/S2C_20260807T104621_31TCN_WVP_60M_V220.tif"
              |            }
              |          },
              |          "auth:refs": [
              |            "oidc"
              |          ]
              |        },
              |        "preview": {
              |          "href": "https://titiler.terrascope.be/collections/terrascope-s2-toc-v2/items/S2C_20260807T104621_31TCN_TOC_V220/preview?assets=B04&assets=B03&assets=B02&format=png&max_size=256&rescale=200,1600&rescale=200,1600&rescale=200,1600",
              |          "type": "image/png",
              |          "title": "Preview",
              |          "description": "Preview image",
              |          "roles": [
              |            "thumbnail",
              |            "overview"
              |          ],
              |          "proj:shape": [256, 256],
              |          "proj:code": null
              |        }
              |      }
              |    }
              |  ],
              |  "links": [
              |    {
              |      "rel": "root",
              |      "type": "application/json",
              |      "href": "https://stac.terrascope.be/"
              |    },
              |    {
              |      "rel": "self",
              |      "type": "application/json",
              |      "href": "https://stac.terrascope.be/search?collections=terrascope-s2-toc-v2&bbox=1.3381310991461013,47.05179515321805,1.3968672829862574,47.088475592749106&limit=200&datetime=2026-08-07T00:00:00Z/2026-08-07T23:59:59Z"
              |    }
              |  ],
              |  "numberReturned": 1,
              |  "numberMatched": 1
              |}""".stripMargin, toS3URL = false)._1.features.foreach(feature => openSearchClient.addFeature(feature))

        new PyramidFactory(
            openSearchClient,
            openSearchCollectionId = "terrascope-s2-toc-v2",
            openSearchLinkTitles = singletonList("SCL"),
            rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
            maxSpatialResolution = CellSize(10, 10)
        )
    }
}
