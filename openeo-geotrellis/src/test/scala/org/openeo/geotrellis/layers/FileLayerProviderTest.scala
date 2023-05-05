package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{FloatingLayoutScheme, LayoutTileSource, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, RasterSource}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.{AfterClass, BeforeClass, Ignore}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotSame, assertSame, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.{AfterClass, BeforeClass}
import org.openeo.geotrellis.TestImplicits._
import org.openeo.geotrellis.layers.FileLayerProvider.rasterSourceRDD
import org.openeo.geotrellis.{LayerFixtures, ProjectedPolygons}
import org.openeo.geotrelliscommon.DatacubeSupport._
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, NoCloudFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceTimePartitioner}
import org.openeo.opensearch.OpenSearchResponses.{CreoFeatureCollection, FeatureCollection}
import org.openeo.opensearch.backends.CreodiasClient
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}

import java.io.File
import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util.Collections
import scala.collection.JavaConverters._
import scala.io.Source

object FileLayerProviderTest {
  // Methods with attributes get called in a non-intuitive order:
  // - BeforeAll
  // - ParameterizedTest
  // - AfterAll
  // - BeforeClass
  // - AfterClass
  //
  // This order feels arbitrary, so I made the code robust against order changes.

  private var _sc: Option[SparkContext] = None

  private def sc: SparkContext = {
    if (_sc.isEmpty) {
      println("Creating SparkContext")

      val sc = SparkUtils.createLocalSparkContext(
        "local[*]",
        appName = classOf[FileLayerProviderTest].getName
      )
      _sc = Some(sc)
    }
    _sc.get
  }

  @BeforeClass
  def setUpSpark_BeforeClass(): Unit = sc

  @BeforeAll
  def setUpSpark_BeforeAll(): Unit = sc

  var gotAfterAll = false

  @AfterAll
  def tearDownSpark_AfterAll(): Unit = {
    gotAfterAll = true
    maybeStopSpark()
  }

  var gotAfterClass = false

  @AfterClass
  def tearDownSpark_AfterClass(): Unit = {
    gotAfterClass = true;
    maybeStopSpark()
  }

  def maybeStopSpark(): Unit = {
    if (gotAfterAll && gotAfterClass) {
      if (_sc.isDefined) {
        println("Stopping SparkContext...")
        _sc.get.stop()
        _sc = None
        println("Stopped SparkContext")
      }
    }
  }
}

@Ignore("2023-05-02, Emile: Activate again when used service works again.")
class FileLayerProviderTest {
  import FileLayerProviderTest._

  private def sentinel5PMaxSpatialResolution = CellSize(0.05, 0.05)
  private def sentinel5PLayoutScheme = ZoomedLayoutScheme(LatLng)
  private def sentinel5PCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1"
  private def sentinel5PFileLayerProvider = new FileLayerProvider(
    openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue")),
    openSearchCollectionId = sentinel5PCollectionId,
    NonEmptyList.one("NO2"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
    maxSpatialResolution = sentinel5PMaxSpatialResolution,
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = sentinel5PLayoutScheme
  )

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def cache(): Unit = {
    // important: multiple instances like in openeo-geopyspark-driver
    val layerProvider1 = sentinel5PFileLayerProvider
    val layerProvider2 = sentinel5PFileLayerProvider

    assertNotSame(layerProvider1, layerProvider2)

    val metadataCall1 = layerProvider1.loadMetadata(sc = null)
    val metadataCall2 = layerProvider2.loadMetadata(sc = null)
    assertSame(metadataCall1, metadataCall2)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def smallBoundingBox(): Unit = {
    val smallBbox = ProjectedExtent(Point(x = 4.9754, y = 50.3244).buffer(0.001).extent, LatLng)

    assertTrue(smallBbox.extent.width < 0.05,s"${smallBbox.extent.width}")
    assertTrue(smallBbox.extent.height < 0.05, s"${smallBbox.extent.height}")

    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))

    val baseLayer = sentinel5PFileLayerProvider.readTileLayer(from = date, to = date, smallBbox, sc = sc)

    val Summary(singleBandMean) = baseLayer
      .toSpatial(date)
      .polygonalSummaryValue(smallBbox.extent.toPolygon(), MeanVisitor)

    val physicalMean = (singleBandMean.mean * 5).toInt

    assertEquals(25, physicalMean)
  }


  private def _getSentinel5PRasterSources(bbox: ProjectedExtent, date: ZonedDateTime, zoom: Int): (RDD[LayoutTileSource[SpaceTimeKey]], TileLayerMetadata[SpaceTimeKey]) = {
    val fileLayerProvider = sentinel5PFileLayerProvider

    val overlappingRasterSources: Seq[RasterSource] = fileLayerProvider.loadRasterSourceRDD(bbox, date, date, zoom).map(_._1)
    val commonCellType = overlappingRasterSources.head.cellType
    val metadata = layerMetadata(bbox, date, date, zoom min zoom, commonCellType, sentinel5PLayoutScheme, sentinel5PMaxSpatialResolution)

    val rasterSources = rasterSourceRDD(overlappingRasterSources, metadata, sentinel5PMaxSpatialResolution, sentinel5PCollectionId)(sc)
    (rasterSources, metadata)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def sparsePartitionerTest(): Unit = {
    val bbox1 = ProjectedExtent(Extent(xmin = 0.0, ymin = 0.0, xmax = 30.0, ymax = 10.0), LatLng)
    val bbox2 = ProjectedExtent(Extent(xmin = 50.0, ymin = 20.0, xmax = 60.0, ymax = 40.0), LatLng)
    val fullBbox = ProjectedExtent(bbox1.extent.combine(bbox2.extent), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))

    val (rasterSources, metadata) = _getSentinel5PRasterSources(fullBbox, date, 10)
    val polygons = Array(MultiPolygon(bbox1.extent.toPolygon(), bbox2.extent.toPolygon()))
    val polygons_crs = fullBbox.crs

    // Create the sparse Partitioner.
    val sparseBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc, retainNoDataTiles = false, NoCloudFilterStrategy)
    val sparsePartitioner: SpacePartitioner[SpaceTimeKey] = sparseBaseLayer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]
    assert(sparsePartitioner.index.getClass == classOf[SparseSpaceTimePartitioner])
    val sparsePartitionerIndex = sparsePartitioner.index.asInstanceOf[SparseSpaceTimePartitioner]

    // Create the default Space Partitioner.
    val defaultBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc, retainNoDataTiles = false, NoCloudFilterStrategy, useSparsePartitioner=false)
    val defaultPartitioner: SpacePartitioner[SpaceTimeKey] = defaultBaseLayer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]
    assert(defaultPartitioner.index == SpaceTimeByMonthPartitioner)

    assert(sparseBaseLayer.getNumPartitions <= defaultBaseLayer.getNumPartitions)

    val requiredKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons).map {
      _.reproject(polygons_crs, metadata.crs)
    }.clipToGrid(metadata.layout).groupByKey()

    val filteredSources: RDD[LayoutTileSource[SpaceTimeKey]] = rasterSources.filter({ tiledLayoutSource =>
      tiledLayoutSource.source.extent.interiorIntersects(tiledLayoutSource.layout.extent)
    })

    val requiredSpacetimeKeys: RDD[SpaceTimeKey] = filteredSources.flatMap(_.keys).map {
      tuple => (tuple.spatialKey, tuple)
    }.rightOuterJoin(requiredKeys).flatMap(_._2._1.toList)

    // Ensure that the sparsePartitioner only creates partitions for the required spacetime regions.
    val requiredRegions = requiredSpacetimeKeys.map(k => sparsePartitionerIndex.toIndex(k))
    assert(requiredRegions.distinct.collect().sorted sameElements sparsePartitioner.regions.sorted)

    // Even though both RDDs have a different number of partitions, the keys for both RDDs are the same.
    // This means that the default partitioner has many empty partitions that have no source.
    val sparseKeys = sparseBaseLayer.keys.collect().sorted
    val defaultKeys = defaultBaseLayer.keys.collect().sorted
    assert(sparseKeys sameElements defaultKeys)

    // Keys corresponding with NoDataTiles are removed from the final RDD.
    // Which means those few partitions will still be empty.
    val partitionKeys = requiredSpacetimeKeys.collect().sorted.toSet
    assert(sparseKeys.toSet.subsetOf(partitionKeys))
    assert(defaultKeys.toSet.subsetOf(partitionKeys))

    // Ensure that the regions in sparsePartitioner are a subset of the default Partitioner.
    sparsePartitioner.regions.toSet.subsetOf(defaultPartitioner.regions.toSet)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def sparsePartitionerMergeTest(): Unit = {
    val zoom = 6
    // Create the first RDD.
    val bbox1 = ProjectedExtent(Extent(xmin = 55.0, ymin = 20.0, xmax = 60.0, ymax = 25.0), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))
    val polygons1 = MultiPolygon(bbox1.extent.toPolygon())
    val (rasterSources1, metadata1) = _getSentinel5PRasterSources(bbox1, date, zoom)
    val sparseBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources1, metadata1, Array(polygons1),
                                                                   bbox1.crs, sc, retainNoDataTiles = false,
                                                                   NoCloudFilterStrategy)
    val defaultBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources1, metadata1, Array(polygons1),
                                                                    bbox1.crs, sc, retainNoDataTiles = false,
                                                                    NoCloudFilterStrategy,
                                                                    useSparsePartitioner = false)

    // Create the second RDD.
    val bbox2 = ProjectedExtent(Extent(xmin = 58.0, ymin = 20.0, xmax = 62.0, ymax = 25.0), LatLng)
    val polygons2 = MultiPolygon(bbox2.extent.toPolygon())
    val (rasterSources2, metadata2) = _getSentinel5PRasterSources(bbox1, date, zoom)
    val sparseBaseLayer2 = FileLayerProvider.readMultibandTileLayer(rasterSources2, metadata2, Array(polygons2),
                                                                    bbox2.crs, sc, retainNoDataTiles = false,
                                                                    NoCloudFilterStrategy)
    val defaultBaseLayer2 = FileLayerProvider.readMultibandTileLayer(rasterSources2, metadata2, Array(polygons2),
                                                                     bbox2.crs, sc, retainNoDataTiles = false,
                                                                     NoCloudFilterStrategy,
                                                                     useSparsePartitioner = false)

    // Merge both RDDs.
    val defaultMergedLayer = defaultBaseLayer.merge(defaultBaseLayer2)
    val defaultMergedLayerKeys = defaultMergedLayer.keys.collect().toSet
    val sparseMergedLayer = sparseBaseLayer.merge(sparseBaseLayer2)
    val sparseMergedLayerKeys = sparseMergedLayer.keys.collect().toSet

    assert(defaultMergedLayerKeys.nonEmpty)
    assertEquals(defaultMergedLayerKeys, sparseMergedLayerKeys)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def sparsePartitionerMaskTest(): Unit = {
    // Create the base layers.
    val bbox = ProjectedExtent(Extent(xmin = 55.0, ymin = 30.0, xmax = 60.0, ymax = 35.0), LatLng)
    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))
    val polygons = MultiPolygon(bbox.extent.toPolygon())
    val (rasterSources, metadata) = _getSentinel5PRasterSources(bbox, date, 8)
    val sparseBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, Array(polygons),
                                                                   bbox.crs, sc, retainNoDataTiles = false,
                                                                   NoCloudFilterStrategy)
    val defaultBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, Array(polygons),
                                                                    bbox.crs, sc, retainNoDataTiles = false,
                                                                    NoCloudFilterStrategy,
                                                                    useSparsePartitioner = false)

    // Create the masked layers.
    val maskBbox = ProjectedExtent(Extent(xmin = 57.0, ymin = 30.0, xmax = 58.0, ymax = 35.0), LatLng)
    val maskPolygons = MultiPolygon(maskBbox.extent.toPolygon())
    val defaultMaskedLayer = defaultBaseLayer.mask(maskPolygons)
    val sparseMaskedLayer = sparseBaseLayer.mask(maskPolygons)

    val defaultMaskedLayerKeys = defaultMaskedLayer.keys.collect().toSet
    val sparseMaskedLayerKeys = sparseMaskedLayer.keys.collect().toSet

    assert(defaultMaskedLayerKeys.nonEmpty)
    assertEquals(defaultMaskedLayerKeys, sparseMaskedLayerKeys)
  }



  @ParameterizedTest
  @ValueSource(ints = Array(101,489,1589,69854))
  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testOptimalLayoutScheme(size:Int): Unit = {

    val crs = CRS.fromEpsgCode(32632)
    val x = 344110.000
    val y = 5600770.000
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(x, y, x+size*10, y+size*10), crs)
    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    val scheme = LayerFixtures.sentinel2TocLayerProviderUTM20M.selectLayoutScheme(boundingBox,false,Some(dataCubeParameters))
    assertTrue(scheme.isInstanceOf[FloatingLayoutScheme])
    val expected = size match {
      case 69854 => 512 // 1024 if experimental flag set
      case 1589 => 512
      case _ => 256
    }
    assertEquals(expected,scheme.asInstanceOf[FloatingLayoutScheme].tileRows)

  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def overlapsFilterTest(): Unit = {
    val date = LocalDate.of(2022, 7, 1).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32632)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(344110.000, 5600770.000, 351420.000, 5608770.000), crs)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)

    val result = LayerFixtures.sentinel2TocLayerProviderUTM20M.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )
    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / 10.0)/256.0)
    val rows = math.ceil((boundingBox.extent.height / 10.0)/256.0)

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)

    val ids = result._1.values.map(_.data._2.id).distinct().collect()
    //overlap filter has removed the other potential sources
    assertEquals(1,ids.length)
    assertEquals("urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20220701T103629_32ULB_TOC_V210",ids(0))
    assertEquals(cols*rows,result._1.count(),0.1)
  }

  val myFeatureJSON =
    """
      |{
      | "totalResults": 1,
      |    "startIndex": 1,
      |    "itemsPerPage": 1,
      |  "features": [{
      |            "type": "Feature",
      |            "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110",
      |            "geometry": {"coordinates":[[[4.995008,49.509308],[5.473188,51.003036],[1.742552,51.41433],[1.379708,49.918747],[4.995008,49.509308]]],"type":"Polygon"},
      |            "bbox": [1.379708,49.509308,5.473188,51.41433],
      |            "properties":
      |            	{"date":"2020-03-15T05:58:49.458Z","identifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110","available":"2020-09-09T14:07:35Z","parentIdentifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1","productInformation":{"processingCenter":"VITO","productVersion":"V110","timeliness":"Fast-24h","processingDate":"2020-03-15T10:23:40.698Z","productType":"SIGMA0","availabilityTime":"2020-09-09T14:07:35Z"},"links":{"related":[],"data":[{"length":1642877038,"type":"image/tiff","title":"VH","href":"https://services.terrascope.be/download/CGS_S1_GRD_SIGMA0_L1/2020/03/15/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110_VH.tif"},{"length":1638893250,"type":"image/tiff","title":"VV","href":"https://services.terrascope.be/download/CGS_S1_GRD_SIGMA0_L1/2020/03/15/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110_VV.tif"},{"length":105791005,"type":"image/tiff","title":"angle","href":"https://services.terrascope.be/download/CGS_S1_GRD_SIGMA0_L1/2020/03/15/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110_angle.tif"}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_GRD_SIGMA0&TIME=2020-03-15&BBOX=153588.3920034059,6361726.342578137,609272.5011758554,6694913.752846391&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","category":"QUICKLOOK"}],"alternates":[{"length":38284,"type":"application/vnd.iso.19139+xml","title":"Inspire metadata","href":"https://services.terrascope.be/download/CGS_S1_GRD_SIGMA0_L1/2020/03/15/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110/S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110.xml"}]},"published":"2020-09-09T14:07:35Z","title":"S1A_IW_GRDH_SIGMA0_DV_20200315T055849_DESCENDING_110_22F3_V110","updated":"2020-03-15T10:23:40.698Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":110,"polarisationChannels":"VV, VH","beginningDateTime":"2020-03-15T05:58:49.458Z","orbitDirection":"DESCENDING","endingDateTime":"2020-03-15T05:59:14.456Z","orbitNumber":31682},"platform":{"platformShortName":"SENTINEL-1","platformSerialIdentifier":"S1A"}}],"status":"ARCHIVED"}
      |         }]}""".stripMargin

  val sentinel1Product =  FeatureCollection.parse(myFeatureJSON, isUTM = true)

  class MockOpenSearch extends OpenSearchClient {
    override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
      val start = dateRange.get._1
      sentinel1Product.features
    }
    override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???
    override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  }

  class MockOpenSearchFeatures(mockedFeatures:Array[OpenSearchResponses.Feature]) extends OpenSearchClient {
    override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
      mockedFeatures
    }

    override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???

    override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  }

  val myCreoFeatureJSON =
    """
      {
      |  "type": "FeatureCollection",
      |  "properties": {
      |    "id": "35d62b97-2c19-56f8-bac1-bd5135ea044c",
      |    "totalResults": 2,
      |    "exactCount": true,
      |    "startIndex": 1,
      |    "itemsPerPage": 100,
      |
      |    "links": [
      |      {
      |        "rel": "self",
      |        "type": "application/json",
      |        "title": "self",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?&box=-5.501993509841079%2C41.716232207553176%2C-5.2206261514227466%2C41.92935559222629&sortParam=startDate&sortOrder=ascending&page=1&maxRecords=100&status=0%7C34%7C37&dataset=ESA-DATASET&productType=L2A&startDate=2021-04-01T00%3A00%3A00Z&completionDate=2021-10-31T23%3A59%3A59.999999999Z"
      |      },
      |      {
      |        "rel": "search",
      |        "type": "application/opensearchdescription+xml",
      |        "title": "OpenSearch Description Document",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel2/describe.xml"
      |      },
      |      {
      |        "rel": "next",
      |        "type": "application/json",
      |        "title": "next",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?&box=-5.501993509841079%2C41.716232207553176%2C-5.2206261514227466%2C41.92935559222629&sortParam=startDate&sortOrder=ascending&page=2&maxRecords=100&status=0%7C34%7C37&dataset=ESA-DATASET&productType=L2A&startDate=2021-04-01T00%3A00%3A00Z&completionDate=2021-10-31T23%3A59%3A59.999999999Z"
      |      },
      |      {
      |        "rel": "last",
      |        "type": "application/json",
      |        "title": "last",
      |        "href": "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?&box=-5.501993509841079%2C41.716232207553176%2C-5.2206261514227466%2C41.92935559222629&sortParam=startDate&sortOrder=ascending&page=3&maxRecords=100&status=0%7C34%7C37&dataset=ESA-DATASET&productType=L2A&startDate=2021-04-01T00%3A00%3A00Z&completionDate=2021-10-31T23%3A59%3A59.999999999Z"
      |      }
      |    ]
      |  },
      |  "features": [
      |    {
      |      "type": "Feature",
      |      "id": "4a5f1c4b-494b-5f8f-a170-ac8d769e5cfb",
      |      "geometry": {
      |        "type": "Polygon",
      |        "coordinates": [
      |          [
      |            [
      |              -6.597992,
      |              41.562119054
      |            ],
      |            [
      |              -6.578247,
      |              41.626124845
      |            ],
      |            [
      |              -6.5322266,
      |              41.773028035
      |            ],
      |            [
      |              -6.4852905,
      |              41.919850791
      |            ],
      |            [
      |              -6.4388733,
      |              42.066956737
      |            ],
      |            [
      |              -6.390991,
      |              42.213715835
      |            ],
      |            [
      |              -6.3444214,
      |              42.360855787
      |            ],
      |            [
      |              -6.331024,
      |              42.402868296
      |            ],
      |            [
      |              -5.3124084,
      |              42.429360872
      |            ],
      |            [
      |              -5.2769775,
      |              41.441212167
      |            ],
      |            [
      |              -6.589264,
      |              41.40772619
      |            ],
      |            [
      |              -6.597992,
      |              41.562119054
      |            ]
      |          ]
      |        ]
      |      },
      |      "properties": {
      |        "collection": "Sentinel2",
      |        "status": 0,
      |        "license": {
      |          "licenseId": "unlicensed",
      |          "hasToBeSigned": "never",
      |          "grantedCountries": null,
      |          "grantedOrganizationCountries": null,
      |          "grantedFlags": null,
      |          "viewService": "public",
      |          "signatureQuota": -1,
      |          "description": {
      |            "shortName": "No license"
      |          }
      |        },
      |        "productIdentifier": "/eodata/Sentinel-2/MSI/L2A/2021/04/01/S2A_MSIL2A_20210401T110621_N0300_R137_T30TTM_20210401T141035.SAFE",
      |        "parentIdentifier": null,
      |        "title": "S2A_MSIL2A_20210401T110621_N0300_R137_T30TTM_20210401T141035.SAFE",
      |        "description": null,
      |        "organisationName": "ESA",
      |        "startDate": "2021-04-01T11:06:21.024Z",
      |        "completionDate": "2021-04-01T11:06:21.024Z",
      |        "productType": "L2A",
      |        "processingLevel": "LEVEL2A",
      |        "platform": "S2A",
      |        "instrument": "MSI",
      |        "resolution": 60,
      |        "sensorMode": "",
      |        "orbitNumber": 30164,
      |        "quicklook": null,
      |        "thumbnail": "https://finder.creodias.eu/files/Sentinel-2/MSI/L2A/2021/04/01/S2A_MSIL2A_20210401T110621_N0300_R137_T30TTM_20210401T141035.SAFE/S2A_MSIL2A_20210401T110621_N0300_R137_T30TTM_20210401T141035-ql.jpg",
      |        "updated": "2021-04-01T18:49:15.903195Z",
      |        "published": "2021-04-01T18:49:15.903195Z",
      |        "snowCover": 0,
      |        "cloudCover": 69.467697,
      |        "gmlgeometry": "<gml:Polygon srsName=\"EPSG:4326\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>-6.597992,41.562119054439 -6.578247,41.626124844758 -6.5322266,41.773028035388 -6.4852905,41.919850790636 -6.4388733,42.06695673689 -6.390991,42.213715835021 -6.3444214,42.360855786794 -6.331024,42.402868295522 -5.3124084,42.429360872359 -5.2769775,41.441212166986 -6.589264,41.407726190111 -6.597992,41.562119054439</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>",
      |        "centroid": {
      |          "type": "Point",
      |          "coordinates": [
      |            -5.892901344,
      |            41.897220387
      |          ]
      |        },
      |        "orbitDirection": "descending",
      |        "timeliness": null,
      |        "relativeOrbitNumber": 137,
      |        "processingBaseline": 3,
      |        "missionTakeId": "GS2A_20210401T110621_030164_N03.00",
      |        "services": {
      |          "download": {
      |            "url": "https://zipper.creodias.eu/download/4a5f1c4b-494b-5f8f-a170-ac8d769e5cfb",
      |            "mimeType": "application/unknown",
      |            "size": 932073644
      |          }
      |        },
      |        "links": [
      |          {
      |            "rel": "self",
      |            "type": "application/json",
      |            "title": "GeoJSON link for 4a5f1c4b-494b-5f8f-a170-ac8d769e5cfb",
      |            "href": "https://finder.creodias.eu/resto/collections/Sentinel2/4a5f1c4b-494b-5f8f-a170-ac8d769e5cfb.json?&lang=en"
      |          }
      |        ]
      |      }
      |    },
      |    {
      |      "type": "Feature",
      |      "id": "ee728e84-04ad-5705-bee0-5de3c1059e1f",
      |      "geometry": {
      |        "type": "Polygon",
      |        "coordinates": [
      |          [
      |            [
      |              -6.6024475,
      |              41.547733882
      |            ],
      |            [
      |              -6.578247,
      |              41.626124845
      |            ],
      |            [
      |              -6.5322266,
      |              41.773028035
      |            ],
      |            [
      |              -6.4852905,
      |              41.919850791
      |            ],
      |            [
      |              -6.4388733,
      |              42.066956737
      |            ],
      |            [
      |              -6.390991,
      |              42.213715835
      |            ],
      |            [
      |              -6.3444214,
      |              42.360855787
      |            ],
      |            [
      |              -6.32547,
      |              42.420318851
      |            ],
      |            [
      |              -5.2368774,
      |              42.390880662
      |            ],
      |            [
      |              -5.2944336,
      |              41.404034746
      |            ],
      |            [
      |              -6.606537,
      |              41.438846245
      |            ],
      |            [
      |              -6.6024475,
      |              41.547733882
      |            ]
      |          ]
      |        ]
      |      },
      |      "properties": {
      |        "collection": "Sentinel2",
      |        "status": 0,
      |        "license": {
      |          "licenseId": "unlicensed",
      |          "hasToBeSigned": "never",
      |          "grantedCountries": null,
      |          "grantedOrganizationCountries": null,
      |          "grantedFlags": null,
      |          "viewService": "public",
      |          "signatureQuota": -1,
      |          "description": {
      |            "shortName": "No license"
      |          }
      |        },
      |        "productIdentifier": "/eodata/Sentinel-2/MSI/L2A/2021/04/01/S2A_MSIL2A_20210401T110621_N0300_R137_T29TQG_20210401T141035.SAFE",
      |        "parentIdentifier": null,
      |        "title": "S2A_MSIL2A_20210401T110621_N0300_R137_T29TQG_20210401T141035.SAFE",
      |        "description": null,
      |        "organisationName": "ESA",
      |        "startDate": "2021-04-01T11:06:21.024Z",
      |        "completionDate": "2021-04-01T11:06:21.024Z",
      |        "productType": "L2A",
      |        "processingLevel": "LEVEL2A",
      |        "platform": "S2A",
      |        "instrument": "MSI",
      |        "resolution": 60,
      |        "sensorMode": "",
      |        "orbitNumber": 30164,
      |        "quicklook": null,
      |        "thumbnail": "https://finder.creodias.eu/files/Sentinel-2/MSI/L2A/2021/04/01/S2A_MSIL2A_20210401T110621_N0300_R137_T29TQG_20210401T141035.SAFE/S2A_MSIL2A_20210401T110621_N0300_R137_T29TQG_20210401T141035-ql.jpg",
      |        "updated": "2021-04-01T18:47:18.613604Z",
      |        "published": "2021-04-01T18:47:18.613604Z",
      |        "snowCover": 0,
      |        "cloudCover": 70.376396,
      |        "gmlgeometry": "<gml:Polygon srsName=\"EPSG:4326\"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>-6.6024475,41.547733882318 -6.578247,41.626124844758 -6.5322266,41.773028035388 -6.4852905,41.919850790636 -6.4388733,42.06695673689 -6.390991,42.213715835021 -6.3444214,42.360855786794 -6.32547,42.420318851084 -5.2368774,42.390880662389 -5.2944336,41.40403474612 -6.606537,41.438846244795 -6.6024475,41.547733882318</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>",
      |        "centroid": {
      |          "type": "Point",
      |          "coordinates": [
      |            -5.875944976,
      |            41.897187921
      |          ]
      |        },
      |        "orbitDirection": "descending",
      |        "timeliness": null,
      |        "relativeOrbitNumber": 137,
      |        "processingBaseline": 3,
      |        "missionTakeId": "GS2A_20210401T110621_030164_N03.00",
      |        "services": {
      |          "download": {
      |            "url": "https://zipper.creodias.eu/download/ee728e84-04ad-5705-bee0-5de3c1059e1f",
      |            "mimeType": "application/unknown",
      |            "size": 943720828
      |          }
      |        },
      |        "links": [
      |          {
      |            "rel": "self",
      |            "type": "application/json",
      |            "title": "GeoJSON link for ee728e84-04ad-5705-bee0-5de3c1059e1f",
      |            "href": "https://finder.creodias.eu/resto/collections/Sentinel2/ee728e84-04ad-5705-bee0-5de3c1059e1f.json?&lang=en"
      |          }
      |        ]
      |      }
      |    }]}""".stripMargin

  private lazy val creoS2Products =  CreoFeatureCollection.parse(myCreoFeatureJSON)

  class MockCreoOpenSearch extends OpenSearchClient {
    override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
      val start = dateRange.get._1
      creoS2Products.features
    }
    override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, startIndex: Int): OpenSearchResponses.FeatureCollection = ???
    override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testEdgeOfLargeFootPrint():Unit = {



    //val layout = LayoutDefinition(Extent(505110.0, 5676980.0, 515350.0, 5682100.0),TileLayout(1024,512,256,256))
    val date = LocalDate.of(2020, 3, 15).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(505110.0, 5676980.0, 515350.0, 5682100.0),crs )

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)

    val flp = new FileLayerProvider(
      new MockOpenSearch(),
      "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/bogus",
      CellSize(10.0,10.0),
      SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    ){
      //avoids having to actually read the product
      override def determineCelltype(overlappingRasterSources: Seq[(RasterSource, OpenSearchResponses.Feature)]): CellType = FloatConstantNoDataCellType
    }

    val result = flp.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / 10.0)/256.0)
    val rows = math.ceil((boundingBox.extent.height / 10.0)/256.0)

    val cube = result._1
    //val ids = cube.values.map(_.data._2.id).distinct().collect()
    //val count = cube.count()
    val all = cube.collect()

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)
    assertEquals(8,all.length)
    assertEquals((cols*rows).toInt,all.length)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testEdgeOfLargeFootPrintLatLon():Unit = {


    //val layout = LayoutDefinition(Extent(505110.0, 5676980.0, 515350.0, 5682100.0),TileLayout(1024,512,256,256))
    val date = LocalDate.of(2020, 3, 15).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(505110.0, 5676980.0, 515350.0, 5682100.0).reproject(crs,LatLng),LatLng )

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)

    val res = 0.0001
    val flp = new FileLayerProvider(
      new MockOpenSearch(),
      "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/bogus",
      CellSize(res,res),
      SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    ){
      //avoids having to actually read the product
      override def determineCelltype(overlappingRasterSources: Seq[(RasterSource, OpenSearchResponses.Feature)]): CellType = FloatConstantNoDataCellType
    }

    val result = flp.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = LatLng,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / res)/256.0)
    val rows = math.ceil((boundingBox.extent.height / res)/256.0)

    val cube = result._1
    //val ids = cube.values.map(_.data._2.id).distinct().collect()
    //val count = cube.count()
    val all = cube.collect()

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(LatLng,result._2.crs)
    assertEquals(12,all.length)
    assertEquals((cols*rows).toInt,all.length)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testBufferingOnTheEdge():Unit = {

    //val layout = LayoutDefinition(Extent(505110.0, 5676980.0, 515350.0, 5682100.0),TileLayout(1024,512,256,256))
    val date = LocalDate.of(2020, 3, 15).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(505110.0, 5676980.0, 515350.0, 5682100.0),crs )

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)
    val buffer = 100
    dataCubeParameters.pixelBufferY = buffer
    dataCubeParameters.pixelBufferX = buffer

    val flp = new FileLayerProvider(
      new MockOpenSearch(),
      "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/bogus",
      CellSize(10.0,10.0),
      SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    ){
      //avoids having to actually read the product
      override def determineCelltype(overlappingRasterSources: Seq[(RasterSource, OpenSearchResponses.Feature)]): CellType = FloatConstantNoDataCellType
    }

    val result = flp.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil(((boundingBox.extent.width + 20.0*buffer) / 10.0)/256.0)
    val rows = math.ceil(((boundingBox.extent.height + 20.0*buffer) / 10.0)/256.0)

    val cube = result._1
    //val ids = cube.values.map(_.data._2.id).distinct().collect()
    //val count = cube.count()
    val all = cube.collect()

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)
    assertEquals(12,all.length)
    //assertEquals((cols*rows).toInt,all.length)

    assertEquals(505110.0 - 1000.0, result._2.extent.xmin,0.01)
    assertEquals(515350.0 + buffer*10.0, result._2.extent.xmax,0.01)
    assertEquals(5676980.0 - 1000.0, result._2.extent.ymin,0.01)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def sentinel1LoadTest(): Unit = {
    LayerFixtures.sentinel1Sigma0LayerProviderUTM

    val date = LocalDate.of(2022, 9, 13).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(5.085980189812683,51.0353667302808,5.146073667675196,51.05305736567695).reproject(LatLng,crs),crs )

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)

    val result = LayerFixtures.sentinel1Sigma0LayerProviderUTM.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / 10.0)/256.0)
    val rows = math.ceil((boundingBox.extent.height / 10.0)/256.0)

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)

    val cube = result._1
    val ids = cube.values.map(_.data._2.id).distinct().collect()
    val count = cube.count()
    //overlap filter has removed the other potential sources
    assertEquals(2,ids.length)
    assertEquals("urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110",ids(0))
    assertEquals("urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110",ids(1))
    //the cube only covers 2 tiles, but we have 2 source products, so times 2
    assertEquals(2*cols*rows,count,0.1)
    println(s"Count: $count")
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testSinglePoint(): Unit = {
    val date = LocalDate.of(2019, 9, 25).atStartOfDay(UTC)
    val endDate = LocalDate.of(2019, 9, 30).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(32631)
    // a mix of 31UGS and 32ULB

//    val boundingBox = ProjectedExtent(Extent(481100.0, 5663200.0, 481100.0, 5663200.0), crs) // TODO: This would cause a crash
    val boundingBox = ProjectedExtent(Extent(2.7355, 51.1281, 2.7355, 51.1281).reproject(LatLng,crs), crs)

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.globalExtent = Some(boundingBox)

    val result = LayerFixtures.sentinel2TocLayerProviderUTM20M.readKeysToRasterSources(
      from = date,
      to = endDate,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )
    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / 10.0)/256.0)
    val rows = math.ceil((boundingBox.extent.height / 10.0)/256.0)

    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)
  }

  @Ignore("2023-05-02, Emile: Activate again when used service works again.")
  def testCreoNonNativeProjection():Unit = {


    val date = LocalDate.of(2021, 4, 6).atStartOfDay(UTC)

    val crs = CRS.fromEpsgCode(3035)
    // a mix of 31UGS and 32ULB
    val boundingBox = ProjectedExtent(Extent(3040003.0,2180000.0,3060000.0,2200000.0),crs )

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    dataCubeParameters.tileSize = 1024

    dataCubeParameters.globalExtent = Some(boundingBox)

    val flp = new FileLayerProvider(
      new MockCreoOpenSearch(),
      "Sentinel2",
      openSearchLinkTitles = NonEmptyList.of("B04"),
      rootPath = "/bogus",
      CellSize(10.0,10.0),
      SplitYearMonthDayPathDateExtractor,
      layoutScheme = FloatingLayoutScheme(1024),
      experimental = false,
      attributeValues = Map("productType"->"L2A")
    ){
      //avoids having to actually read the product
      override def determineCelltype(overlappingRasterSources: Seq[(RasterSource, OpenSearchResponses.Feature)]): CellType = FloatConstantNoDataCellType
    }

    val result = flp.readKeysToRasterSources(
      from = date,
      to = date,
      boundingBox,
      polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
      polygons_crs = crs,
      zoom = 0,
      sc,
      Some(dataCubeParameters)
    )

    val minKey = result._2.bounds.get.minKey

    val cols = math.ceil((boundingBox.extent.width / 10.0)/1024.0)
    val rows = math.ceil((boundingBox.extent.height / 10.0)/1024.0)

    val cube = result._1
    //val ids = cube.values.map(_.data._2.id).distinct().collect()
    //val count = cube.count()
    val all = cube.collect()

    val tileSources = all.map(t=>(t._1,LayoutTileSource.spatial(t._2.data._1,result._2.layout).rasterRegionForKey(t._1.spatialKey).get))
    val minKeySource = tileSources.toMap.get(minKey).get
    assertEquals(3040003.0,minKeySource.extent.xmin,0.1)
    //the test should reach this point without requiring access to the actual files. If it fails because of not having creo mounts, something is wrong.
    assertEquals(0,minKey.col)
    assertEquals(0,minKey.row)
    assertEquals(crs,result._2.crs)
    assertEquals(8,all.length)
    assertEquals((2*cols*rows).toInt,all.length)
  }

  @Test
  def testPixelValueOffsetNeeded(): Unit = {
    val bandNames = Collections.singletonList("B04")
    //    val extent4326 = Extent(51.62, 4.4, 51.63, 4.42)
    val extentTAP32631 = Extent(703109 - 100, 5600100, 709000, 5620000 - 100) // corner
//    val extentTAP32631 = Extent(610000 - 100, 5600100, 690101, 5690000 - 100) // safe option
    //    val extentTAP32631 = Extent(610000 - 100, 5600100, 690101, 5690000 - 100)
    //    val extentTAP32631 = extent4326.reproject(CRS.fromName("EPSG:4326" ), CRS.fromName("EPSG:32631" ))
    val srs32631 = "EPSG:32631"
    val projected_polygons_native_crs = ProjectedPolygons.fromExtent(extentTAP32631, srs32631)

    val resourcePath = "/org/openeo/geotrellis/"
    val fullPath = resourcePath + "Sentinel-2-L2A/creodiasPixelValueOffsetNeeded.json"

    val fileSource = Source.fromURL(getClass.getResource(fullPath))
    var txt = try fileSource.mkString
    finally fileSource.close()
    val file = getClass.getResource(fullPath)
    val basePath = new java.io.File(file.getFile).getParent
    // Use artifactory to avoid havy git repo
    val basePathArtifactory = "https://artifactory.vgt.vito.be/testdata-public/eodata/Sentinel-2/MSI/L2A/2023/04/05/"
    val rest = "S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/GRANULE/L2A_T31UFS_A040660_20230405T105026/IMG_DATA/R10m/T31UFS_20230405T105031_B04_10m.jp2"
    val jp2File = new File(basePath, rest)
    if (!jp2File.exists()) {
      println("Copy from artifactory to: " + jp2File)
      FileUtils.copyURLToFile(new URL(basePathArtifactory + rest), jp2File)
    }
    txt = txt.replace("\"./", "\"" + basePath + "/")
    val mockedFeatures = CreoFeatureCollection.parse(txt)
    val client = new MockOpenSearchFeatures(mockedFeatures.features)
//    val client = CreodiasClient()

    val factory = new org.openeo.geotrellis.file.PyramidFactory(
      client, "Sentinel2", bandNames,
      null,
      maxSpatialResolution = CellSize(10, 10),
    )
    factory.crs = projected_polygons_native_crs.crs

    val localFromDate = LocalDate.of(2023, 4, 4)
    val localToDate = LocalDate.of(2023, 4, 6)
    val ZonedFromDate = ZonedDateTime.of(localFromDate, java.time.LocalTime.MIDNIGHT, UTC)
    val zonedToDate = ZonedDateTime.of(localToDate, java.time.LocalTime.MIDNIGHT, UTC)

    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
    val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate

    val dataCubeParameters = new DataCubeParameters()
    dataCubeParameters.tileSize = 256
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
    //    dataCubeParameters.globalExtent = Some(projectedExtent)

    val cube: Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = factory.datacube_seq(
      projected_polygons_native_crs,
      from_date, to_date, Collections.emptyMap(), ""
    )
    val cubeSpatial = cube.head._2.toSpatial()
    val band = cubeSpatial.collect().array(0)._2.toArrayTile().band(0)
    cubeSpatial.writeGeoTiff("tmp/l2_offseteda.tiff")
//
//    assertEquals(751, band.get(0, 0), 1)
//    assertEquals(778, band.get(1, 1), 1)
//    assertEquals(370, band.get(100, 100), 1)
  }
}
