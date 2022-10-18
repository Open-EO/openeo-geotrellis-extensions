package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.layer.{LayoutTileSource, SpaceTimeKey, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.{CellSize, RasterSource, UByteUserDefinedNoDataCellType}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.{assertEquals, assertNotSame, assertSame, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.LayerFixtures
import org.openeo.geotrellis.layers.FileLayerProvider.rasterSourceRDD
import org.openeo.geotrelliscommon.DatacubeSupport._
import org.openeo.geotrelliscommon.{DataCubeParameters, NoCloudFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceTimePartitioner}
import org.openeo.opensearch.OpenSearchClient

import java.net.URL
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZoneId, ZonedDateTime}

object FileLayerProviderTest {
  private var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit =
    sc = SparkUtils.createLocalSparkContext("local[*]", appName = classOf[FileLayerProviderTest].getName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()
}

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

  @Test
  def cache(): Unit = {
    // important: multiple instances like in openeo-geopyspark-driver
    val layerProvider1 = sentinel5PFileLayerProvider
    val layerProvider2 = sentinel5PFileLayerProvider

    assertNotSame(layerProvider1, layerProvider2)

    val metadataCall1 = layerProvider1.loadMetadata(sc = null)
    val metadataCall2 = layerProvider2.loadMetadata(sc = null)
    assertSame(metadataCall1, metadataCall2)
  }

  @Test
  def smallBoundingBox(): Unit = {
    val smallBbox = ProjectedExtent(Point(x = 4.9754, y = 50.3244).buffer(0.001).extent, LatLng)

    assertTrue(s"${smallBbox.extent.width}", smallBbox.extent.width < 0.05)
    assertTrue(s"${smallBbox.extent.height}", smallBbox.extent.height < 0.05)

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

  @Test
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

  @Test
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

  @Test
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

  @Test
  def overlapsFilterTest(): Unit = {
    val date = LocalDate.of(2019, 6, 27).atStartOfDay(UTC)

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
    assertEquals("urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20190627T104029_32ULB_TOC_V200",ids(0))
    assertEquals(cols*rows,result._1.count(),0.1)
  }
}
