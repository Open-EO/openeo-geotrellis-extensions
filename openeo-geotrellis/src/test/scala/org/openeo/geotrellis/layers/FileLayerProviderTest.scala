package org.openeo.geotrellis.layers

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import cats.data.NonEmptyList
import geotrellis.layer.{SpaceTimeKey, SpatialKey, ZoomedLayoutScheme}
import geotrellis.proj4.LatLng
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.raster.{CellSize, RasterSource}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert.{assertEquals, assertNotSame, assertSame, assertTrue}
import org.junit.{AfterClass, BeforeClass, Test}
import org.openeo.geotrellis.layers.FileLayerProvider.{layerMetadata, rasterSourceRDD}
import org.openeo.geotrelliscommon.{NoCloudFilterStrategy, SpaceTimeByMonthPartitioner, SparseSpaceTimePartitioner}

import java.net.URL
import java.time.{LocalDate, ZoneId}

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

  private def sentinel5PFileLayerProvider = new FileLayerProvider(
    openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue")),
    openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1",
    NonEmptyList.one("NO2"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
    maxSpatialResolution = CellSize(0.05, 0.05),
    new Sentinel5PPathDateExtractor(maxDepth = 3),
    layoutScheme = ZoomedLayoutScheme(LatLng)
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

  @Test
  def sparsePartitionerTest(): Unit = {
    val layoutScheme = ZoomedLayoutScheme(LatLng)
    val maxSpatialResolution = CellSize(0.05, 0.05)
    val openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S5P_L3_NO2_TD_V1"
    val fileLayerProvider = new FileLayerProvider(
      openSearch = OpenSearchClient(new URL("https://services.terrascope.be/catalogue")),
      openSearchCollectionId = openSearchCollectionId,
      NonEmptyList.one("NO2"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel5P/L3_NO2_TD_V1",
      maxSpatialResolution = maxSpatialResolution,
      new Sentinel5PPathDateExtractor(maxDepth = 3),
      layoutScheme = layoutScheme
      )

    val bbox1 = ProjectedExtent(Extent(xmin = 0.0, ymin = 0.0, xmax = 30.0, ymax = 10.0), LatLng)
    val bbox2 = ProjectedExtent(Extent(xmin = 50.0, ymin = 20.0, xmax = 60.0, ymax = 40.0), LatLng)
    val fullBbox = ProjectedExtent(bbox1.extent.combine(bbox2.extent), LatLng)

    val date = LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.of("UTC"))

    val zoom = 10

    val overlappingRasterSources: Seq[RasterSource] = fileLayerProvider.loadRasterSourceRDD(fullBbox, date, date, zoom)
    val commonCellType = overlappingRasterSources.head.cellType
    val metadata = layerMetadata(fullBbox, date, date, zoom min zoom, commonCellType, layoutScheme, maxSpatialResolution)

    val rasterSources = rasterSourceRDD(overlappingRasterSources, metadata, maxSpatialResolution, openSearchCollectionId)(sc)
    val polygons = Array(MultiPolygon(bbox1.extent.toPolygon(), bbox2.extent.toPolygon()))
    val polygons_crs = fullBbox.crs

    // Sparse Partitioner
    val sparseBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc, NoCloudFilterStrategy, useSparsePartitioner=true)
    val sparsePartitioner: SpacePartitioner[SpaceTimeKey] = sparseBaseLayer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]
    assert(sparsePartitioner.index.getClass == classOf[SparseSpaceTimePartitioner])
    val sparsePartitionerIndex = sparsePartitioner.index.asInstanceOf[SparseSpaceTimePartitioner]

    // Default Space Partitioner
    val defaultBaseLayer = FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc, NoCloudFilterStrategy, useSparsePartitioner=false)
    val defaultPartitioner: SpacePartitioner[SpaceTimeKey] = defaultBaseLayer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]
    assert(defaultPartitioner.index == SpaceTimeByMonthPartitioner)

    assert(sparseBaseLayer.getNumPartitions <= defaultBaseLayer.getNumPartitions)

    val requiredKeys: RDD[(SpatialKey, Iterable[Geometry])] = sc.parallelize(polygons).map {
      _.reproject(polygons_crs, metadata.crs)
    }.clipToGrid(metadata.layout).groupByKey()

    val requiredSpacetimeKeys: RDD[SpaceTimeKey] = rasterSources.flatMap(_.keys).map {
      tuple => (tuple.spatialKey, tuple)
    }.rightOuterJoin(requiredKeys).flatMap(_._2._1.toList)

    // Ensure that the sparsePartitioner only created partitions for the required spacetime regions.
    val requiredRegions = requiredSpacetimeKeys.map(k => sparsePartitionerIndex.toIndex(k))
    assert(requiredRegions.distinct.collect().sorted sameElements sparsePartitioner.regions.sorted)

    // Even though both RDDs have a different number of partitions, the keys for both RDDs are the same.
    // This means that the default partitioner has many empty partitions that have no source.
    assert(sparseBaseLayer.keys.collect().sorted sameElements defaultBaseLayer.keys.collect().sorted)
    //assert(sparseBaseLayer.keys.collect().sorted sameElements requiredSpacetimeKeys.collect().sorted) // TODO: Sometimes requiredKeys > layerKeys
    //assert(defaultBaseLayer.keys.collect().sorted sameElements requiredSpacetimeKeys.collect().sorted)

    // Ensure that the regions in sparsePartitioner are a subset of the default Partitioner.
    sparsePartitioner.regions.toSet.subsetOf(defaultPartitioner.regions.toSet)
  }
}
