package org.openeo.geotrellis

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import cats.data.NonEmptyList
import geotrellis.layer.{Bounds, FloatingLayoutScheme, KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.{ArrayMultibandTile, ArrayTile, ByteArrayTile, CellSize, MultibandTile, Tile, TileLayout}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.file.Sentinel2PyramidFactory
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrellisaccumulo.PyramidFactory
import org.openeo.geotrelliscommon.SparseSpaceTimePartitioner

import java.awt.image.DataBufferByte
import java.net.URL
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import java.util.Collections.singletonList
import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object LayerFixtures {

  def ClearNDVILayerForSingleDate()(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] ={

    val factory = new Sentinel2PyramidFactory(
      openSearchEndpoint = opensearchEndpoint,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_NDVI_V2",
      openSearchLinkTitles = singletonList("NDVI_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/NDVI_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
    val dateWithClearPostelArea = ZonedDateTime.of(LocalDate.of(2020, 5, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(5.176178620365679,51.24922676145928,5.258576081303179,51.27449711952613), LatLng)
    val layer = factory.layer(bbox, dateWithClearPostelArea, dateWithClearPostelArea, 11, correlationId = "")
    return layer
  }

  def buildSpatioTemporalDataCube(tiles: util.List[Tile], dates: Seq[String]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val mbTile = ArrayMultibandTile(tiles.asScala)
    val cubeXYB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, mbTile, new TileLayout(1, 1, tiles.get(0).cols.asInstanceOf[Integer], tiles.get(0).rows.asInstanceOf[Integer])).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val cubeXYTB: RDD[(SpaceTimeKey,MultibandTile)] = cubeXYB.flatMap((pair: Tuple2[SpatialKey, MultibandTile]) => {
      times.map((time: ZonedDateTime) => (SpaceTimeKey(pair._1, TemporalKey(time)), pair._2))
    })
    val md: TileLayerMetadata[SpatialKey] = cubeXYB.metadata
    val bounds: Bounds[SpatialKey] = md.bounds
    val minKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.minKey, TemporalKey(times.head))
    val maxKey: SpaceTimeKey = SpaceTimeKey.apply(bounds.get.maxKey, TemporalKey(times.last))
    val metadata: TileLayerMetadata[SpaceTimeKey] = new TileLayerMetadata[SpaceTimeKey](md.cellType, md.layout, md.extent, md.crs, new KeyBounds[SpaceTimeKey](minKey, maxKey))
    new ContextRDD(cubeXYTB, metadata)
  }

  def buildSingleBandSpatioTemporalDataCube(tiles: util.List[Tile], dates: Seq[String]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    implicit val sc = SparkContext.getOrCreate
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val layout = new TileLayout(1, 1, tiles.get(0).cols.asInstanceOf[Integer], tiles.get(0).rows.asInstanceOf[Integer])
    val cubeXYB: TileLayerRDD[SpaceTimeKey] = TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(JavaConverters.collectionAsScalaIterableConverter(tiles).asScala.zip(times),layout)

    cubeXYB.withContext{_.mapValues(MultibandTile(_)).repartitionAndSortWithinPartitions(new SpacePartitioner(cubeXYB.metadata.bounds))}
  }

  private[geotrellis] def tileToSpaceTimeDataCube(zeroTile: Tile): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val emptyTile = ArrayTile.empty(zeroTile.cellType, zeroTile.cols.asInstanceOf[Integer], zeroTile.rows.asInstanceOf[Integer])
    val minDate = "2017-01-01T00:00:00Z"
    val maxDate = "2018-01-15T00:00:00Z"
    val dates = Seq(minDate,"2017-01-15T00:00:00Z","2017-02-01T00:00:00Z",maxDate)
    val tiles = util.Arrays.asList(zeroTile, emptyTile)
    buildSpatioTemporalDataCube(tiles,dates)
  }

  private def accumuloPyramidFactory = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")

  def accumuloDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid: Seq[(Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]])] = accumuloPyramidFactory.pyramid_seq(layer, bbox, srs, minDateString, maxDateString)
    System.out.println("pyramid = " + pyramid)

    val (_, datacube) = pyramid.maxBy { case (zoom, _) => zoom }
    datacube
  }

  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor
  val opensearchEndpoint = "https://services.terrascope.be/catalogue"
  val client: OpenSearchClient = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"))

  def defaultExtent = Extent(xmin = 3.248235121238894, ymin = 50.9753557675801, xmax = 3.256396825072918, ymax = 50.98003212949561)

  def probav_ndvi(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent) = accumuloDataCube(
    layer = "PROBAV_L3_S10_TOC_NDVI_333M_V3",
    minDateString = from_date,
    maxDateString = to_date,
    bbox,
    srs = "EPSG:4326"
  )

  def s2_fapar(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent)=accumuloDataCube("S2_FAPAR_PYRAMID_20200408", from_date, to_date, bbox, "EPSG:4326")

  def sceneClassificationV200PyramidFactory = new Sentinel2PyramidFactory(
    openSearchEndpoint = opensearchEndpoint,
    openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
    openSearchLinkTitles = singletonList("SCENECLASSIFICATION_20M"),
    rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
    maxSpatialResolution = CellSize(10, 10)
  )

  def s2_scl(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent) = sceneClassificationV200PyramidFactory.layer(ProjectedExtent(defaultExtent,LatLng),ZonedDateTime.parse(from_date),ZonedDateTime.parse(to_date),12, correlationId = "")(SparkContext.getOrCreate())

  def sentinel2TocLayerProviderUTM =
    new FileLayerProvider(
      client,
      "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = true
    )

  lazy val b04RasterSource =GeoTiffRasterSource("https://artifactory.vgt.vito.be/testdata-public/S2_B04_timeseries.tiff")

  lazy val b04Raster = {
    b04RasterSource.read().get
  }

  def sentinel2B04Layer = {
    val tiles = b04Raster.tile.bands
    val timesteps = Array(0, 25, 35, 37, 55, 60, 67, 70, 80, 82, 85, 87, 90, 110, 112, 117, 122, 137, 140, 147, 152, 157, 160, 165, 167, 177, 180, 185, 190, 195, 210, 212, 215, 217, 222, 230, 232, 237, 240, 242, 265, 275, 280, 292, 302, 305, 312, 317, 325, 342, 350, 357, 360, 362, 367, 370, 372, 380, 382, 422, 425, 427, 430, 432, 435, 440, 442, 445, 447, 450, 452, 455, 457, 460, 462, 470, 472, 480, 482, 485, 490, 492, 495, 497, 515, 517, 520, 522, 532, 545, 547, 550, 552, 555, 557, 562, 565, 570, 572, 575, 587, 590, 600, 602, 605, 607, 610, 617, 637, 652, 667, 670, 697)
    val startDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val dates = timesteps.map(startDate.plusDays(_))

    val timeseries: Array[(SpaceTimeKey, MultibandTile)] = dates.zip(tiles).map({ date_tile => {
      (SpaceTimeKey(0, 0, date_tile._1), MultibandTile(date_tile._2.withNoData(Some(32767))))
    }
    })

    val rdd = SparkContext.getOrCreate().parallelize(timeseries)
    val layer = ContextRDD(rdd, TileLayerMetadata(timeseries(0)._2.cellType, LayoutDefinition(b04Raster.rasterExtent, tiles.head.cols,tiles.head.rows), b04Raster.extent, b04RasterSource.crs, KeyBounds[SpaceTimeKey](timeseries.head._1, timeseries.last._1)))
    new ContextRDD(layer,layer.metadata)
  }

  def rgbLayerProvider =
    new FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )

  def createLayerWithGaps(layoutCols:Int,layoutRows:Int) = {

    val intImage = createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val secondBand = imageTile.map { x => if (x >= 5) 10 else 100 }
    val thirdBand = imageTile.map { x => if (x >= 5) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, secondBand, thirdBand), TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
    print(tileLayerRDD.keys.collect())
    val filtered: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = tileLayerRDD.withContext {
      _.filter { case (key, tile) => (key.col > 0 && (key.col != 1 || key.row != 1)) }
    }
    (imageTile, filtered)
  }

  def aSpacetimeTileLayerRdd(layoutCols: Int, layoutRows: Int, nbDates:Int = 2) = {
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(layoutCols, layoutRows)
    val startDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val temporal: RDD[(SpaceTimeKey, MultibandTile)] = filtered.flatMap(tuple => {
      (1 to nbDates).map(index => (SpaceTimeKey(tuple._1, TemporalKey( startDate.plusDays(index) )), tuple._2))
    }).repartition(layoutCols * layoutRows)
    val spatialM = filtered.metadata
    val newBounds = KeyBounds[SpaceTimeKey](SpaceTimeKey(spatialM.bounds.get._1,TemporalKey(0L)),SpaceTimeKey(spatialM.bounds.get._2,TemporalKey(0L)))
    val temporalMetadata = new TileLayerMetadata[SpaceTimeKey](spatialM.cellType,spatialM.layout,spatialM.extent,spatialM.crs,newBounds)
    (ContextRDD(temporal,temporalMetadata),imageTile)
  }

  def aSparseSpacetimeTileLayerRdd(desiredKeys:Seq[SpatialKey] = Seq(SpatialKey(0,0),SpatialKey(3,1),SpatialKey(7,2))): MultibandTileLayerRDD[SpaceTimeKey] = {
    val collection = aSpacetimeTileLayerRdd(8,4,4)

    val allKeys = collection._1.map(_._1).filter(k => desiredKeys.contains(k.spatialKey)).collect().toArray

    val indices = allKeys.map(SparseSpaceTimePartitioner.toIndex(_,indexReduction = 0)).distinct.sorted.toArray
    val partitionerIndex = new SparseSpaceTimePartitioner(indices,0)
    val partitioner = SpacePartitioner(collection._1.metadata.bounds)(SpaceTimeKey.Boundable,ClassTag(classOf[SpaceTimeKey]), partitionerIndex)
    return collection._1.withContext{_.filter(t => desiredKeys.contains(t._1.spatialKey)).partitionBy(partitioner)}
  }

  def createTextImage(width:Int,height:Int, fontSize:Int = 500) = {
    import java.awt.Font
    import java.awt.image.BufferedImage

    val font = new Font("Arial", Font.PLAIN, fontSize)
    val text = "openEO"

    val img = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g2d = img.createGraphics

    g2d.setFont(font)
    val fm = g2d.getFontMetrics
    g2d.setColor(java.awt.Color.WHITE)
    g2d.translate(20,400)
    g2d.drawString(text, 0, fm.getAscent)
    g2d.dispose()

    img.getData().getDataBuffer().asInstanceOf[DataBufferByte].getData()


  }

}
