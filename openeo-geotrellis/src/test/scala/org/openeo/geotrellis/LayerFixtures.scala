package org.openeo.geotrellis

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.testkit.TileLayerRDDBuilders
import geotrellis.spark.testkit.TileLayerRDDBuilders.defaultCRS
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.layers.{FileLayerProvider, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrellisaccumulo
import org.openeo.geotrelliscommon.{DataCubeParameters, SparseSpaceTimePartitioner}
import org.openeo.opensearch.OpenSearchClient

import java.awt.image.DataBufferByte
import java.net.URL
import java.nio.file.Paths
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util
import java.util.Collections.singletonList
import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object LayerFixtures {

  def ClearNDVIPyramid(): file.PyramidFactory = {
    val openSearchClient = OpenSearchClient(new URL(opensearchEndpoint), isUTM = true)
    new org.openeo.geotrellis.file.PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_NDVI_V2",
      openSearchLinkTitles = singletonList("NDVI_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/NDVI_V2",
      maxSpatialResolution = CellSize(10, 10)
    )
  }

  def ClearNDVILayerForSingleDate()(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] ={
    val factory = ClearNDVIPyramid()
    val dateWithClearPostelArea = ZonedDateTime.of(LocalDate.of(2020, 5, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(5.176178620365679,51.24922676145928,5.258576081303179,51.27449711952613), LatLng)
    val layer = factory.layer(bbox, dateWithClearPostelArea, dateWithClearPostelArea, 11, correlationId = "")
    layer
  }

  def buildSpatioTemporalDataCube(tiles: java.util.List[_ <: Tile], dates: Seq[String], extent: Option[Extent] = None, tilingFactor:Int=1): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val mbTile = ArrayMultibandTile(tiles.asScala)
    val raster = Raster[MultibandTile](mbTile, extent.getOrElse(TileLayerRDDBuilders.defaultCRS.worldExtent))
    val tileLayout = new TileLayout(tilingFactor, tilingFactor, (raster.cols / tilingFactor).asInstanceOf[Integer], (raster.rows / tilingFactor).asInstanceOf[Integer])
    val cubeXYB: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] =
      TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, raster, tileLayout).asInstanceOf[ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]]]
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


  /**
   * Based on TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(...)
   * This returns an RDD on a single time
   */
  def createSpaceTimeMultibandTileLayerRDD(
                                            tiles: Traversable[MultibandTile],
                                            dataTime: ZonedDateTime,
                                            tileLayout: TileLayout,
                                            cellType: CellType = IntConstantNoDataCellType,
                                            extent: Extent = defaultCRS.worldExtent,
                                          )(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {

    val layout = LayoutDefinition(extent, tileLayout)
    val keyBounds = {
      val GridBounds(colMin, rowMin, colMax, rowMax) = layout.mapTransform(extent)
      KeyBounds(SpaceTimeKey(colMin, rowMin, dataTime), SpaceTimeKey(colMax, rowMax, dataTime))
    }
    val metadata = TileLayerMetadata(
      cellType,
      layout,
      extent,
      defaultCRS,
      keyBounds
    )

    val re = RasterExtent(
      extent = extent,
      cols = tileLayout.layoutCols,
      rows = tileLayout.layoutRows
    )

    val tileBounds = re.gridBoundsFor(extent)

    val tmsTiles = tileBounds.coordsIter.zip(tiles.toIterator).map {
      case ((col, row), tile) => (SpaceTimeKey(col, row, dataTime), tile)
    }

    new ContextRDD(sc.parallelize(tmsTiles.toSeq), metadata)
  }

  /**
   * Returns an RDD with tiles that switch between data and noData.
   * patternScale 2 gives [0 0 T T 0 0 T T] (where 0 is noData, and T is a data tile)
   */
  def buildSpatioTemporalDataCubePattern(tilingFactor: Int = 1, patternScale: Int = 1): MultibandTileLayerRDD[SpaceTimeKey] = {
    val horizontalTiles = 8
    val tilePixelSize = 16
    val tileLayout = new TileLayout(tilingFactor * horizontalTiles, tilingFactor, (tilePixelSize / tilingFactor), (tilePixelSize / tilingFactor))

    val rand = new scala.util.Random(42) // Fixed seed to make test predictable

    val tile1 = DoubleArrayTile.apply((1 to tilePixelSize * tilePixelSize).map(_ => 20 + 100 * rand.nextDouble).toArray, tilePixelSize, tilePixelSize)

    val mbt0 = new EmptyMultibandTile(tile1.cols, tile1.rows, tile1.cellType, 1)
    val mbt1 = ArrayMultibandTile(Array(tile1))

    val mbTiles = (0 until horizontalTiles).map(i => if ((i * 1.0 / patternScale).floor % 2 == 0) mbt0 else mbt1)

    assert(mbTiles.length == horizontalTiles)
    val dateTime = ZonedDateTime.parse("2019-01-01T00:00:00Z")

    implicit val sc: SparkContext = SparkContext.getOrCreate()
    val cubeXYTB = createSpaceTimeMultibandTileLayerRDD(
      mbTiles,
      dateTime,
      tileLayout,
      extent = LayerFixtures.defaultExtent,
    )
    new ContextRDD(cubeXYTB, cubeXYTB.metadata)
  }

  def buildSingleBandSpatioTemporalDataCube(tiles: java.util.List[Tile], dates: Seq[String]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    implicit val sc = SparkContext.getOrCreate
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val layout = new TileLayout(1, 1, tiles.get(0).cols.asInstanceOf[Integer], tiles.get(0).rows.asInstanceOf[Integer])
    val cubeXYB: TileLayerRDD[SpaceTimeKey] = TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(JavaConverters.collectionAsScalaIterableConverter(tiles).asScala.zip(times),layout)

    cubeXYB.withContext{_.mapValues(MultibandTile(_)).repartitionAndSortWithinPartitions(new SpacePartitioner(cubeXYB.metadata.bounds))}
  }

  private[geotrellis] def tileToSpaceTimeDataCube(zeroTile: Tile, extent: Option[Extent] = None, tilingFactor: Int = 1): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val emptyTile = ArrayTile.empty(zeroTile.cellType, zeroTile.cols.asInstanceOf[Integer], zeroTile.rows.asInstanceOf[Integer])
    val minDate = "2017-01-01T00:00:00Z"
    val maxDate = "2018-01-15T00:00:00Z"
    val dates = Seq(minDate,"2017-01-15T00:00:00Z","2017-02-01T00:00:00Z",maxDate)
    val tiles = java.util.Arrays.asList(zeroTile, emptyTile)
    buildSpatioTemporalDataCube(tiles, dates, extent, tilingFactor)
  }

  private def accumuloPyramidFactory = new geotrellisaccumulo.PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")

  def accumuloDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, srs: String) = {
    val pyramid: Seq[(Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]])] = accumuloPyramidFactory.pyramid_seq(layer, bbox, srs, minDateString, maxDateString)
    System.out.println("pyramid = " + pyramid)

    val (_, datacube) = pyramid.maxBy { case (zoom, _) => zoom }
    datacube
  }

  def catalogDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, resolution:CellSize, bandNames:List[String]) = {
    new file.PyramidFactory(OpenSearchClient.apply(new URL(opensearchEndpoint), false, "oscars"),layer,bandNames.asJava,null,resolution).pyramid_seq(bbox,"EPSG:4326",minDateString,maxDateString,util.Collections.emptyMap[String,Any](),"").head._2

  }

  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor
  val opensearchEndpoint = "https://services.terrascope.be/catalogue"
  val client: OpenSearchClient = OpenSearchClient(new URL("https://services.terrascope.be/catalogue"),isUTM = true)

  def defaultExtent = Extent(xmin = 3.248235121238894, ymin = 50.9753557675801, xmax = 3.256396825072918, ymax = 50.98003212949561)

  def probav_ndvi(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent) =
    catalogDataCube("urn:ogc:def:EOP:VITO:PROBAV_S10-TOC_333M_V001",from_date,to_date,bbox,CellSize(333, 333), NonEmptyList.of("NDVI", "SM").toList)


  def sentinel1Sigma0LayerProviderUTM =
    FileLayerProvider(
      client,
      "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/bogus",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    )

  def s2_fapar(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent)=
    catalogDataCube("urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2",from_date,to_date,bbox,CellSize(10, 10), NonEmptyList.one("FAPAR_10M").toList)


  def s2_ndvi_bands(from_date: String = "2017-11-01T00:00:00Z", to_date: String = "2017-11-16T02:00:00Z", polygons:Seq[Polygon],crs:String)={
    val parameters = new DataCubeParameters
    parameters.layoutScheme = "FloatingLayoutScheme"
    parameters.globalExtent = Some(ProjectedExtent(polygons.extent,CRS.fromName(crs)))
    new file.PyramidFactory(OpenSearchClient.apply(new URL(opensearchEndpoint), false, "oscars"), "urn:eop:VITO:TERRASCOPE_S2_TOC_V2", NonEmptyList.of("TOC-B04_10M", "TOC-B08_10M").toList.asJava, null, CellSize(10, 10))
      .datacube_seq(ProjectedPolygons(polygons, crs), from_date, to_date, util.Collections.emptyMap[String, Any](), "",parameters).head._2
  }




  def sentinel2TocLayerProviderUTM =
    FileLayerProvider(
      client,
      "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    )

  def sentinel2TocLayerProviderUTMMultiResolution =
    FileLayerProvider(
      client,
      "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B05_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    )

  def sentinel2TocLayerProviderUTM20M =
    FileLayerProvider(
      client,
      "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B11_20M", "SCENECLASSIFICATION_20M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    )

  lazy val b04RasterSource =GeoTiffRasterSource("https://artifactory.vgt.vito.be/artifactory/testdata-public/S2_B04_timeseries.tiff")

  lazy val b04Raster = {
    b04RasterSource.read().get
  }

  lazy val b04Polygons = ProjectedPolygons.fromVectorFile(getClass.getResource("/org/openeo/geotrellis/S2_B04_polygons.geojson").getPath)

  /**
   * Creates a noisy data to test with.
   * BitPixelType is treated differently.
   * mean: 10
   * min: 5
   * max: 15
   */
  def randomNoiseLayer(pixelType: PixelType = PixelType.Byte,
                       extent: Extent = defaultExtent,
                       crs: CRS = CRS.fromEpsgCode(32631),
                       dates: Option[List[ZonedDateTime]] = None,
                      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val rows = 256;
    val cols = 256;

    val rand = new scala.util.Random(42) // Fixed seed to make test predictable

    val defaultStartDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val datesGet = dates.getOrElse(0 to 4 map (defaultStartDate.plusDays(_)))

    val timeSeries: Array[(SpaceTimeKey, MultibandTile)] = datesGet.map({ date =>
      val v = pixelType match {
        // Uses values in the 0-127 range, so that windows thumbnails show something visible
        case PixelType.Double => DoubleArrayTile.apply((1 to cols * rows).map(_ => 20 + 100 * rand.nextDouble).toArray, cols, rows)
        case PixelType.Float => FloatArrayTile.apply((1 to cols * rows).map(_ => 20 + 100 * rand.nextFloat).toArray, cols, rows)
        case PixelType.Int => IntArrayTile.apply((1 to cols * rows).map(_ => 20 + rand.nextInt(101)).toArray, cols, rows)
        case PixelType.Short => ShortArrayTile.apply((1 to cols * rows).map(_ => (20 + rand.nextInt(101)).toShort).toArray, cols, rows)
        case PixelType.Byte => ByteArrayTile.apply((1 to cols * rows).map(_ => (20 + rand.nextInt(101)).toByte).toArray, cols, rows)
        case PixelType.Bit =>
          val bytes = Array.fill[Byte](cols * rows / 8)(0)
          rand.nextBytes(bytes)
          BitArrayTile.apply(bytes, cols, rows)
        case _ => throw new IllegalStateException(s"pixelType $pixelType not supported")
      }
      (
        SpaceTimeKey(0, 0, date),
        MultibandTile(v.withNoData(Some(32767)))
      )
    }).toArray

    val rdd = SparkContext.getOrCreate().parallelize(timeSeries)
    val metadata = TileLayerMetadata(
      timeSeries(0)._2.cellType,
      LayoutDefinition(RasterExtent(extent, cols, rows), cols, rows),
      extent,
      crs,
      KeyBounds[SpaceTimeKey](timeSeries.head._1, timeSeries.last._1)
    )
    new ContextRDD(rdd, metadata)
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

  def sentinel2B04LayerSparse = {
    val cube = sentinel2B04Layer
    val keys = cube.map(_._1).distinct().collect()
    implicit val newIndex = new SparseSpaceTimePartitioner(keys.map(SparseSpaceTimePartitioner.toIndex(_, 0)), 0, Some(keys.toArray))
    val partitioner = new SpacePartitioner[SpaceTimeKey](cube.metadata.bounds)(implicitly,implicitly,newIndex)
    new ContextRDD(cube.partitionBy(partitioner),cube.metadata)
  }

  def rgbLayerProvider =
    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )

  def createLayerWithGaps(layoutCols:Int,layoutRows:Int, extent:Extent = defaultExtent ) = {

    val intImage = createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val secondBand = imageTile.map { x => if (x >= 5) 10 else 100 }
    val thirdBand = imageTile.map { x => if (x >= 5) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, secondBand, thirdBand), TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
    print(tileLayerRDD.keys.collect())
    // Remove some tiles at the left of the image:
    val filtered: ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = tileLayerRDD.withContext {
      _.filter { case (key, tile) => (key.col > 0 && (key.col != 1 || key.row != 1)) }
    }
    (imageTile, filtered)
  }

  /**
   * Returned cube intentionally has missing Tiles.
   */
  def aSpacetimeTileLayerRdd(layoutCols: Int, layoutRows: Int, nbDates:Int = 2, extent:Extent = defaultExtent): (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = {
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(
      layoutCols,
      layoutRows,
      extent,
    )
    val startDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val temporal = filtered.flatMap(tuple => {
      (1 to nbDates).map(index => (SpaceTimeKey(tuple._1, TemporalKey( startDate.plusDays(index) )), tuple._2))
    }).repartition(layoutCols * layoutRows)
    val spatialM = filtered.metadata
    val newBounds = KeyBounds[SpaceTimeKey](SpaceTimeKey(spatialM.bounds.get._1,TemporalKey(0L)),SpaceTimeKey(spatialM.bounds.get._2,TemporalKey(0L)))
    val temporalMetadata = new TileLayerMetadata[SpaceTimeKey](
      spatialM.cellType,
      spatialM.layout,
      spatialM.extent,
      spatialM.crs,
      newBounds,
    )
    (ContextRDD(temporal, temporalMetadata), imageTile)
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

  val CGLS1KMResolution = CellSize(0.008928571428584, 0.008928571428584)
  val cglsFAPARPath = {
    val uri = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/cgls_fapar_2009/c_gls_FAPAR_200907100000_GLOBE_VGT_V2.0.1.nc").toURI
    uri
  }

  def cglsFAPAR1km = {
    val dataGlob = Paths.get(cglsFAPARPath).getParent.resolve( "*.nc" ).toString
    val netcdfVariables = java.util.Arrays.asList("FAPAR")
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val openSearchClient = OpenSearchClient(dataGlob, isUTM = false, dateRegex, netcdfVariables, "cgls")

    new org.openeo.geotrellis.file.PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "", openSearchLinkTitles = netcdfVariables, "",
      maxSpatialResolution = CGLS1KMResolution,
      experimental = false
    )
  }


  val cglsNDVI300 = {
    val dataGlob = "/data/MTDA/BIOPAR/BioPar_NDVI300_V1_Global/2019/201906*/*/*.nc"
    val netcdfVariables = java.util.Arrays.asList("NDVI")
    val dateRegex = raw".+_(\d{4})(\d{2})(\d{2})0000_.+"
    val openSearchClient = OpenSearchClient(dataGlob, isUTM = false, dateRegex, netcdfVariables, "cgls")
    new org.openeo.geotrellis.file.PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "", openSearchLinkTitles = netcdfVariables, "",
      maxSpatialResolution = CellSize(0.002976190476204, 0.002976190476190),
      experimental = false
    )
  }

}
