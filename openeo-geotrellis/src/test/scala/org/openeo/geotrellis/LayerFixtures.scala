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
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.file.{FixedFeaturesOpenSearchClient, PyramidFactory}
import org.openeo.geotrellis.layers.{FileLayerProvider, MockOpenSearchFeatures, SplitYearMonthDayPathDateExtractor}
import org.openeo.geotrelliscommon.{DataCubeParameters, SparseSpaceTimePartitioner}
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{CreoFeatureCollection, FeatureBuilder, FeatureCollection}
import org.openeo.opensearch.backends.CreodiasClient
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import spire.math.UShort

import java.awt.image.DataBufferByte
import java.io.File
import java.net.{URI, URL}
import java.nio.file.Paths
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util
import java.util.Collections
import java.util.Collections.singletonList
import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.io.Source
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
      cellType = mbTiles.filter(_.bandCount>0).head.cellType
    )
    new ContextRDD(cubeXYTB, cubeXYTB.metadata)
  }

  def buildSingleBandSpatioTemporalDataCube(tiles: java.util.List[Tile], dates: Seq[String]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    implicit val sc = SparkContext.getOrCreate
    val times: Seq[ZonedDateTime] = dates.map(ZonedDateTime.parse(_))
    val layout = new TileLayout(1, 1, tiles.get(0).cols.asInstanceOf[Integer], tiles.get(0).rows.asInstanceOf[Integer])
    val cubeXYB: TileLayerRDD[SpaceTimeKey] = TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(tiles.asScala.zip(times),layout)

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

  def catalogDataCube(layer: String, minDateString: String, maxDateString: String, bbox: Extent, resolution:CellSize, bandNames:List[String]) = {
    new file.PyramidFactory(OpenSearchClient.apply(new URI(opensearchEndpoint).toURL, false, "oscars"),layer,bandNames.asJava,null,resolution).pyramid_seq(bbox,"EPSG:4326",minDateString,maxDateString,util.Collections.emptyMap[String,Any](),"").head._2

  }

  private val maxSpatialResolution = CellSize(10, 10)
  private val pathDateExtractor = SplitYearMonthDayPathDateExtractor
  val opensearchEndpoint = "https://services.terrascope.be/catalogue"
  val client: OpenSearchClient = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
    """{
      |    "features": [
      |        {
      |            "type": "Feature",
      |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20200405T104619_31UDS_TOC_V210",
      |            "geometry": {"type":"Polygon","coordinates":[[[1.5885551,50.5261385],[1.5907216,50.4552653],[3.1375125,50.4637172],[3.140458,51.4510981],[1.9612252,51.444565],[1.9365656,51.3849571],[1.8766533,51.2391211],[1.8170155,51.093279],[1.7577979,50.9474451],[1.6990549,50.8015836],[1.6405846,50.6556649],[1.5885551,50.5261385]]]},
      |            "bbox": [1.5885551,50.4552653,3.140458,51.4510981],
      |            "properties":
      |            	{"date":"2020-04-05T10:46:19.024Z","updated":"2024-05-18T17:47:54.738Z","available":"2024-05-18T17:47:56Z","published":"2024-05-18T17:47:56Z","status":"ARCHIVED","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","title":"S2B_20200405T104619_31UDS_TOC_V210","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20200405T104619_31UDS_TOC_V210","acquisitionInformation":[{"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"},"acquisitionParameters":{"acquisitionType":"NOMINAL","orbitDirection":"DESCENDING","orbitNumber":16093,"relativeOrbitNumber":51,"beginningDateTime":"2020-04-05T10:46:19.024Z","endingDateTime":"2020-04-05T10:46:19.024Z","tileId":"31UDS"}}],"productInformation":{"cloudCover":0.009,"productType":"TOC","availabilityTime":"2024-05-18T17:47:56Z","productVersion":"V210","processingCenter":"VITO","processingDate":"2024-05-18T17:47:54.738Z"},"links":{"previews":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC_QUICKLOOK_V210.tif","type":"image/tiff","length":916241,"category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2020-04-05&BBOX=176837.14482905777,6525496.291736421,349594.1854176624,6701479.020766405&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC_V210.xml","type":"application/vnd.iso.19139+xml","length":39917,"title":"Inspire metadata"}],"related":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_AOT_60M_V210.tif","type":"image/tiff","length":126778,"title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_RAA_60M_V210.tif","type":"image/tiff","length":446816,"title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_SCENECLASSIFICATION_20M_V210.tif","type":"image/tiff","length":2750970,"title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_SZA_60M_V210.tif","type":"image/tiff","length":96322,"title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_VZA_60M_V210.tif","type":"image/tiff","length":236833,"title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_WVP_60M_V210.tif","type":"image/tiff","length":4372298,"title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B01_60M_V210.tif","type":"image/tiff","length":4145578,"title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B02_10M_V210.tif","type":"image/tiff","length":150087700,"title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B03_10M_V210.tif","type":"image/tiff","length":150248378,"title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B04_10M_V210.tif","type":"image/tiff","length":151146309,"title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B05_20M_V210.tif","type":"image/tiff","length":39667111,"title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B06_20M_V210.tif","type":"image/tiff","length":40032943,"title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B07_20M_V210.tif","type":"image/tiff","length":40619597,"title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B08_10M_V210.tif","type":"image/tiff","length":147847722,"title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B11_20M_V210.tif","type":"image/tiff","length":38187925,"title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B12_20M_V210.tif","type":"image/tiff","length":38712077,"title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2020/04/05/S2B_20200405T104619_31UDS_TOC_V210/S2B_20200405T104619_31UDS_TOC-B8A_20M_V210.tif","type":"image/tiff","length":40492355,"title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}]}}
      |         }
      |    ]
      |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))
      client
  }

  def defaultExtent = Extent(xmin = 3.248235121238894, ymin = 50.9753557675801, xmax = 3.256396825072918, ymax = 50.98003212949561)

  def probav_ndvi(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z",bbox:Extent=defaultExtent) =
    catalogDataCube("urn:ogc:def:EOP:VITO:PROBAV_S10-TOC_333M_V001",from_date,to_date,bbox,CellSize(333, 333), NonEmptyList.of("NDVI", "SM").toList)


  def sentinel1Sigma0LayerProviderUTM = {
    val client = new FixedFeaturesOpenSearchClient
    FeatureCollection.parse(
      """{
        |    "features": [
        |        {
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110",
        |            "geometry": {"coordinates":[[[5.470964,51.002892],[5.967735,52.495502],[2.110356,52.909264],[1.739041,51.414223],[5.470964,51.002892]]],"type":"Polygon"},
        |            "bbox": [1.739041,51.002892,5.967735,52.909264],
        |            "properties":
        |            	{"date":"2022-09-13T05:58:45.622Z","identifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110","available":"2022-09-13T11:23:10Z","parentIdentifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1","productInformation":{"processingCenter":"VITO","productVersion":"V110","timeliness":"NRT-3h","processingDate":"2022-09-13T11:23:08.049Z","productType":"SIGMA0","availabilityTime":"2022-09-13T11:23:10Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":75688563,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110_angle.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"angle","bandNames":["angle"]},{"length":1626191427,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":1639934755,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_GRD_SIGMA0&TIME=2022-09-13&BBOX=193589.15858862526,6621805.30000011,664325.2213891965,6966231.82056091&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":38273,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2022-09-13T11:23:10Z","title":"S1A_IW_GRDH_SIGMA0_DV_20220913T055845_DESCENDING_110_2A71_V110","updated":"2022-09-13T11:23:08.049Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":110,"polarisationChannels":"VV, VH","beginningDateTime":"2022-09-13T05:58:45.622Z","orbitDirection":"DESCENDING","endingDateTime":"2022-09-13T05:59:10.62Z","orbitNumber":44982},"platform":{"platformShortName":"Sentinel-1","platformSerialIdentifier":"S1A"}}],"status":"ARCHIVED"}
        |         }
        |        ,{
        |            "type": "Feature",
        |            "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110",
        |            "geometry": {"coordinates":[[[4.992859,49.50906],[5.470935,51.0028],[1.740583,51.413986],[1.377835,49.9184],[4.992859,49.50906]]],"type":"Polygon"},
        |            "bbox": [1.377835,49.50906,5.470935,51.413986],
        |            "properties":
        |            	{"date":"2022-09-13T05:59:10.622Z","identifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1:S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110","available":"2022-09-13T11:10:42Z","parentIdentifier":"urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1","productInformation":{"processingCenter":"VITO","productVersion":"V110","timeliness":"NRT-3h","processingDate":"2022-09-13T11:10:41.693Z","productType":"SIGMA0","availabilityTime":"2022-09-13T11:10:42Z","referenceSystemIdentifier":"EPSG:32631"},"links":{"related":[],"data":[{"length":105758827,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110_angle.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"angle","bandNames":["angle"]},{"length":1649481383,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110_VH.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VH","bandNames":["VH"]},{"length":1639884275,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110_VV.tif","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/tiff","title":"VV","bandNames":["VV"]}],"previews":[{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S1_GRD_SIGMA0&TIME=2022-09-13&BBOX=153379.89059715008,6361683.825834345,609021.6983630981,6694852.353558086&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","conformsTo":"https://www.opengis.net/def/crs/EPSG/0/32631","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":38267,"href":"file:///data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2022/09/13/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110/S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2022-09-13T11:10:42Z","title":"S1A_IW_GRDH_SIGMA0_DV_20220913T055910_DESCENDING_110_4192_V110","updated":"2022-09-13T11:10:41.693Z","acquisitionInformation":[{"acquisitionParameters":{"operationalMode":"IW","polarisationMode":"D","acquisitionType":"NOMINAL","relativeOrbitNumber":110,"polarisationChannels":"VV, VH","beginningDateTime":"2022-09-13T05:59:10.622Z","orbitDirection":"DESCENDING","endingDateTime":"2022-09-13T05:59:35.621Z","orbitNumber":44982},"platform":{"platformShortName":"Sentinel-1","platformSerialIdentifier":"S1A"}}],"status":"ARCHIVED"}
        |         }
        |    ]
        |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))

    new FileLayerProvider(
      client,
      "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
      openSearchLinkTitles = NonEmptyList.of("VV"),
      rootPath = "/bogus",
      maxSpatialResolution,
      pathDateExtractor,
      layoutScheme = FloatingLayoutScheme(256),
      experimental = false
    ){
      override def determineCelltype(overlappingRasterSources: Seq[(RasterSource, OpenSearchResponses.Feature)]): CellType = FloatConstantNoDataCellType
    }
  }

  def s2_fapar(from_date:String = "2017-11-01T00:00:00Z", to_date:String="2017-11-16T02:00:00Z", polygons:Seq[Polygon],crs:String) = {
    val parameters = new DataCubeParameters
    parameters.layoutScheme = "FloatingLayoutScheme"
    parameters.globalExtent = Some(ProjectedExtent(polygons.extent, CRS.fromName(crs)))
    new file.PyramidFactory(OpenSearchClient.apply(new URL(opensearchEndpoint), false, "oscars"), "urn:eop:VITO:TERRASCOPE_S2_FAPAR_V2", NonEmptyList.of("FAPAR_10M").toList.asJava, null, CellSize(10, 10))
      .datacube_seq(ProjectedPolygons(polygons, crs), from_date, to_date, util.Collections.emptyMap[String, Any](), "", parameters).head._2
  }



  def s2_ndvi_bands(from_date: String = "2017-11-01T00:00:00Z", to_date: String = "2017-11-16T02:00:00Z", polygons:Seq[Polygon],crs:String)={
    val parameters = new DataCubeParameters
    parameters.layoutScheme = "FloatingLayoutScheme"
    parameters.globalExtent = Some(ProjectedExtent(polygons.extent,CRS.fromName(crs)))
    new file.PyramidFactory(OpenSearchClient.apply(new URL(opensearchEndpoint), false, "oscars"), "urn:eop:VITO:TERRASCOPE_S2_TOC_V2", NonEmptyList.of("TOC-B04_10M", "TOC-B08_10M").toList.asJava, null, CellSize(10, 10))
      .datacube_seq(ProjectedPolygons(polygons, crs), from_date, to_date, util.Collections.emptyMap[String, Any](), "",parameters).head._2
  }

  def s2_scl(from_date: String = "2017-11-01T00:00:00Z", to_date: String = "2017-11-16T02:00:00Z", polygons: Seq[Polygon], crs: String) = {
    val parameters = new DataCubeParameters
    parameters.layoutScheme = "FloatingLayoutScheme"
    parameters.globalExtent = Some(ProjectedExtent(polygons.extent, CRS.fromName(crs)))
    new file.PyramidFactory(OpenSearchClient.apply(new URL(opensearchEndpoint), false, "oscars"), "urn:eop:VITO:TERRASCOPE_S2_TOC_V2", NonEmptyList.of("SCENECLASSIFICATION_20M").toList.asJava, null, CellSize(10, 10))
      .datacube_seq(ProjectedPolygons(polygons, crs), from_date, to_date, util.Collections.emptyMap[String, Any](), "", parameters).head._2
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

  def sentinel2TocLayerProviderUTM20M = {
    val client: OpenSearchClient = {
      val client = new FixedFeaturesOpenSearchClient
      FeatureCollection.parse(
        """{
          |    "features": [
          |        {
          |            "type": "Feature",
          |            "id": "urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20220701T103629_31UGS_TOC_V210",
          |            "geometry": {"coordinates":[[[7.3576219,50.3821223],[7.4505897,51.366602],[5.8757173,51.4158977],[5.8155101,50.4297266],[7.3576219,50.3821223]]],"type":"Polygon"},
          |            "bbox": [5.8155101,50.3821223,7.4505897,51.4158977],
          |            "properties":
          |            	{"date":"2022-07-01T10:36:29.024Z","identifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2:S2B_20220701T103629_31UGS_TOC_V210","available":"2022-07-01T23:17:58Z","parentIdentifier":"urn:eop:VITO:TERRASCOPE_S2_TOC_V2","productInformation":{"processingCenter":"VITO","productVersion":"V210","processingDate":"2022-07-01T23:17:56.351Z","cloudCover":57.769,"productType":"TOC","availabilityTime":"2022-07-01T23:17:58Z"},"links":{"related":[{"length":288356,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_AOT_60M_V210.tif","type":"image/tiff","title":"AOT_60M","bandNames":["AOT_60M"],"category":"QUALITY"},{"length":1919500,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_RAA_60M_V210.tif","type":"image/tiff","title":"RAA_60M","bandNames":["RAA_60M"],"category":"QUALITY"},{"length":3733979,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_SCENECLASSIFICATION_20M_V210.tif","type":"image/tiff","title":"SCENECLASSIFICATION_20M","bandNames":["SCENECLASSIFICATION_20M"],"category":"QUALITY"},{"length":100464,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_SZA_60M_V210.tif","type":"image/tiff","title":"SZA_60M","bandNames":["SZA_60M"],"category":"QUALITY"},{"length":298197,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_VZA_60M_V210.tif","type":"image/tiff","title":"VZA_60M","bandNames":["VZA_60M"],"category":"QUALITY"},{"length":2384084,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_WVP_60M_V210.tif","type":"image/tiff","title":"WVP_60M","bandNames":["WVP_60M"],"category":"QUALITY"}],"data":[{"length":4553644,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B01_60M_V210.tif","type":"image/tiff","title":"TOC-B01_60M","bandNames":["TOC-B01_60M"]},{"length":143795453,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B02_10M_V210.tif","type":"image/tiff","title":"TOC-B02_10M","bandNames":["TOC-B02_10M"]},{"length":144650054,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B03_10M_V210.tif","type":"image/tiff","title":"TOC-B03_10M","bandNames":["TOC-B03_10M"]},{"length":146193161,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B04_10M_V210.tif","type":"image/tiff","title":"TOC-B04_10M","bandNames":["TOC-B04_10M"]},{"length":40444968,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B05_20M_V210.tif","type":"image/tiff","title":"TOC-B05_20M","bandNames":["TOC-B05_20M"]},{"length":41546720,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B06_20M_V210.tif","type":"image/tiff","title":"TOC-B06_20M","bandNames":["TOC-B06_20M"]},{"length":41945062,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B07_20M_V210.tif","type":"image/tiff","title":"TOC-B07_20M","bandNames":["TOC-B07_20M"]},{"length":151992869,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B08_10M_V210.tif","type":"image/tiff","title":"TOC-B08_10M","bandNames":["TOC-B08_10M"]},{"length":40383019,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B11_20M_V210.tif","type":"image/tiff","title":"TOC-B11_20M","bandNames":["TOC-B11_20M"]},{"length":39859136,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B12_20M_V210.tif","type":"image/tiff","title":"TOC-B12_20M","bandNames":["TOC-B12_20M"]},{"length":41979880,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC-B8A_20M_V210.tif","type":"image/tiff","title":"TOC-B8A_20M","bandNames":["TOC-B8A_20M"]}],"previews":[{"length":883416,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC_QUICKLOOK_V210.tif","type":"image/tiff","category":"QUICKLOOK"},{"href":"https://services.terrascope.be/wms/v2?SERVICE=WMS&REQUEST=getMap&VERSION=1.3.0&CRS=EPSG:3857&SRS=EPSG:3857&LAYERS=CGS_S2_RADIOMETRY&TIME=2022-07-01&BBOX=647379.6230351395,6512717.581266398,829395.8515136089,6695193.571674648&WIDTH=80&HEIGHT=80&FORMAT=image/png&TRANSPARENT=true","type":"image/png","title":"WMS","bandNames":["WMS"],"category":"QUICKLOOK"}],"alternates":[{"length":39913,"href":"file:///data/MTDA/TERRASCOPE_Sentinel2/TOC_V2/2022/07/01/S2B_20220701T103629_31UGS_TOC_V210/S2B_20220701T103629_31UGS_TOC_V210.xml","type":"application/vnd.iso.19139+xml","title":"Inspire metadata"}]},"published":"2022-07-01T23:17:58Z","title":"S2B_20220701T103629_31UGS_TOC_V210","updated":"2022-07-01T23:17:56.351Z","acquisitionInformation":[{"acquisitionParameters":{"acquisitionType":"NOMINAL","tileId":"31UGS","relativeOrbitNumber":8,"beginningDateTime":"2022-07-01T10:36:29.024Z","orbitDirection":"ASCENDING","endingDateTime":"2022-07-01T10:36:29.024Z","orbitNumber":27776},"platform":{"platformShortName":"Sentinel-2","platformSerialIdentifier":"S2B"}}],"status":"ARCHIVED"}
          |         }
          |    ]
          |  }""".stripMargin).features.foreach(feature => client.addFeature(feature))
      client
    }

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
  }

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
                       extent: Extent = ProjectedExtent(defaultExtent,LatLng).reproject(CRS.fromEpsgCode(32631)),
                       crs: CRS = CRS.fromEpsgCode(32631),
                       dates: Option[List[ZonedDateTime]] = None,cols:Int = 256,rows:Int = 256
                      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    val rand = new scala.util.Random(42) // Fixed seed to make test predictable

    val defaultStartDate = ZonedDateTime.parse("2019-01-21T00:00:00Z")
    val datesGet = dates.getOrElse(0 to 4 map (defaultStartDate.plusDays(_)))

    val timeSeries: Array[(Tile, ZonedDateTime)] = datesGet.map({ date =>
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
        v.withNoData(Some(32767)),
        date
      )
    }).toArray

    implicit val sc = SparkContext.getOrCreate()

    val layout = LayoutDefinition(RasterExtent(extent, cols, rows), 64, 64)
    val rdd = TileLayerRDDBuilders.createSpaceTimeTileLayerRDD(timeSeries,layout.tileLayout,timeSeries(0)._1.cellType)

    new ContextRDD(rdd.mapValues(t => MultibandTile(t)),rdd.metadata.copy(layout= rdd.metadata.layout.copy(extent=extent),extent = extent,crs=crs))
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

  def loadFeaturesWithArtifactoryMock(jsonPath: String): OpenSearchClient = {
    val jsonPathFull = getClass.getResource(jsonPath)
    val fileSource = Source.fromURL(jsonPathFull)
    var txt = try fileSource.mkString
    finally fileSource.close()
    val basePath = new File(jsonPathFull.getFile).getParent
    // Use artifactory to avoid heavy git repo
    val basePathArtifactory = "https://artifactory.vgt.vito.be/artifactory/testdata-public"

    /*
    To upload new files;
    - mount /eodata
    - change openeo-opensearch-client to use fs instead of s3
    - run this Python script, and copy the printed paths here:
root = Path("/tmp/eodata/")
l = set(filter(lambda p: p.is_file(), root.rglob("*.*")))
l = {f for f in l if os.stat(f).st_size > 0}
l = set(map(lambda p: os.path.relpath(p, root), l))
for p in l:
    cmd = f'curl -uUSERNAME:PASS -T {root / p} "https://artifactory.vgt.vito.be/artifactory/testdata-public/eodata/{p}"'
    print(cmd)
    code = os.system(cmd)
    if code != 0:
        raise Exception("Failed: " + cmd)
print("\n")
for p in l:
    print(f'"/eodata/{p}",')
     */

    val artifactoryPaths = Set(
      "/eodata/Sentinel-2/MSI/L2A/2023/01/17/S2B_MSIL2A_20230117T104259_N0509_R008_T31UGS_20230117T120337.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2023/01/17/S2B_MSIL2A_20230117T104259_N0509_R008_T31UGS_20230117T120337.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2023/01/17/S2B_MSIL2A_20230117T104259_N0509_R008_T31UGS_20230117T120337.SAFE/GRANULE/L2A_T31UGS_A030636_20230117T104258/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2023/01/17/S2B_MSIL2A_20230117T104259_N0509_R008_T31UGS_20230117T120337.SAFE/GRANULE/L2A_T31UGS_A030636_20230117T104258/IMG_DATA/R10m/T31UGS_20230117T104259_B04_10m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2023/04/05/S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2023/04/05/S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2023/04/05/S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/GRANULE/L2A_T31UFS_A040660_20230405T105026/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2023/04/05/S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/GRANULE/L2A_T31UFS_A040660_20230405T105026/IMG_DATA/R10m/T31UFS_20230405T105031_B04_10m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2023/04/05/S2A_MSIL2A_20230405T105031_N0509_R051_T31UFS_20230405T162253.SAFE/GRANULE/L2A_T31UFS_A040660_20230405T105026/IMG_DATA/R20m/T31UFS_20230405T105031_SCL_20m.jp2",
      // for testMissingS2:
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWT_20240324T234241.SAFE/GRANULE/L2A_T03WWT_A036821_20240324T230529/IMG_DATA/R20m/T03WWT_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWT_20240324T234241.SAFE/GRANULE/L2A_T03WWT_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWT_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWT_20240324T234241.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWU_20240324T234241.SAFE/GRANULE/L2A_T03WWU_A036821_20240324T230529/IMG_DATA/R20m/T03WWU_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWU_20240324T234241.SAFE/GRANULE/L2A_T03WWU_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWU_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WWU_20240324T234241.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXT_20240324T234241.SAFE/GRANULE/L2A_T03WXT_A036821_20240324T230529/IMG_DATA/R20m/T03WXT_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXT_20240324T234241.SAFE/GRANULE/L2A_T03WXT_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXT_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXT_20240324T234241.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXU_20240324T234241.SAFE/GRANULE/L2A_T03WXU_A036821_20240324T230529/IMG_DATA/R20m/T03WXU_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXU_20240324T234241.SAFE/GRANULE/L2A_T03WXU_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXU_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T03WXU_20240324T234241.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDC_20240324T234241.SAFE/GRANULE/L2A_T04WDC_A036821_20240324T230529/IMG_DATA/R20m/T04WDC_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDC_20240324T234241.SAFE/GRANULE/L2A_T04WDC_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDC_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDC_20240324T234241.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDD_20240324T234241.SAFE/GRANULE/L2A_T04WDD_A036821_20240324T230529/IMG_DATA/R20m/T04WDD_20240324T230529_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDD_20240324T234241.SAFE/GRANULE/L2A_T04WDD_A036821_20240324T230529/MTD_TL.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDD_20240324T234241.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/03/24/S2B_MSIL2A_20240324T230529_N0510_R044_T04WDD_20240324T234241.SAFE/MTD_MSIL2A.xml",
      // testMissingS2DateLine
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T01WCU_20240402T003652.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T01WCU_20240402T003652.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T60WWD_20240402T003652.SAFE/MTD_MSIL2A.xml",
      "/eodata/Sentinel-2/MSI/L1C/2024/04/02/S2B_MSIL1C_20240402T000609_N0510_R016_T01WCU_20240402T001958.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L1C/2024/04/02/S2B_MSIL1C_20240402T000609_N0510_R016_T01WCU_20240402T001958.SAFE/MTD_MSIL1C.xml",
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T60WWD_20240402T003652.SAFE/GRANULE/L2A_T60WWD_A036936_20240402T000609/IMG_DATA/R20m/T60WWD_20240402T000609_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L1C/2024/04/02/S2B_MSIL1C_20240402T000609_N0510_R016_T60WWD_20240402T001958.SAFE/MTD_MSIL1C.xml",
      "/eodata/Sentinel-2/MSI/L1C/2024/04/02/S2B_MSIL1C_20240402T000609_N0510_R016_T60WWD_20240402T001958.SAFE/manifest.safe",
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T01WCU_20240402T003652.SAFE/GRANULE/L2A_T01WCU_A036936_20240402T000609/IMG_DATA/R20m/T01WCU_20240402T000609_SCL_20m.jp2",
      "/eodata/Sentinel-2/MSI/L2A/2024/04/02/S2B_MSIL2A_20240402T000609_N0510_R016_T60WWD_20240402T003652.SAFE/manifest.safe",
      // testAntimerideanArtifacts:
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_01WCS_0_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_01WDS_0_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_01WCT_0_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_60WWB_0_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_60WWC_0_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_60WWC_1_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_60WWB_1_0/VV.tif",
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/08/01/Sentinel-1_IW_mosaic_2020_M08_01WDT_0_0/VV.tif",
      // testAvoidCroppingAwayNoData:
      "/eodata/Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM/2020/01/01/Sentinel-1_IW_mosaic_2020_M01_43XDB_0_0/VV.tif",
    )

    val matches = "\"(/eodata/.*?)\"".r.findAllIn(txt).toList
    for (m <- matches) {
      val pathFromJson = m.substring(1, m.length - 1)

      for (artifactoryPath <- artifactoryPaths) {
        if (artifactoryPath.startsWith(pathFromJson)) {
          txt = txt.replace('"' + pathFromJson + '"', '"' + basePath + pathFromJson + '"')

          // Only download when needed for current test:
          val jp2File = new File(basePath, artifactoryPath)
          if (!jp2File.exists()) {
            println("Copy from artifactory to: " + jp2File)
            FileUtils.copyURLToFile(new URL(basePathArtifactory + artifactoryPath), jp2File)
          }

        }
      }
    }

    val mockedFeatures = CreoFeatureCollection.parse(txt)
    new MockOpenSearchFeatures(mockedFeatures.features)
  }

  def sentinel2Cube(localDate: LocalDate,
                    projected_polygons_native_crs: ProjectedPolygons,
                    jsonPath: String,
                    dataCubeParameters: DataCubeParameters = new DataCubeParameters,
                    bandNames: util.List[String] = util.Arrays.asList("IMG_DATA_Band_B04_10m_Tile1_Data", "S2_Level-2A_Tile1_Metadata##1", "S2_Level-2A_Tile1_Metadata##0")
                   ): MultibandTileLayerRDD[SpaceTimeKey] = {
    creodiasCube(
      localDate,
      projected_polygons_native_crs,
      jsonPath,
      bandNames,
      dataCubeParameters,
    )
  }

  /**
   * Creates a Sentinel-2 cube by downloading data locally.
   */
  def creodiasCube(localDate: LocalDate,
                    projected_polygons_native_crs: ProjectedPolygons,
                    jsonPath: String,
                   bandNames: util.List[String],
                    dataCubeParameters: DataCubeParameters = new DataCubeParameters,
                   ): MultibandTileLayerRDD[SpaceTimeKey] = {
    val client = loadFeaturesWithArtifactoryMock(jsonPath)
    //    val client = CreodiasClient() // More difficult to capture a nodata piece

    val localFromDate = localDate
    val localToDate = localDate.plusDays(1)
    val ZonedFromDate = ZonedDateTime.of(localFromDate, java.time.LocalTime.MIDNIGHT, UTC)
    val zonedToDate = ZonedDateTime.of(localToDate, java.time.LocalTime.MIDNIGHT, UTC)

    val factory = new PyramidFactory(
      client, "<fakeOpenSearchCollectionId>", bandNames,
      null,
      maxSpatialResolution = if (projected_polygons_native_crs.crs == LatLng)
        CellSize(0.0001471299295632278, 0.0001471299295632278) else CellSize(10, 10),
    )
    factory.crs = projected_polygons_native_crs.crs

    val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format ZonedFromDate
    val to_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format zonedToDate

    val cube: Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = factory.datacube_seq(
      projected_polygons_native_crs,
      from_date, to_date, Collections.emptyMap(), "", dataCubeParameters = dataCubeParameters
    )
    cube.head._2
  }

  def rgbLayerProvider = {

    FileLayerProvider(
      openSearch = client,
      openSearchCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
      openSearchLinkTitles = NonEmptyList.of("TOC-B04_10M", "TOC-B03_10M", "TOC-B02_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2",
      maxSpatialResolution = CellSize(10, 10),
      pathDateExtractor = SplitYearMonthDayPathDateExtractor
    )
  }

  def createLayerWithGaps(layoutCols:Int,layoutRows:Int, extent:Extent = defaultExtent , crs:CRS = LatLng) = {

    val intImage = createTextImage(layoutCols * 256, layoutRows * 256)
    val imageTile = ByteArrayTile(intImage, layoutCols * 256, layoutRows * 256)

    val secondBand = imageTile.map { x => if (x >= 5) 10 else 100 }
    val thirdBand = imageTile.map { x => if (x >= 5) 50 else 200 }

    val tileLayerRDD = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, secondBand, thirdBand), TileLayout(layoutCols, layoutRows, 256, 256), crs)
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
  def aSpacetimeTileLayerRdd(layoutCols: Int, layoutRows: Int, nbDates:Int = 2, extent:Extent = defaultExtent, crs:CRS= LatLng): (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = {
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(
      layoutCols,
      layoutRows,
      extent,
      crs,
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


  def aSpacetimeTileLayerRddShortFillValue(layoutCols: Int, layoutRows: Int, nbDates:Int = 2, fillValue:Short = UShort.MaxValue.toShort): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val imageTile = IntArrayTile.fill(40000,1024, 1024).convert(UShortUserDefinedNoDataCellType(fillValue)).mutable
    val filtered: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(imageTile, imageTile, imageTile), TileLayout(layoutCols, layoutRows, 256, 256), LatLng)
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
    ContextRDD(temporal, temporalMetadata)
  }

  def aSpacetimeTileLayerRddArrayTile(arrayTile: IntArrayTile, layoutCols: Int, layoutRows: Int, nbDates:Int = 2, crs:CRS=LatLng): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val filtered: MultibandTileLayerRDD[SpatialKey] = TileLayerRDDBuilders.createMultibandTileLayerRDD(SparkContext.getOrCreate, MultibandTile(arrayTile, arrayTile, arrayTile), TileLayout(layoutCols, layoutRows, arrayTile.cols/layoutCols, arrayTile.rows/layoutRows), crs)
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
    ContextRDD(temporal, temporalMetadata)
  }


  def aSpacetimeTileLayerHoursRdd(layoutCols: Int, layoutRows: Int, nbDates:Int = 2, extent:Extent = defaultExtent): (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], ByteArrayTile) = {
    val (imageTile: ByteArrayTile, filtered: MultibandTileLayerRDD[SpatialKey]) = LayerFixtures.createLayerWithGaps(
      layoutCols,
      layoutRows,
      extent,
    )
    val startDate = ZonedDateTime.parse("2017-01-01T00:00:00Z")
    val temporal = filtered.flatMap(tuple => {
      (1 to nbDates).map(index => (SpaceTimeKey(tuple._1, TemporalKey( startDate.plusHours(index) )), tuple._2))
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

  def aSparseSpacetimeTileLayerRdd(desiredKeys:Seq[SpatialKey] = Seq(SpatialKey(0,0),SpatialKey(3,1),SpatialKey(7,2)), crs:CRS =LatLng): MultibandTileLayerRDD[SpaceTimeKey] = {
    val collection = aSpacetimeTileLayerRdd(8,4,4,crs= crs)

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


  lazy val cglsNDVI300 = {
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

  def STACCOGCollection(resolution:CellSize = CellSize(0.1, 0.1) ,bands: util.List[String] = util.Arrays.asList("temperature-mean","precipitation-flux") )= {
    val client = new FixedFeaturesOpenSearchClient()
    client.addFeature(OpenSearchResponses.featureBuilder().withId("openEO_2020-07-01Z.tif").withNominalDate("2020-07-01T00:00:00Z").withBBox(-180.05,-90.05,180.05,90.05).withResolution(0.1).withRasterExtent(-180.05,-90.05,180.05,90.05).withCRS("EPSG:4326").addLink("https://s3.waw3-1.cloudferro.com/swift/v1/agera/AgERA5_monthly_2017-12-01.tif","openEO_2020-07-01Z.tif",bandNames = util.Arrays.asList("temperature-mean","precipitation-flux")).build)
    client.addFeature(OpenSearchResponses.featureBuilder().withId("openEO_2020-08-01Z.tif").withNominalDate("2020-08-01T00:00:00Z").withBBox(-180.05,-90.05,180.05,90.05).withResolution(0.1).withRasterExtent(-180.05,-90.05,180.05,90.05).withCRS("EPSG:4326").addLink("https://s3.waw3-1.cloudferro.com/swift/v1/agera/AgERA5_monthly_2017-11-01.tif","openEO_2020-08-01Z.tif",bandNames = util.Arrays.asList("temperature-mean","precipitation-flux")).build)
    client.addFeature(OpenSearchResponses.featureBuilder().withId("openEO_2020-09-01Z.tif").withNominalDate("2020-09-01T00:00:00Z").withBBox(-180.05,-90.05,180.05,90.05).withResolution(0.1).withRasterExtent(-180.05,-90.05,180.05,90.05).withCRS("EPSG:4326").addLink("https://s3.waw3-1.cloudferro.com/swift/v1/agera/AgERA5_monthly_2017-10-01.tif","openEO_2020-09-01Z.tif",bandNames = util.Arrays.asList("temperature-mean","precipitation-flux")).build)

    val factory = new PyramidFactory(
      client, "STAC_AGERA", bands,
      null,
      maxSpatialResolution = resolution,
    )
    factory
  }

  def stacCogNoNoDataCollection: PyramidFactory = {
    val openSearchClient = new FixedFeaturesOpenSearchClient

    val bandNames = singletonList("L2A-B02-P10")
    val resolution = 10

    openSearchClient.addFeature(
      OpenSearchResponses.featureBuilder()
        .withId("2020_34TFR_001")
        .withNominalDate("2020-01-01T00:00:00Z")
        .withBBox(22.4224844035807, 45.950836422259, 22.557151965576, 46.0446922710336)
        .addLink(
          href = "https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/LCFM_LSF-ANNUAL_V100_2020_34TFR_001_L2A-BANDS.tif",
          title = "Sentinel-2_AnnualFeatures",
          bandNames,
        )
        .withCRS("EPSG:32634")
        .withRasterExtent(610240, 5089760, 620480, 5100000)
        .withResolution(resolution)
        .build
    )

    openSearchClient.addFeature(
      OpenSearchResponses.featureBuilder()
        .withId("2020_34TFR_000")
        .withNominalDate("2020-01-01T00:00:00Z")
        .withBBox(22.2903879775156, 45.9525572152198, 22.4248477757042, 46.046265455836)
        .addLink(
          href = "https://artifactory.vgt.vito.be/artifactory/testdata-public/openeo/geotrellis-extensions/LCFM_LSF-ANNUAL_V100_2020_34TFR_000_L2A-BANDS.tif",
          title = "Sentinel-2_AnnualFeatures",
          bandNames,
        )
        .withCRS("EPSG:32634")
        .withRasterExtent(600000, 5089760, 610240, 5100000)
        .withResolution(resolution)
        .build
    )

    new PyramidFactory(
      openSearchClient,
      openSearchCollectionId = "https://stac.openeo.vito.be",
      openSearchLinkTitles = bandNames,
      rootPath = null,
      maxSpatialResolution = CellSize(resolution, resolution),
    )
  }
}
