package org.openeo.geotrellisvlm

import java.nio.file.{Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util

import be.vito.eodata.biopar.EOProduct
import be.vito.eodata.catalog.CatalogClient
import geotrellis.contrib.vlm.RasterSourceRDD.PARTITION_BYTES
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal.GDALReprojectRasterSource
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster._
import geotrellis.raster.render.ColorMap
import geotrellis.raster.reproject.Reproject
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.{ArrayBuilder, ListBuffer}
import scala.math._
import scala.reflect.ClassTag

object LoadSigma0 {

//  val ROOT_PATH = Paths.get("/", "data", "users", "Private", "nielsh", "PNG")
  val ROOT_PATH = Paths.get("/", "home", "niels", "Data", "PNG")
  val COLOR_MAP = Paths.get("/", "home", "niels", "Data", "styles_ColorTable_FAPAR_V12.sld")

  def renderPng(productType: String, date: LocalDate, colorMap: Option[String] = None): Unit = {
    val layout = GlobalLayout(256, 14, 0.1)
    val layoutDefWithZoom = layout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))

    implicit val sc: SparkContext =
      SparkContext.getOrCreate(
        new SparkConf()
          .setMaster("local[8]")
          .setAppName("Geotiffloading")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max", "1024m"))
    
    getRddAndRender(productType, date, layoutDefWithZoom._1, layout.zoom, colorMap)(partitions = 500)
  }

  private def getRddAndRender(productType: String, date: LocalDate, layout: LayoutDefinition, zoom: Int, colorMap: Option[String])
                             (partitions: Int)
                             (implicit sc: SparkContext): Unit = {
    colorMap match {
      case Some(name) =>
        val map = ColorMapParser.parse(name)
        getSinglebandRDD(productType, date, layout)
          .repartition(partitions)
          .foreach(renderSinglebandRDD(zoom, map))
      case None =>
        getMultibandRDD(productType, date, layout)
          .repartition(partitions)
          .foreach(renderMultibandRDD(zoom))
    }
  }

  private def getMultibandRDD(productType: String, date: LocalDate, layout: LayoutDefinition)(implicit sc: SparkContext) = {
    val (sourcePathsVV, sourcePathsVH) = multibandSourcePathsForDate(productType, date)
    val (sourcesVV, sourcesVH) = (reproject(sourcePathsVV, layout), reproject(sourcePathsVH, layout))

    implicit val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[8]").setAppName("Geotiffloading")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "1024m"))
 
    loadMultibandTiles(sourcesVV, sourcesVH, layout, date)
  }
  
  private def getSinglebandRDD(productType: String, date: LocalDate, layout: LayoutDefinition)(implicit sc: SparkContext) = {
    val sourcePaths = singlebandSourcePathsForDate(productType, date)
    val sources = reproject(sourcePaths, layout)
    
    loadSinglebandTiles(sources, layout, date)
  }

  private def reproject(sourcePaths: List[String], layout: LayoutDefinition) = {
    //in the case of global layout, we need to warp input into the right format
    sourcePaths.map(p => GDALReprojectRasterSource(p, WebMercator,
      Reproject.Options(targetCellSize = Some(layout.cellSize))))
  }

  private def renderMultibandRDD(zoom: Int)(item: (SpaceTimeKey, (Iterable[RasterRegion], Iterable[RasterRegion]))) {
    item match {
      case (key, (vvRegions, vhRegions)) =>
        val tileR = regionsToTile(vvRegions)
        val tileG = regionsToTile(vhRegions)

        val multibandTile = tilesToArrayMultibandTile(tileR, tileG)
        multibandTile.renderPng().write(pathForTile(ROOT_PATH, key, zoom))
    }
  }
  
  private def renderSinglebandRDD(zoom: Int, colorMap: ColorMap)(item: (SpaceTimeKey, Iterable[RasterRegion])) {
    item match {
      case (key, regions) =>
        val tile = regionsToTile(regions)
        val coloredTile = tile.color(colorMap)
        coloredTile.renderPng().write(pathForTile(ROOT_PATH, key, zoom))
    }
  }
  
  private def multibandSourcePathsForDate(productType: String, date: LocalDate) = {
    val catalog = new CatalogClient()
    val products = catalog.getProducts(productType, date, date, "GEOTIFF")

    val paths = pathsFromProducts(products, "VV", "VH")
    (paths(0), paths(1))
  }
  
  private def singlebandSourcePathsForDate(productType: String, date: LocalDate) = {
    val catalog = new CatalogClient()
    val products = catalog.getProducts(productType, date, date, "GEOTIFF")
    
    pathsFromProducts(products, "FAPAR").head
  }
  
  private def pathsFromProducts(products: util.Collection[_ <: EOProduct], bands: String*) = {
    val result = new ListBuffer[List[String]]
    
    bands.foreach(b => {
      val paths = new ListBuffer[String]
      products.asScala.foreach(p => {
        p.getFiles.asScala.foreach(f => {
          if (f.getBands.contains(b)) {
            paths += f.getFilename.getPath
          }
        })
      })
      result += paths.toList
    })
    
    result.toList
  }

  private def loadMultibandTiles(sourcesVV: Seq[RasterSource], sourcesVH: Seq[RasterSource], layout: LayoutDefinition, date: LocalDate)
                                (implicit sc: SparkContext):
                                RDD[(SpaceTimeKey, (Iterable[RasterRegion], Iterable[RasterRegion]))]
                                with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    val layerMetadata = getLayerMetadata(sourcesVV ++ sourcesVH, layout)

    val vvRegionRDD: RDD[(SpaceTimeKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sourcesVV, layout, date)
    val vhRegionRDD: RDD[(SpaceTimeKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sourcesVH, layout, date)

    val regionRDD: RDD[(SpaceTimeKey, (Iterable[RasterRegion], Iterable[RasterRegion]))] = vvRegionRDD.join(vhRegionRDD)

    ContextRDD(regionRDD, layerMetadata)
  }
  
  private def loadSinglebandTiles(sources: List[RasterSource], layout: LayoutDefinition, date: LocalDate)(implicit sc: SparkContext) = {
    
    val layerMetadata = getLayerMetadata(sources, layout)

    val regionRDD: RDD[(SpaceTimeKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sources, layout, date)

    ContextRDD(regionRDD, layerMetadata)
  }
  
  private def getLayerMetadata(sources: Seq[RasterSource], layout: LayoutDefinition) = {
    val cellTypes = sources.map { _.cellType }.toSet
    val projections = sources.map { _.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val spacetimeBounds = KeyBounds(SpaceTimeKey(layerKeyBounds._1, ZonedDateTime.now()), SpaceTimeKey(layerKeyBounds._2, ZonedDateTime.now()))

    TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, spacetimeBounds)
  }

  private def rasterRegionRDDFromSources(sources: Seq[RasterSource], layout: LayoutDefinition, date: LocalDate)
                                        (implicit sc: SparkContext): RDD[(SpaceTimeKey, Iterable[RasterRegion])] = {
    
    val rdd = sc.parallelize(sources).flatMap { source =>
      val tileSource = new LayoutTileSource(source, layout)
      tileSource.keys().flatMap { key =>
        try {
          val region = tileSource.rasterRegionForKey(key)

          Some(SpaceTimeKey(key, date.atStartOfDay(ZoneId.of("UTC"))), region)
        } catch {
          case _: IllegalArgumentException => None
        }
      }
    }
    rdd.groupByKey()
  }

  private def regionsToTile(regions: Iterable[RasterRegion]): Tile = {
    mapToSingleTile(regions.map(r => r.raster.get.tile.band(0)))
  }

  private def mapToSingleTile(tiles: Iterable[Tile]): Tile = {
    if (tiles.size > 1) {
      val nonNullTiles = tiles.filter(t => t.toArrayDouble().exists(d => !isNoData(d)))
      if (nonNullTiles.isEmpty) {
        tiles.head
      } else if (nonNullTiles.size == 1) {
        nonNullTiles.head
      } else {
        val tile1 = nonNullTiles.head.toArrayTile()
        val tile2 = nonNullTiles.tail.head.toArrayTile()
        
        tile1.combineDouble(tile2)((t1, t2) => {
          if (isNoData(t1)) 
            t2
          else if (isNoData(t2)) 
            t1
          else 
            (t1 + t2) / 2
        })
      }
    } else {
      tiles.head
    }
  }

  private def tilesToArrayMultibandTile(tileR: Tile, tileG: Tile): ArrayMultibandTile = {

    def logTile(tile: Tile): Tile = {
      tile.mapDouble(d => 10 * log10(d))
    }

    def convert(tile: Tile) = {
      tile.convert(FloatConstantNoDataCellType)
    }

    val tileB = tileR.toArrayTile().combineDouble(tileG.toArrayTile())((a, b) => a / b)

    val normTileR = convert(logTile(tileR)).normalize(-25, 3, 0, 255)
    val normTileG = convert(logTile(tileG)).normalize(-30, -2, 0, 255)
    val normTileB = convert(tileB).normalize(0.2, 1, 0, 255)
    
    ArrayMultibandTile(normTileR, normTileG, normTileB)
  }

  private def pathForTile(rootPath: Path, key: SpaceTimeKey, zoom: Int): String = {
    val grid = "g"
    val dateStr = key.time.format(DateTimeFormatter.ISO_LOCAL_DATE)

    val z = zoom.formatted("%02d")

    val x = key.col.formatted("%09d")
    val x2 = x.substring(0, 3)
    val x1 = x.substring(3, 6)
    val x0 = x.substring(6)

    val invertedRow = math.pow(2, zoom).toInt - 1 - key.row
    val y = invertedRow.formatted("%09d")
    val y2 = y.substring(0, 3)
    val y1 = y.substring(3, 6)
    val y0 = y.substring(6)

    val dir = ROOT_PATH.resolve(Paths.get(grid, dateStr, z, x2, x1, x0, y2, y1))
    dir.toFile.mkdirs()
    dir.resolve(y0 + ".png").toString
  }

  def loadRasterRegions(sources: Seq[RasterSource],
                        layout: LayoutDefinition,
                        partitionBytes: Long = PARTITION_BYTES
                       )(implicit sc: SparkContext): RDD[(SpaceTimeKey, RasterRegion)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    val cellTypes = sources.map { _.cellType }.toSet
    //require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = sources.map { _.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val spacetimeBounds = KeyBounds(SpaceTimeKey(layerKeyBounds._1,ZonedDateTime.now()),SpaceTimeKey(layerKeyBounds._2,ZonedDateTime.now()))

    val layerMetadata =
      TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, spacetimeBounds)

    val sourcesRDD: RDD[(RasterSource, Array[SpatialKey])] =
      sc.parallelize(sources).flatMap { source =>
        val keys: Traversable[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              layout.mapTransform.keysForGeometry(intersection.toPolygon)
            case None =>
              Seq.empty[SpatialKey]
          }
        val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
        partition(keys, partitionBytes)( _ => tileSize).map { res => (source, res) }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size)
        sourcesRDD//.repartition(count)
      else
        sourcesRDD
    }

    val result: RDD[(SpaceTimeKey, RasterRegion)] =
      repartitioned.flatMap { case (source, keys) =>
        val date = LocalDate.from(DateTimeFormatter.BASIC_ISO_DATE.parse(source.uri.split("_DV_")(1).substring(0,8)))
        val tileSource = new LayoutTileSource(source, layout)
        tileSource.keyedRasterRegions().map(key => (SpaceTimeKey(key._1,date.atStartOfDay(ZoneId.of("UTC"))),key._2))
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def partition[T: ClassTag](
                              chunks: Traversable[T],
                              maxPartitionSize: Long
                            )(chunkSize: T => Long = { c: T => 1l }): Array[Array[T]] = {
    if (chunks.isEmpty) {
      Array[Array[T]]()
    } else {
      val partition = ArrayBuilder.make[T]
      partition.sizeHintBounded(128, chunks)
      var partitionSize: Long = 0l
      var partitionCount: Long = 0l
      val partitions = ArrayBuilder.make[Array[T]]

      def finalizePartition() {
        val res = partition.result
        if (res.nonEmpty) partitions += res
        partition.clear()
        partitionSize = 0l
        partitionCount = 0l
      }

      def addToPartition(chunk: T) {
        partition += chunk
        partitionSize += chunkSize(chunk)
        partitionCount += 1
      }

      for (chunk <- chunks) {
        if ((partitionCount == 0) || (partitionSize + chunkSize(chunk)) < maxPartitionSize)
          addToPartition(chunk)
        else {
          finalizePartition()
          addToPartition(chunk)
        }
      }

      finalizePartition()
      partitions.result
    }
  }

  def createRDD(layername: String, envelope: Extent, str: String, startDate: Option[ZonedDateTime], endDate: Option[ZonedDateTime]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    val layout = new GlobalLayout(256,14,0.1)
    val localLayout = new LocalLayout(256,256)

    val utm31 = CRS.fromEpsgCode(32631)

    val source = new geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource("/home/niels/Data/S1B_IW_GRDH_SIGMA0_DV_20180502T054914_DESCENDING_37_B39C_V110_VV.tif")
    val source2 = new geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource("/home/niels/Data/S1B_IW_GRDH_SIGMA0_DV_20181224T173153_ASCENDING_161_42B7_V110_VV.tif")
    //in the case of global layout, we need to warp input into the right format
    //    val source = new geotrellis.contrib.vlm.gdal.GDALRasterSource("/home/driesj/alldata/CGS_S1/S1B_IW_GRDH_SIGMA0_DV_20181216T054946_DESCENDING_37_130C_V110_VH.tif")
    //    val secondfile = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2018/12/09/S1B_IW_GRDH_SIGMA0_DV_20181209T055749_DESCENDING_110_520B_V110/S1B_IW_GRDH_SIGMA0_DV_20181209T055749_DESCENDING_110_520B_V110_VH.tif"
    //    val source2 = new geotrellis.contrib.vlm.gdal.GDALRasterSource(secondfile)
    val croppedSource = source.resampleToRegion(RasterExtent(envelope,source.cellSize))
    val localLayoutDefWithZoom = localLayout.layoutDefinitionWithZoom(utm31,envelope,croppedSource.cellSize)
    val conf = new SparkConf().setMaster("local[4]").setAppName("Geotiffloading")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","1024m")
    implicit val sc = SparkContext.getOrCreate()

    var rdd = loadRasterRegions(Seq(source,source2.resampleToRegion(RasterExtent(envelope,source.cellSize))),localLayoutDefWithZoom._1)

    val tiledLayer = rdd.withContext(r => r.mapValues(f => f.raster.get.tile))

    return tiledLayer

  }

  def testS1(): Unit = {
    val productType = "CGS_S1_GRD_SIGMA0_L1"
    val date = LocalDate.of(2018, 5, 2)
    
    renderPng(productType, date)
  }
  
  def testS2(): Unit = {
    val productType = "CGS_S2_FAPAR_10M"
    val date = LocalDate.of(2018, 5, 31)
    
    renderPng(productType, date, Some(COLOR_MAP.toString))
  }
  
  def main(args: Array[String]): Unit = {

    LoadSigma0.testS1()
    LoadSigma0.testS2()
    
  }

}
