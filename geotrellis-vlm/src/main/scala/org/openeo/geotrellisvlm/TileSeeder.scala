package org.openeo.geotrellisvlm

import java.nio.file.{Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util

import be.vito.eodata.biopar.EOProduct
import be.vito.eodata.catalog.CatalogClient
import com.beust.jcommander.JCommander
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal.GDALReprojectRasterSource
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.render.ColorMap
import geotrellis.raster.reproject.Reproject
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, Metadata, SpaceTimeKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ListBuffer
import scala.math._

object TileSeeder {

  private val ROOT_PATH = Paths.get("/", "data", "tiles")
  
  private var logger: Logger = new EmptyLogger

  def renderPng(productType: String, date: LocalDate, colorMap: Option[String] = None)(implicit sc: SparkContext): Unit = {
    val layout = GlobalLayout(256, 14, 0.1)
    val layoutDefWithZoom = layout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))

    getRddAndRender(productType, date, layoutDefWithZoom._1, layout.zoom, colorMap)(partitions = 500)
  }

  private def getRddAndRender(productType: String, date: LocalDate, layout: LayoutDefinition, zoom: Int, colorMap: Option[String])
                             (partitions: Int)
                             (implicit sc: SparkContext): Unit = {

    val catalog = new CatalogClient()
    val products = catalog.getProducts(productType, date, date, "GEOTIFF")
    
    colorMap match {
      case Some(name) =>
        val map = ColorMapParser.parse(name)
        getSinglebandRDD(products, date, layout)
          .repartition(partitions)
          .foreach(renderSinglebandRDD(productType, zoom, map))
      case None =>
        getMultibandRDD(products, date, layout)
          .repartition(partitions)
          .foreach(renderMultibandRDD(productType, zoom))
    }
  }

  private def getMultibandRDD(products: util.Collection[_ <: EOProduct], date: LocalDate, layout: LayoutDefinition)
                             (implicit sc: SparkContext) = {

    val r = reproject(_: List[String], layout)
    val (sourcePathsVV, sourcePathsVH) = multibandSourcePathsForDate(products)
    val (sourcesVV, sourcesVH) = (r(sourcePathsVV), r(sourcePathsVH))

    loadMultibandTiles(sourcesVV, sourcesVH, layout, date)
  }
  
  private def getSinglebandRDD(products: util.Collection[_ <: EOProduct], date: LocalDate, layout: LayoutDefinition)
                              (implicit sc: SparkContext) = {
    
    val sourcePaths = singlebandSourcePathsForDate(products)
    val sources = reproject(sourcePaths, layout)
    
    loadSinglebandTiles(sources, layout, date)
  }

  private def reproject(sourcePaths: List[String], layout: LayoutDefinition) = {
    //in the case of global layout, we need to warp input into the right format
    sourcePaths.map {
      GDALReprojectRasterSource(_, WebMercator, Reproject.Options(targetCellSize = Some(layout.cellSize)))
    }
  }

  private def renderSinglebandRDD(productType: String, zoom: Int, colorMap: ColorMap)(item: (SpaceTimeKey, Iterable[RasterRegion])) {
    item match {
      case (key, regions) =>
        logger.logKey(key.spatialKey)
        

        val tile = regionsToTile(regions).convert(UByteConstantNoDataCellType).convert(IntUserDefinedNoDataCellType(255))
        if (!tile.isNoDataTile) {
          val path = pathForTile(ROOT_PATH, productType, key, zoom)
          tile.toArrayTile().renderPng(colorMap).write(path)

          logger.logTile(key.spatialKey, path)
        } else {
          logger.logNoDataTile(key.spatialKey)
        }
    }
  }

  private def renderMultibandRDD(productType: String, zoom: Int)(item: (SpaceTimeKey, (Iterable[RasterRegion], Iterable[RasterRegion]))) {
    item match {
      case (key, (vvRegions, vhRegions)) =>
        logger.logKey(key.spatialKey)
        
        val tileR = regionsToTile(vvRegions)
        val tileG = regionsToTile(vhRegions)

        if (!tileR.isNoDataTile || !tileG.isNoDataTile) {
          val path = pathForTile(ROOT_PATH, productType, key, zoom)
          tilesToArrayMultibandTile(tileR, tileG).renderPng().write(path)

          logger.logTile(key.spatialKey, path)
        } else {
          logger.logNoDataTile(key.spatialKey)
        }
    }
  }
  
  private def multibandSourcePathsForDate(products: util.Collection[_ <: EOProduct]) = {
    val paths = pathsFromProducts(products, "VV", "VH")
    (paths(0), paths(1))
  }
  
  private def singlebandSourcePathsForDate(products: util.Collection[_ <: EOProduct]) = {
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

    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }
    val layerKeyBounds = KeyBounds(layout.mapTransform(combinedExtents))
    val spacetimeBounds = KeyBounds(SpaceTimeKey(layerKeyBounds._1, ZonedDateTime.now()), SpaceTimeKey(layerKeyBounds._2, ZonedDateTime.now()))

    TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, spacetimeBounds)
  }

  private def rasterRegionRDDFromSources(sources: Seq[RasterSource], layout: LayoutDefinition, date: LocalDate)
                                        (implicit sc: SparkContext): RDD[(SpaceTimeKey, Iterable[RasterRegion])] = {
    
    val rdd = sc.parallelize(sources).flatMap { source =>
      val tileSource = new LayoutTileSource(source, layout)
      tileSource.keys.flatMap { key =>
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
      val nonNullTiles = tiles.filter(t => !t.isNoDataTile)
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
      tile.mapDouble(10 * log10(_))
    }

    def convert(tile: Tile) = {
      tile.convert(FloatConstantNoDataCellType)
    }

    val tileB = tileR.toArrayTile().combineDouble(tileG.toArrayTile())(_ / _)

    val normTileR = convert(logTile(tileR)).normalize(-25, 3, 0, 255)
    val normTileG = convert(logTile(tileG)).normalize(-30, -2, 0, 255)
    val normTileB = convert(tileB).normalize(0.2, 1, 0, 255)
    
    ArrayMultibandTile(normTileR, normTileG, normTileB)
  }

  private def pathForTile(rootPath: Path, productType: String, key: SpaceTimeKey, zoom: Int): String = {
    val grid = "g"
    val dateStr = key.time.format(DateTimeFormatter.ISO_LOCAL_DATE)

    val z = f"$zoom%02d"

    val x = f"${key.col}%09d"
    val x2 = x.substring(0, 3)
    val x1 = x.substring(3, 6)
    val x0 = x.substring(6)

    val invertedRow = math.pow(2, zoom).toInt - 1 - key.row
    val y = f"$invertedRow%09d"
    val y2 = y.substring(0, 3)
    val y1 = y.substring(3, 6)
    val y0 = y.substring(6)

    val dir = resolvePath(rootPath, productType).resolve(Paths.get(grid, dateStr, z, x2, x1, x0, y2, y1))
    dir.toFile.mkdirs()
    dir.resolve(y0 + ".png").toString
  }

  private def resolvePath(path: Path, productType: String) = {
    path.resolve(s"tiles_$productType").resolve(productType)
  
  def verboseLogging() {
    logger match {
      case _: VerboseLogger => 
      case _ => logger = new VerboseLogger
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "Geotiffloading"

    val jCommanderArgs = new JCommanderArgs
    val jCommander = new JCommander(jCommanderArgs, args: _*)
    jCommander.setProgramName(appName)

    if (jCommanderArgs.help) {
      jCommander.usage()
    } else {
      implicit val sc: SparkContext =
        SparkContext.getOrCreate(
          new SparkConf()
            .setAppName("Geotiffloading")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer.max", "1024m"))

      if (!jCommanderArgs.verbose) {
        verboseLogging()
      }

      val date = jCommanderArgs.date
      val productType = jCommanderArgs.productType
      val colorMap = jCommanderArgs.colorMap
      
      renderPng(productType, date, colorMap)
    }
  }
}
