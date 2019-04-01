package org.openeo.geotrellisvlm

import java.nio.file.{Path, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import be.vito.eodata.biopar.EOProduct
import be.vito.eodata.catalog.CatalogClient
import com.beust.jcommander.JCommander
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff.GeoTiffReprojectRasterSource
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.render.ColorMap
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, Metadata, SpatialKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ListBuffer
import scala.math._

object TileSeeder {
  
  private var logger: Logger = new EmptyLogger

  private val ROOT_PATH = Paths.get("/", "data", "tiles")

  private val globalLayout = GlobalLayout(256, 14, 0.1)
  private val layout = globalLayout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))._1
  
  private val partitions = 500

  def renderPng(productType: String, date: LocalDate, colorMap: Option[String] = None)(implicit sc: SparkContext): Unit = {
  
    val catalog = new CatalogClient()
    val products = catalog.getProducts(productType, date, date, "GEOTIFF")
    
    colorMap match {
      case Some(name) =>
        val map = ColorMapParser.parse(name)
        getSinglebandRDD(products, date)
          .repartition(partitions)
          .foreach(renderSinglebandRDD(productType, date, map))
      case None =>
        getMultibandRDD(products, date)
          .repartition(partitions)
          .foreach(renderMultibandRDD(productType, date))
    }
  }

  private def getMultibandRDD(products: util.Collection[_ <: EOProduct], date: LocalDate)
                             (implicit sc: SparkContext) = {

    val r = reproject(_: List[String], layout)
    val (sourcePathsVV, sourcePathsVH) = multibandSourcePathsForDate(products)
    val (sourcesVV, sourcesVH) = (r(sourcePathsVV), r(sourcePathsVH))

    loadMultibandTiles(sourcesVV, sourcesVH, layout, date)
  }
  
  private def getSinglebandRDD(products: util.Collection[_ <: EOProduct], date: LocalDate)
                              (implicit sc: SparkContext): RDD[(SpatialKey, Iterable[RasterRegion])] with Metadata[TileLayerMetadata[SpatialKey]] = {

    val sourcePaths = singlebandSourcePathsForDate(products)

    getSinglebandRDD(sourcePaths, date)
  }
  
  private def getSinglebandRDD(sourcePaths: List[String], date: LocalDate)(implicit sc: SparkContext): RDD[(SpatialKey, Iterable[RasterRegion])] with Metadata[TileLayerMetadata[SpatialKey]] = {
    
    val sources = reproject(sourcePaths, layout)
    
    loadSinglebandTiles(sources, layout, date)
  }

  private def reproject(sourcePaths: List[String], layout: LayoutDefinition) = {
    sourcePaths.map(GeoTiffReprojectRasterSource(_, WebMercator))
  }

  private def renderSinglebandRDD(productType: String, date: LocalDate, colorMap: ColorMap)(item: (SpatialKey, Iterable[RasterRegion])) {
    item match {
      case (key, regions) =>
        logger.logKey(key)

        val tile = regionsToTile(regions).convert(UByteUserDefinedNoDataCellType(-1))
        if (!tile.isNoDataTile) {
          val path = pathForTile(ROOT_PATH, productType, date, key, globalLayout.zoom)
          tile.toArrayTile().renderPng(colorMap).write(path)

          logger.logTile(key, path)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }

  private def renderMultibandRDD(productType: String, date: LocalDate)(item: (SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion]))) {
    item match {
      case (key, (vvRegions, vhRegions)) =>
        logger.logKey(key)

        val tileR = regionsToTile(vvRegions)
        val tileG = regionsToTile(vhRegions)

        if (!tileR.isNoDataTile && !tileG.isNoDataTile) {
          val path = pathForTile(ROOT_PATH, productType, date, key, globalLayout.zoom)
          tilesToArrayMultibandTile(tileR, tileG).renderPng().write(path)

          logger.logTile(key, path)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }
  
  private def multibandSourcePathsForDate(products: util.Collection[_ <: EOProduct]) = {
    val paths = pathsFromProducts(products, "VV", "VH")
    (paths(0), paths(1))
  }
  
  private def singlebandSourcePathsForDate(products: util.Collection[_ <: EOProduct]) = {
    pathsFromProducts(products, "").head
  }
  
  private def pathsFromProducts(products: util.Collection[_ <: EOProduct], bands: String*) = {
    val result = new ListBuffer[List[String]]
    
    bands.foreach(b => {
      val paths = new ListBuffer[String]
      products.asScala.foreach(p => {
        p.getFiles.asScala.foreach(f => {
          if (b.isEmpty || f.getBands.contains(b)) {
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
                                RDD[(SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion]))]
                                with Metadata[TileLayerMetadata[SpatialKey]] = {

    val layerMetadata = getLayerMetadata(sourcesVV ++ sourcesVH, layout)

    val vvRegionRDD: RDD[(SpatialKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sourcesVV, layout, date)
    val vhRegionRDD: RDD[(SpatialKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sourcesVH, layout, date)

    val regionRDD: RDD[(SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion]))] = vvRegionRDD.join(vhRegionRDD)

    ContextRDD(regionRDD, layerMetadata)
  }
  
  private def loadSinglebandTiles(sources: List[RasterSource], layout: LayoutDefinition, date: LocalDate)(implicit sc: SparkContext) = {
    
    val layerMetadata = getLayerMetadata(sources, layout)

    val regionRDD: RDD[(SpatialKey, Iterable[RasterRegion])] = rasterRegionRDDFromSources(sources, layout, date)

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

    TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)
  }

  private def rasterRegionRDDFromSources(sources: Seq[RasterSource], layout: LayoutDefinition, date: LocalDate)
                                        (implicit sc: SparkContext): RDD[(SpatialKey, Iterable[RasterRegion])] = {
    
    val rdd = sc.parallelize(sources).flatMap { source =>
      val tileSource = new LayoutTileSource(source, layout)
      tileSource.keys.flatMap { key =>
        try {
          val region = tileSource.rasterRegionForKey(key).get

          Some(key, region)
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
  

  private def pathForTile(rootPath: Path, productType: String, date: LocalDate, key: SpatialKey, zoom: Int): String = {
    val grid = "g"
    val dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE)

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
  }
  
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
