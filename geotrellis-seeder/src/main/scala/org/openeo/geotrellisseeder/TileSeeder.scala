package org.openeo.geotrellisseeder

import java.io.File
import java.nio.file.Paths
import java.time.LocalDate

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

case class TileSeeder(zoomLevel: Int, partitions: Int, verbose: Boolean) {

  private val logger = if (verbose) VerboseLogger(classOf[TileSeeder]) else StandardLogger(classOf[TileSeeder])

  def renderSinglePng(productType: String, date: LocalDate, key: SpatialKey, path: String, colorMap: Option[String] = None,
                      bands: Option[Array[Band]] = None)(implicit sc: SparkContext): Unit = {

    renderPng(path, productType, date.toString, colorMap, bands, Some(key))
  }

  def renderPng(path: String, productType: String, dateStr: String, colorMap: Option[String] = None, bands: Option[Array[Band]] = None,
                spatialKey: Option[SpatialKey] = None)(implicit sc: SparkContext): Unit = {

    val date = LocalDate.parse(dateStr.substring(0, 10))

    val catalog = new CatalogClient()
    val products = catalog.getProducts(productType, date, date, "GEOTIFF").asScala
    
    logger.logProducts(products)

    val globalLayout = GlobalLayout(256, zoomLevel, 0.1)

    implicit val layout: LayoutDefinition =
      globalLayout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))._1

    if (colorMap.isDefined) {
      val map = ColorMapParser.parse(colorMap.get)
      getSinglebandRDD(products, date, spatialKey)
        .repartition(partitions)
        .foreach(renderSinglebandRDD(path, dateStr, map, zoomLevel))
    } else if (bands.isDefined) {
      getMultibandRDD(products, date, bands.get.map(_.name), spatialKey)
        .repartition(partitions)
        .foreach(renderMultibandRDD(path, dateStr, bands.get, zoomLevel))
    } else {
      getMultibandRDD(products, date, spatialKey)
        .repartition(partitions)
        .foreach(renderMultibandRDD(path, dateStr, zoomLevel))
    }
  }

  private def getMultibandRDD(products: Iterable[_ <: EOProduct], date: LocalDate, bands: Array[String], spatialKey: Option[SpatialKey])
                             (implicit sc: SparkContext, layout: LayoutDefinition) = {

    val sources = multibandSourcePathsForDate(products, bands).map(reproject)

    loadMultibandTiles(sources, date, spatialKey)
  }

  private def getMultibandRDD(products: Iterable[_ <: EOProduct], date: LocalDate, spatialKey: Option[SpatialKey])
                             (implicit sc: SparkContext, layout:LayoutDefinition) = {

    val (sourcePathsVV, sourcePathsVH) = multibandSourcePathsForDate(products)
    val (sourcesVV, sourcesVH) = (reproject(sourcePathsVV), reproject(sourcePathsVH))

    loadMultibandTiles(sourcesVV, sourcesVH, date, spatialKey)
  }

  private def getSinglebandRDD(products: Iterable[_ <: EOProduct], date: LocalDate, spatialKey: Option[SpatialKey])
                              (implicit sc: SparkContext, layout: LayoutDefinition): RDD[(SpatialKey, Iterable[RasterRegion])] with Metadata[TileLayerMetadata[SpatialKey]] = {

    val sourcePaths = singlebandSourcePathsForDate(products)
    val sources = reproject(sourcePaths)

    loadSinglebandTiles(sources, date, spatialKey)
  }

  private def reproject(sourcePaths: List[String]) = {
    sourcePaths.map(GeoTiffReprojectRasterSource(_, WebMercator))
  }

  private def renderSinglebandRDD(path: String, dateStr: String, colorMap: ColorMap, zoom: Int)(item: (SpatialKey, Iterable[RasterRegion])) {
    item match {
      case (key, regions) =>
        logger.logKey(key)

        val tile = regionsToTile(regions).convert(UByteUserDefinedNoDataCellType(-1))
        if (!tile.isNoDataTile) {
          val tilePath = pathForTile(path, dateStr, key, zoom)
          
          deleteSymLink(tilePath)
          
          tile.toArrayTile().renderPng(colorMap).write(tilePath)

          logger.logTile(key, tilePath)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }

  private def renderMultibandRDD(path: String, dateStr: String, bands: Array[Band], zoom: Int)(item: (SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion], Iterable[RasterRegion]))): Unit = {
    item match {
      case (key, (rRegions, gRegions, bRegions)) =>
        logger.logKey(key)

        val tileR = regionsToTile(rRegions)
        val tileG = regionsToTile(gRegions)
        val tileB = regionsToTile(bRegions)

        if (!tileR.isNoDataTile && !tileG.isNoDataTile && !tileB.isNoDataTile) {
          val tilePath = pathForTile(path, dateStr, key, zoom)
          
          deleteSymLink(tilePath)

          tilesToArrayMultibandTile(tileR, tileG, tileB, bands).renderPng().write(tilePath)

          logger.logTile(key, tilePath)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }

  private def renderMultibandRDD(path: String, dateStr: String, zoom: Int)(item: (SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion]))) {
    item match {
      case (key, (vvRegions, vhRegions)) =>
        logger.logKey(key)

        val tileR = regionsToTile(vvRegions)
        val tileG = regionsToTile(vhRegions)

        if (!tileR.isNoDataTile && !tileG.isNoDataTile) {
          val tilePath = pathForTile(path, dateStr, key, zoom)
          
          deleteSymLink(tilePath)
          
          tilesToArrayMultibandTile(tileR, tileG).renderPng().write(tilePath)

          logger.logTile(key, tilePath)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }
  
  private def deleteSymLink(path: String) {
    val file = new File(path)
    
    if (file.getAbsolutePath != file.getCanonicalPath || !file.exists())
      file.delete()
  }

  private def multibandSourcePathsForDate(products: Iterable[_ <: EOProduct], bands: Array[String]) = {
    pathsFromProducts(products, bands: _*).toArray
  }

  private def multibandSourcePathsForDate(products: Iterable[_ <: EOProduct]) = {
    val paths = pathsFromProducts(products, "VV", "VH")
    (paths(0), paths(1))
  }

  private def singlebandSourcePathsForDate(products: Iterable[_ <: EOProduct]) = {
    pathsFromProducts(products, "").head
  }

  private def pathsFromProducts(products: Iterable[_ <: EOProduct], bands: String*) = {
    val result = new ListBuffer[List[String]]

    bands.foreach(b => {
      val paths = new ListBuffer[String]
      products.foreach(p => {
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

  private def loadMultibandTiles(sources: Array[_ <: Seq[RasterSource]], date: LocalDate, spatialKey: Option[SpatialKey])
                                (implicit sc: SparkContext, layout: LayoutDefinition):
                                RDD[(SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion], Iterable[RasterRegion]))]
                                with Metadata[TileLayerMetadata[SpatialKey]] = {

    val allSources = sources.foldLeft(Seq[RasterSource]())((r1: Seq[RasterSource], r2: Seq[RasterSource]) => r1 ++ r2)
    val layerMetadata = getLayerMetadata(allSources)

    val rRegionRDD = rasterRegionRDDFromSources(sources(0), date, spatialKey)
    val gRegionRDD = rasterRegionRDDFromSources(sources(1), date, spatialKey)
    val bRegionRDD = rasterRegionRDDFromSources(sources(2), date, spatialKey)

    val regionRDD = rRegionRDD.join(gRegionRDD).join(bRegionRDD).mapValues(v => (v._1._1, v._1._2, v._2))

    ContextRDD(regionRDD, layerMetadata)
  }

  private def loadMultibandTiles(sourcesVV: Seq[RasterSource], sourcesVH: Seq[RasterSource], date: LocalDate, spatialKey: Option[SpatialKey])
                                (implicit sc: SparkContext, layout: LayoutDefinition):
                                RDD[(SpatialKey, (Iterable[RasterRegion], Iterable[RasterRegion]))]
                                with Metadata[TileLayerMetadata[SpatialKey]] = {

    val layerMetadata = getLayerMetadata(sourcesVV ++ sourcesVH)

    val vvRegionRDD = rasterRegionRDDFromSources(sourcesVV, date, spatialKey)
    val vhRegionRDD = rasterRegionRDDFromSources(sourcesVH, date, spatialKey)

    val regionRDD = vvRegionRDD.join(vhRegionRDD)

    ContextRDD(regionRDD, layerMetadata)
  }

  private def loadSinglebandTiles(sources: List[RasterSource], date: LocalDate, spatialKey: Option[SpatialKey])(implicit sc: SparkContext, layout: LayoutDefinition) = {

    val layerMetadata = getLayerMetadata(sources)

    val regionRDD = rasterRegionRDDFromSources(sources, date, spatialKey)

    ContextRDD(regionRDD, layerMetadata)
  }

  private def getLayerMetadata(sources: Seq[RasterSource])(implicit layout: LayoutDefinition) = {
    val cellTypes = sources.map {
      _.cellType
    }.toSet
    val projections = sources.map {
      _.crs
    }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val combinedExtents = sources.map {
      _.extent
    }.reduce {
      _ combine _
    }
    val layerKeyBounds = KeyBounds(layout.mapTransform(combinedExtents))

    TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)
  }

  private def rasterRegionRDDFromSources(sources: Seq[RasterSource], date: LocalDate, spatialKey: Option[SpatialKey])
                                        (implicit sc: SparkContext, layout: LayoutDefinition): RDD[(SpatialKey, Iterable[RasterRegion])] = {

    val rdd = sc.parallelize(sources).flatMap { source =>
      val tileSource = source.tileToLayout(layout)
      val keys = spatialKey match {
        case Some(k) => tileSource.keys.filter(_ == k)
        case None => tileSource.keys
      }
      keys.flatMap { key =>
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
        nonNullTiles.reduce(_.combine(_)((t1, t2) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)))
      }
    } else {
      tiles.head
    }
  }

  private def tilesToArrayMultibandTile(tileR: Tile, tileG: Tile, tileB: Tile, bands: Array[Band]): ArrayMultibandTile = {
    def norm(tile: Tile, oldMin: Int, oldMax: Int, newMin: Int, newMax: Int) = {
      tile.normalize(oldMin, oldMax, newMin, newMax).map(i => if (i > newMax) newMax else if (i < newMin && !isNoData(i)) newMin else i)
    }

    val normTileR = norm(tileR, bands(0).min, bands(0).max, 1, 255)
    val normTileG = norm(tileG, bands(1).min, bands(1).max, 1, 255)
    val normTileB = norm(tileB, bands(2).min, bands(2).max, 1, 255)

    ArrayMultibandTile(normTileR, normTileG, normTileB)
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


  private def pathForTile(path: String, dateStr: String, key: SpatialKey, zoom: Int): String = {
    if (path.endsWith(".png")) {
      path
    } else {
      val grid = "g"

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

      val dir = Paths.get(path).resolve(Paths.get(grid, dateStr, z, x2, x1, x0, y2, y1))
      dir.toFile.mkdirs()
      dir.resolve(y0 + ".png").toString
    }
  }
}

object TileSeeder {
  def main(args: Array[String]): Unit = {
    val appName = "GeotrellisSeeder"

    val jCommanderArgs = new JCommanderArgs
    val jCommander = new JCommander(jCommanderArgs, args: _*)
    jCommander.setProgramName(appName)

    if (jCommanderArgs.help) {
      jCommander.usage()
    } else {
      val date = jCommanderArgs.date
      val productType = jCommanderArgs.productType
      val layer = jCommanderArgs.layer
      val rootPath = jCommanderArgs.rootPath
      val zoomLevel = jCommanderArgs.zoomLevel
      val colorMap = jCommanderArgs.colorMap
      val bands = jCommanderArgs.bands
      val verbose = jCommanderArgs.verbose

      val seeder = new TileSeeder(zoomLevel, 500, verbose)

      implicit val sc: SparkContext =
        SparkContext.getOrCreate(
          new SparkConf()
            .setAppName(s"GeotrellisSeeder:${layer.getOrElse(productType)}:$date")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer.max", "1024m"))

      seeder.renderPng(rootPath, productType, date, colorMap, bands)
      
      sc.stop()
    }
  }
}
