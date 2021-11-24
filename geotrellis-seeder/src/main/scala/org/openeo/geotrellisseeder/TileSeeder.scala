package org.openeo.geotrellisseeder

import java.io.File
import java.net.URL
import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern
import be.vito.eodata.biopar.EOProduct
import be.vito.eodata.catalog.CatalogClient
import be.vito.eodata.gwcgeotrellis.colormap
import be.vito.eodata.gwcgeotrellis.geotrellis.TileFetcher
import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchClient
import be.vito.eodata.gwcgeotrellis.s3.S3ClientConfigurator
import com.beust.jcommander.JCommander
import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata, _}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.ResampleMethods._
import geotrellis.raster.geotiff.GeoTiffReprojectRasterSource
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.render.ColorMap
import geotrellis.raster.resample.Average
import geotrellis.raster.{RasterRegion, RasterSource, _}
import geotrellis.spark._
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT

import javax.ws.rs.client.ClientBuilder
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.openeo.geotrellisseeder.TileSeeder.CLOUD_MILKINESS
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.{NoSuchKeyException, PutObjectRequest}

import scala.collection.JavaConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter}
import scala.collection.mutable.ListBuffer
import scala.math._
import scala.xml.SAXParseException

case class TileSeeder(zoomLevel: Int, verbose: Boolean, partitions: Option[Int] = None, selectOverlappingTile: Boolean = false) {

  private val logger = if (verbose) VerboseLogger(classOf[TileSeeder]) else StandardLogger(classOf[TileSeeder])

  def renderSinglePng(productType: String, date: LocalDate, key: SpatialKey, path: String, colorMap: Option[String] = None,
                      bands: Option[Array[Band]] = None)(implicit sc: SparkContext): Unit = {

    renderPng(path, productType, date.toString, colorMap, bands, spatialKey = Some(key))
  }


  def renderPng(path: String, productType: String, dateStr: String, colorMap: Option[String] = None, bands: Option[Array[Band]] = None,
                productGlob: Option[String] = None, maskValues: Array[Int] = Array(), permissions: Option[String] = None,
                spatialKey: Option[SpatialKey] = None, tooCloudyFile: Option[String] = None, datePattern: Option[String] = None,
                oscarsEndpoint: Option[String] = None, oscarsCollection: Option[String] = None, oscarsSearchFilters: Option[Map[String, String]] = None, resampleMethod: Option[ResampleMethod] = Some(NearestNeighbor))
               (implicit sc: SparkContext): Unit = {

    val date = dateStr match {
      case d if d.nonEmpty => Some(LocalDate.parse(dateStr.substring(0, 10)))
      case _ => None
    }

    var sourcePathsWithBandId: Seq[(Seq[String], Int)] = Seq()

    if (oscarsEndpoint.isDefined && oscarsCollection.isDefined) {
      configureDebugLogging()

      val openSearchClient = OpenSearchClient(new URL(oscarsEndpoint.get))
      val products = if (date.isDefined) {
        val attributeValues = Map(Seq(("productType", productType), ("accessedFrom", "S3")) ++ oscarsSearchFilters.getOrElse(Map()).toSeq: _*)
        openSearchClient.getProducts(oscarsCollection.get, date.map(d => (d, d)).get, ProjectedExtent(LatLng.worldExtent, LatLng), attributeValues, "", "")
      } else {
        //Divide world extent to get less then 10000 products per request
        val extents = Seq(Extent(-180, -89.99999, -90, 89.99999), Extent(-90, -89.99999, 0, 89.99999), Extent(0, -89.99999, 90, 89.99999), Extent(90, -89.99999, 179.99999, 89.99999))

        extents.flatMap(e => openSearchClient.getProducts(oscarsCollection.get, None, ProjectedExtent(e, LatLng)))
          .groupBy(_.id)
          .map(_._2.head)
          .toSeq
      }

      val paths = products.flatMap(_.links.filter(_.title.contains(productType)).map(_.href.toString))

      val s3Path = """(s3://)(.*:)(.*)""".r.unanchored

      val s3Paths = paths.flatMap {
        case s3Path(prefix, _, key) => Some(s"$prefix$key")
        case _ => None
      }

      if (s3Paths.nonEmpty) {
        sourcePathsWithBandId = Seq((s3Paths, 0))

        S3ClientConfigurator.configure()
      } else {
        sourcePathsWithBandId = Seq((paths, 0))
      }

    } else if (productGlob.isEmpty) {
      val catalog = new CatalogClient()
      val products = catalog.getProducts(productType, date.get, date.get, "GEOTIFF").asScala

      logger.logProducts(products)

      if (colorMap.isDefined) {
        sourcePathsWithBandId = singlebandSourcePathsForDate(products).map((_, 0))
      } else if (bands.isDefined) {
        sourcePathsWithBandId = multibandSourcePathsForDate(products, bands.get.map(_.name)).zip(bands.get.map(_.id))
      } else {
        sourcePathsWithBandId = multibandSourcePathsForDate(products).map((_, 0))
      }
    } else {
      def listFiles(path: Path) = HdfsUtils.listFiles(path, sc.hadoopConfiguration).map(_.toString)

      var dateGlob = productGlob.get
      if (!date.isEmpty) {
        val pattern = datePattern.getOrElse("yyyy/MM/dd")
        dateGlob = productGlob.get.replace("#DATE#", date.get.format(DateTimeFormatter.ofPattern(pattern)))
      }

      if (colorMap.isDefined) {
        sourcePathsWithBandId = Seq((listFiles(new Path(s"file://$dateGlob")), 0))
      } else if (bands.isDefined) {
        sourcePathsWithBandId = bands.get.map(b => (listFiles(new Path(s"file://${dateGlob.replace("#BAND#", b.name)}")), b.id))
      } else {
        sourcePathsWithBandId = Array("VV, VH").map(b => (listFiles(new Path(s"file://${dateGlob.replace("#BAND#", b)}")), 0))
      }
    }

    if (sourcePathsWithBandId.map(_._1.length).sum > 0) {
      val globalLayout = GlobalLayout(256, zoomLevel, 0.1)

      implicit val layout: LayoutDefinition =
        globalLayout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10, 10))._1

      def getPartitions = partitions.getOrElse(max(1, round(pow(2, zoomLevel) / 20).toInt))

      if (colorMap.isDefined) {
        val map = colormap.ColorMapParser.parse(colorMap.get)
        getSinglebandRDD(sourcePathsWithBandId.head, spatialKey, resampleMethod.getOrElse(NearestNeighbor))
          .repartition(getPartitions)
          .foreachPartition { items =>
            configureDebugLogging()
            S3ClientConfigurator.configure()
            items.foreach(renderSinglebandRDD(path, dateStr, map, zoomLevel))
          }
      } else if (bands.isDefined) {
        getMultibandRDD(sourcePathsWithBandId, spatialKey)
          .fullOuterJoin(getTooCloudyRdd(if (!date.isEmpty) date.get else null, maskValues, tooCloudyFile))
          .repartition(getPartitions)
          .foreach(renderMultibandRDD(path, dateStr, bands.get, zoomLevel, maskValues))
      } else {
        getS1MultibandRDD(sourcePathsWithBandId, spatialKey)
          .repartition(getPartitions)
          .foreach(renderMultibandRDD(path, dateStr, zoomLevel))
      }

      permissions.foreach(setFilePermissions(path, dateStr, _))
    }
  }

  private def getTooCloudyRdd(date: LocalDate, maskValues: Array[Int], tooCloudyFile: Option[String] = None)(implicit sc: SparkContext, layout: LayoutDefinition) = {
    var result = sc.emptyRDD[(SpatialKey, Tile)]

    if (maskValues.nonEmpty) {
      def getJson = {
        tooCloudyFile
          .map(file => new ObjectMapper().readTree(new File(file)))
          .orElse {
            val response = ClientBuilder.newClient()
              .target("http://es1.vgt.vito.be:9200/product_catalog_prod/_search")
              .queryParam("q", s"properties.identifier:S2*MSIL*_${date.format(ofPattern("yyyyMMdd"))}*%20AND%20properties.processing_status.status:TOO_CLOUDY")
              .queryParam("size", "10000")
              .request()
              .get()

            if (response.getStatus == 200) {
              Some(new ObjectMapper().readTree(response.readEntity(classOf[String])))
            } else None
          }
      }

      val geometries = getJson.map(json => json.get("hits").get("hits").elements().asScala
        .map(hit => WKT.read(hit.get("_source").get("properties").get("footprint").textValue()))
        .map(_.reproject(LatLng, WebMercator)))

      if (geometries.isDefined) {
        result = sc.parallelize(geometries.get.toSeq)
          .flatMap(g => layout.mapTransform.keysForGeometry(g).map(k => (k, g)))
          .groupByKey()
          .map { case (key, geoms) =>
            val extent = layout.mapTransform(key)
            val rasterExtent = RasterExtent(extent, layout.tileCols, layout.tileRows)
            (key, geoms.map(Rasterizer.rasterizeWithValue(_, rasterExtent, maskValues(0))))
          }
          .mapValues(mapToSingleTile(maskValues))
      }
    }

    result
  }

  private def getMultibandRDD(sourcePathsWithBandId: Seq[(Seq[String], Int)], spatialKey: Option[SpatialKey])
                             (implicit sc: SparkContext, layout: LayoutDefinition) = {

    val sourcesWithBandId = sourcePathsWithBandId.map(s => (reproject(s._1), s._2))

    loadMultibandTiles(sourcesWithBandId, spatialKey)
  }

  private def getS1MultibandRDD(sourcePathsWithBandId: Seq[(Seq[String], Int)], spatialKey: Option[SpatialKey])
                               (implicit sc: SparkContext, layout: LayoutDefinition) = {

    val sourcesVV = (reproject(sourcePathsWithBandId(0)._1), sourcePathsWithBandId(0)._2)
    val sourcesVH = (reproject(sourcePathsWithBandId(1)._1), sourcePathsWithBandId(1)._2)

    loadMultibandTiles(sourcesVV, sourcesVH, spatialKey)
  }

  private def getSinglebandRDD(sourcePathsWithBandId: (Seq[String], Int), spatialKey: Option[SpatialKey], resampleMethod: ResampleMethod = NearestNeighbor)
                              (implicit sc: SparkContext, layout: LayoutDefinition): RDD[(SpatialKey, (Iterable[RasterRegion], Int))] with Metadata[TileLayerMetadata[SpatialKey]] = {

    val sourcesWithBandId = (reproject(sourcePathsWithBandId._1, resampleMethod), sourcePathsWithBandId._2)

    loadSinglebandTiles(sourcesWithBandId, spatialKey, resampleMethod)
  }

  private def reproject(sourcePaths: Seq[String], resampleMethod: ResampleMethod = NearestNeighbor) = {
    sourcePaths.map(GeoTiffReprojectRasterSource(_, WebMercator, resampleMethod = resampleMethod))
  }

  private def renderSinglebandRDD(path: String, dateStr: String, colorMap: ColorMap, zoom: Int)
                                 (item: (SpatialKey, (Iterable[RasterRegion], Int))) {
    item match {
      case (key, regions) =>
        logger.logKey(key)

        val tile = (regionsToTile _).tupled(regions)
        if (!tile.isNoDataTile) {
          val tilePath = pathForTile(path, dateStr, key, zoom)

          if (tilePath.startsWith("/")) deleteSymLink(tilePath)

          val png = tile.toArrayTile().renderPng(colorMap)

          if (tilePath.startsWith("s3:")) {
            val s3 = S3ClientProducer.get()
            val uri = new AmazonS3URI(tilePath)
            s3.putObject(PutObjectRequest.builder.bucket(uri.getBucket).key(uri.getKey).build, RequestBody.fromBytes(png))
          } else {
            png.write(tilePath)
          }

          logger.logTile(key, tilePath)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }

  private def renderMultibandRDD(path: String, dateStr: String, bands: Array[Band], zoom: Int, maskValues: Array[Int])
                                (item: (SpatialKey, (Option[((Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int))], Option[Tile]))): Unit = {
    item match {
      case (key, (regions, cloudTile)) =>
        logger.logKey(key)

        val tile = regions
          .flatMap { case (rRegions, gRegions, bRegions) =>
            val tileR = (regionsToTile _).tupled(rRegions)
            val tileG = (regionsToTile _).tupled(gRegions)
            val tileB = (regionsToTile _).tupled(bRegions)

            if (!tileR.isNoDataTile && !tileG.isNoDataTile && !tileB.isNoDataTile) {
              Some(MultibandTile(tileR, tileG, tileB))
            } else {
              None
            }
          }
          .map(t => cloudTile.map(ct => t.mapBands((_, b) => mapToSingleTile(maskValues)(Seq(b, ct)))).getOrElse(t))
          .orElse(cloudTile.map(ct => MultibandTile(ct, ct, ct)))
          .map(t => toNormalizedMultibandTile(t, bands, maskValues))

        if (tile.isDefined) {
          val tilePath = pathForTile(path, dateStr, key, zoom)

          deleteSymLink(tilePath)

          tile.get.renderPng().write(tilePath)

          logger.logTile(key, tilePath)
        } else {
          logger.logNoDataTile(key)
        }
    }
  }

  private def renderMultibandRDD(path: String, dateStr: String, zoom: Int)
                                (item: (SpatialKey, ((Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int)))) {
    item match {
      case (key, (vvRegions, vhRegions)) =>
        logger.logKey(key)

        val tileR = (regionsToTile _).tupled(vvRegions)
        val tileG = (regionsToTile _).tupled(vhRegions)

        if (!tileR.isNoDataTile && !tileG.isNoDataTile) {
          val tilePath = pathForTile(path, dateStr, key, zoom)

          deleteSymLink(tilePath)

          toNormalizedMultibandTile(tileR, tileG).renderPng().write(tilePath)

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

  private def setFilePermissions(path: String, dateStr: String, permissions: String) {
    val datePath = Paths.get(path, "g", dateStr)
    if (datePath.toFile.exists()) {
      val builder = new ProcessBuilder("find", s"${datePath.toString}", "-type", "d", "-exec", "chmod", permissions, "{}", ";")
      val command = String.join(" ", builder.command)
      val process = builder.start
      val exitCode = process.waitFor
      if (exitCode != 0) {
        throw new IllegalStateException(s"Command $command exited with status code $exitCode")
      }
    }
  }

  private def multibandSourcePathsForDate(products: Iterable[_ <: EOProduct], bands: Seq[String]) = {
    pathsFromProducts(products, bands: _*)
  }

  private def multibandSourcePathsForDate(products: Iterable[_ <: EOProduct]) = {
    pathsFromProducts(products, "VV", "VH")
  }

  private def singlebandSourcePathsForDate(products: Iterable[_ <: EOProduct]) = {
    pathsFromProducts(products, "")
  }

  private def pathsFromProducts(products: Iterable[_ <: EOProduct], bands: String*) = {
    val result = new ListBuffer[Seq[String]]

    bands.foreach(b => {
      val paths = new ListBuffer[String]
      products.foreach(p => {
        p.getFiles.asScala.foreach(f => {
          if (b.isEmpty || f.getBands.contains(b)) {
            paths += f.getFilename.getPath
          }
        })
      })
      result += paths
    })

    Seq(result: _*)
  }

  private def loadMultibandTiles(sourcesWithBandId: Seq[(_ <: Seq[RasterSource], Int)], spatialKey: Option[SpatialKey])
                                (implicit sc: SparkContext, layout: LayoutDefinition):
  RDD[(SpatialKey, ((Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int)))]
    with Metadata[TileLayerMetadata[SpatialKey]] = {

    val allSources = sourcesWithBandId.map(_._1).foldLeft(Seq[RasterSource]())((r1: Seq[RasterSource], r2: Seq[RasterSource]) => r1 ++ r2)
    val layerMetadata = getLayerMetadata(allSources)

    val rRegionRDD = rasterRegionRDDFromSources(sourcesWithBandId(0), spatialKey)
    val gRegionRDD = rasterRegionRDDFromSources(sourcesWithBandId(1), spatialKey)
    val bRegionRDD = rasterRegionRDDFromSources(sourcesWithBandId(2), spatialKey)

    val regionRDD = rRegionRDD.join(gRegionRDD).join(bRegionRDD).mapValues(v => (v._1._1, v._1._2, v._2))

    ContextRDD(regionRDD, layerMetadata)
  }

  private def loadMultibandTiles(sourcesVV: (Seq[RasterSource], Int), sourcesVH: (Seq[RasterSource], Int), spatialKey: Option[SpatialKey])
                                (implicit sc: SparkContext, layout: LayoutDefinition):
  RDD[(SpatialKey, ((Iterable[RasterRegion], Int), (Iterable[RasterRegion], Int)))]
    with Metadata[TileLayerMetadata[SpatialKey]] = {

    val layerMetadata = getLayerMetadata(sourcesVV._1 ++ sourcesVH._1)

    val vvRegionRDD = rasterRegionRDDFromSources(sourcesVV, spatialKey)
    val vhRegionRDD = rasterRegionRDDFromSources(sourcesVH, spatialKey)

    val regionRDD = vvRegionRDD.join(vhRegionRDD)

    ContextRDD(regionRDD, layerMetadata)
  }

  private def loadSinglebandTiles(sourcesWithBandId: (Seq[RasterSource], Int), spatialKey: Option[SpatialKey], resampleMethod: ResampleMethod = NearestNeighbor)(implicit sc: SparkContext, layout: LayoutDefinition) = {

    val layerMetadata = getLayerMetadata(sourcesWithBandId._1)

    val regionRDD = rasterRegionRDDFromSources(sourcesWithBandId, spatialKey, resampleMethod)

    ContextRDD(regionRDD, layerMetadata)
  }

  private def getLayerMetadata(sources: Seq[RasterSource])(implicit layout: LayoutDefinition) = {
    val cellTypes = sources.flatMap(s => {
      try {
        Some(s.cellType)
      } catch {
        case _: SAXParseException => None
        case e: NoSuchKeyException =>
          logger.logNoSuchKeyException(s.name.toString)
          throw e
      }
    }).toSet
    require(
      cellTypes.size <= 1,
      s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes"
    )

    val projections = sources.flatMap(s => {
      try {
        Some(s.crs)
      } catch {
        case _: SAXParseException => None
      }
    }).toSet
    require(
      projections.size <= 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val combinedExtents = sources.flatMap(s => {
      try {
        Some(s.extent)
      } catch {
        case _: SAXParseException => None
      }
    }).reduce(_ combine _)
    val layerKeyBounds = KeyBounds(layout.mapTransform(combinedExtents))

    TileLayerMetadata[SpatialKey](cellType, layout, combinedExtents, crs, layerKeyBounds)
  }

  private def rasterRegionRDDFromSources(sourcesWithBandId: (Seq[RasterSource], Int), spatialKey: Option[SpatialKey], resampleMethod: ResampleMethod = NearestNeighbor)
                                        (implicit sc: SparkContext, layout: LayoutDefinition): RDD[(SpatialKey, (Iterable[RasterRegion], Int))] = {

    sc.parallelize(sourcesWithBandId._1, partitions.getOrElse(sc.defaultParallelism))
      .mapPartitions { sources =>
        S3ClientConfigurator.configure()

        sources.flatMap { source =>
          try {
            val tileSource = source.tileToLayout(layout, resampleMethod = resampleMethod)
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
                case _: NoSuchElementException => None
              }
            }
          } catch {
            case _: SAXParseException =>
              logger.logParseException(source.toString)
              None
          }
        }
      }
      .groupByKey()
      .mapValues((_, sourcesWithBandId._2))
  }

  private def regionsToTile(regions: Iterable[RasterRegion], band: Int): Tile = {
    mapToSingleTile(Array())(regions.map(r => r.raster.get.tile.band(band)))
  }

  private def mapToSingleTile(maskValues: Array[Int])(tiles: Iterable[Tile]): Tile = {
    val intCombine = (t1: Int, t2: Int) =>
      if (isNoData(t1)) t2
      else if (isNoData(t2)) t1
      else if (maskValues.contains(t1)) t2
      else if (maskValues.contains(t2)) t1
      else max(t1, t2)
    val doubleCombine = (t1: Double, t2: Double) =>
      if (isNoData(t1)) t2
      else if (isNoData(t2)) t1
      else if (maskValues.contains(t1)) t2
      else if (maskValues.contains(t2)) t1
      else max(t1, t2)

    def dualCombine(t1: Tile, t2: Tile): Tile = {
      if (selectOverlappingTile) {
        TileFetcher.combineSelectTile(t1, t2)
      } else {
        t1.dualCombine(t2)(intCombine)(doubleCombine)
      }
    }

    tiles.map(_.toArrayTile()).reduce[Tile](dualCombine)
  }

  private def toNormalizedMultibandTile(tile: MultibandTile, bands: Array[Band], maskValues: Array[Int]): MultibandTile = {
    def markClouds(t: Tile, array: Array[Int]) {
      t.toArray().zipWithIndex.foreach {
        case (v, i) => if (maskValues.contains(v)) {
          array(i) = CLOUD_MILKINESS
        }  else if (isNoData(v)) {
          array(i) = 0
        }
      }
    }


    /**
     * Custom implementation of the GeoTrellis normalize function. This applied the normalization function and
     * clipping in a single call. If done separately, issues appeared with incorrect and overflowing cell data types.
     */
     def normalizeClip(t: Tile, oldMin: Int, oldMax: Int, newMin: Int, newMax: Int): Tile = {
      val dnew = newMax - newMin
      val dold = oldMax - oldMin
      if (dold <= 0 || dnew <= 0) {
        sys.error(s"Invalid parameters: $oldMin, $oldMax, $newMin, $newMax")
      }
      t.mapIfSet(z => {
        val newZ = (((z - oldMin) * dnew) / dold) + newMin
        1 max newZ min 255
      })
    }


    def normalize(bandIndex: Int, tile: Tile) = {
      val band = bands(bandIndex)
      normalizeClip(tile, band.min, band.max, 1, 255)
        .convert(IntConstantNoDataCellType)
    }

    val alphaArray = Array.fill(tile.size)(255)
    tile.bands.foreach(markClouds(_, alphaArray))

    val alphaTile = ArrayTile(alphaArray, tile.cols, tile.rows)

    val normalizedTile = tile.mapBands(normalize)

    MultibandTile(normalizedTile.bands :+ alphaTile)
  }

  private def toNormalizedMultibandTile(tileR: Tile, tileG: Tile): MultibandTile = {

    def logTile(tile: Tile): Tile = {
      tile.mapDouble(10 * log10(_))
    }

    def normalizeClip(t: Tile, oldMin: Double, oldMax: Double, newMin: Double, newMax: Double): Tile = {
      val dnew = newMax - newMin
      val dold = oldMax - oldMin
      if (dold <= 0 || dnew <= 0) {
        sys.error(s"Invalid parameters: $oldMin, $oldMax, $newMin, $newMax")
      }
      t.mapIfSetDouble(z => {
        val newZ = (((z - oldMin) * dnew) / dold) + newMin
        1.toDouble max newZ min 255.toDouble
      })
    }

    def normalize(t: Tile, min: Double, max: Double) = {
      normalizeClip(t, min, max, 1, 255)
    }

    val logTileR = logTile(tileR.toArrayTile())
    val logTileG = logTile(tileG.toArrayTile())
    val tileB = logTileR.combineDouble(logTileG)(_ / _)

    val normTileR = normalize(logTileR, -25, 3)
    val normTileG = normalize(logTileG, -30, -2)
    val normTileB = normalize(tileB, 0.2, 1)

    MultibandTile(normTileR, normTileG, normTileB)
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

      val invertedRow = pow(2, zoom).toInt - 1 - key.row
      val y = f"$invertedRow%09d"
      val y2 = y.substring(0, 3)
      val y1 = y.substring(3, 6)
      val y0 = y.substring(6)

      if (path.startsWith("s3")) {
        path + Paths.get("/", grid, dateStr, z, x2, x1, x0, y2, y1, y0 + ".png")
      } else {
        val dir = Paths.get(path, grid, dateStr, z, x2, x1, x0, y2, y1)
        dir.toFile.mkdirs()
        dir.resolve(y0 + ".png").toString
      }
    }
  }

  private def configureDebugLogging(): Unit = {
    if (System.getProperty("logger.debug") != null) {
      Logger.getRootLogger.setLevel(Level.DEBUG)
    }
  }
}

object TileSeeder {
  private val CLOUD_MILKINESS = 150

  private def getResampleMethod(method: String): ResampleMethod = {
    if (method == null) {
      return NearestNeighbor
    }
    method.toLowerCase match {
      case "mode" => Mode
      case "nearestneighbor" => NearestNeighbor
      case "bilinear" => Bilinear
      case "cubicspline" => CubcSpline
      case "average" => Average
      case "median" => Median
      case "max" => Max
      case "min" => Min
      case _ => NearestNeighbor
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "GeotrellisSeeder"

    val jCommanderArgs = new JCommanderArgs
    val jCommander = new JCommander(jCommanderArgs, args: _*)
    jCommander.setProgramName(appName)

    if (jCommanderArgs.help) {
      jCommander.usage()
    } else {
      val date = jCommanderArgs.date
      val datePattern = jCommanderArgs.datePattern
      val productType = jCommanderArgs.productType
      val layer = jCommanderArgs.layer
      val rootPath = jCommanderArgs.rootPath
      val zoomLevel = jCommanderArgs.zoomLevel
      val colorMap = jCommanderArgs.colorMap
      val bands = jCommanderArgs.bands
      val productGlob = jCommanderArgs.productGlob
      val maskValues = jCommanderArgs.maskValues
      val permissions = jCommanderArgs.setPermissions
      val tooCloudyFile = jCommanderArgs.tooCloudyFile
      val oscarsEndpoint = jCommanderArgs.oscarsEndpoint
      val oscarsCollection = jCommanderArgs.oscarsCollection
      val oscarsSearchFilters = jCommanderArgs.oscarsSearchFilters
      val selectOverlappingTile = jCommanderArgs.selectOverlappingTile
      val partitions = jCommanderArgs.partitions
      val verbose = jCommanderArgs.verbose
      val resampleMethod = jCommanderArgs.resampleMethod.map(getResampleMethod)

      val seeder = new TileSeeder(zoomLevel, verbose, partitions, selectOverlappingTile)

      implicit val sc: SparkContext =
        SparkContext.getOrCreate(
          new SparkConf()
            .setAppName(s"GeotrellisSeeder:${layer.getOrElse(productType)}:$date")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryoserializer.buffer.max", "1024m"))

      seeder.renderPng(rootPath, productType, date, colorMap, bands, productGlob, maskValues, permissions, tooCloudyFile = tooCloudyFile, oscarsEndpoint = oscarsEndpoint, oscarsCollection = oscarsCollection, oscarsSearchFilters = oscarsSearchFilters, datePattern = datePattern, resampleMethod = resampleMethod)

      sc.stop()
    }
  }
}
