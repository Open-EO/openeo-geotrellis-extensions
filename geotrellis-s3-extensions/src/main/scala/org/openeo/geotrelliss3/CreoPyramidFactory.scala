package org.openeo.geotrelliss3

import java.io.FileInputStream
import java.lang.System.getenv
import java.net.URI
import java.nio.file.{Files, Paths}
import java.time._
import java.util

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.{CellSize, MultibandTile, RasterRegion, RasterSource, TargetAlignment, Tile, isNoData}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import javax.net.ssl.HttpsURLConnection
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.file.AbstractPyramidFactory
import org.openeo.geotrellis.layers.FileLayerProvider.{bestCRS, getLayout, layerMetadata}
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.math.max
import scala.xml.XML

object CreoPyramidFactory {

  private val maxSpatialResolution = CellSize(10, 10)
  private val awsDirect = "TRUE".equals(getenv("AWS_DIRECT"))
  private val logger = LoggerFactory.getLogger(classOf[CreoPyramidFactory])

  private implicit val dateOrdering: Ordering[ZonedDateTime] = new Ordering[ZonedDateTime] {
    override def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.withZoneSameInstant(ZoneId.of("UTC")) compareTo b.withZoneSameInstant(ZoneId.of("UTC"))
  }
}

class CreoPyramidFactory(productPaths: Seq[String], bands: Seq[String]) extends Serializable {

  import CreoPyramidFactory._
  import org.openeo.geotrellis.layers.BandCompositeRasterSource

  def this(productPaths: util.List[String], bands: util.List[String]) =
    this(productPaths.asScala, bands.asScala)

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- 14 to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, ZoomedLayoutScheme(WebMercator, 256))
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))

    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    pyramid(projectedExtent, from, to).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  /**
   * Same as #datacube, but return same structure as pyramid_seq
   *
   * @param polygons
   * @param from_date
   * @param to_date
   * @param metadata_properties
   * @return
   */
  def datacube_seq(polygons:ProjectedPolygons, from_date: String, to_date: String,
  metadata_properties: util.Map[String, Any], correlationId: String):
  Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val cube = datacube(polygons.polygons, polygons.crs, from_date, to_date)
    Seq((0, cube))
  }

  def datacube(polygons:ProjectedPolygons, from_date: String, to_date: String): MultibandTileLayerRDD[SpaceTimeKey] =
  datacube(polygons.polygons, polygons.crs, from_date, to_date)

  def datacube(polygons: Array[MultiPolygon], polygons_crs: CRS, from_date: String, to_date: String): MultibandTileLayerRDD[SpaceTimeKey] = {
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val intersectsPolygons = AbstractPyramidFactory.preparePolygons(polygons, polygons_crs)

    layer(intersectsPolygons, polygons_crs, from, to, 0, FloatingLayoutScheme(256))
  }

  private def layer(polygons: Array[MultiPolygon], polygons_crs: CRS, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, layoutScheme: LayoutScheme): MultibandTileLayerRDD[SpaceTimeKey] = {
    val sc: SparkContext = SparkContext.getOrCreate()

    val bbox = polygons.toSeq.extent

    val boundingBox = ProjectedExtent(bbox, polygons_crs)

    val xAlignedBoundingBox = xAlign(boundingBox)

    val crs = bestCRS(xAlignedBoundingBox, layoutScheme)

    val layout = getLayout(layoutScheme, xAlignedBoundingBox, zoom, maxSpatialResolution)

    val productKeys = productPaths.flatMap(listProducts) //TODO: map instead of flatmap

    if (productKeys.isEmpty) throw new IllegalArgumentException("no files found for given product paths")

    logger.debug(s"Products keys:\n${productKeys.mkString("\n")}")

    def extractDate(key: String): ZonedDateTime = {
      val date = raw"\/(\d{4})\/(\d{2})\/(\d{2})\/".r.unanchored
      key match {
        case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      }
    }

    val bandFileMaps: Seq[Map[ZonedDateTime, Seq[String]]] = bands.map(b =>
    productKeys.filter(_.contains(b)) //TODO map on product keys and filter out not needed bands
      .map(pk => extractDate(pk) -> pk)
      .groupBy(_._1)
      .map { case (k, v) => (k, v.map(_._2)) }
    )

    val dates = bandFileMaps.flatMap(_.keys).distinct

    val overlappingKeys = dates.flatMap(date => {
      val set = polygons.map(_.reproject(polygons_crs, crs))
        .flatMap(layout.mapTransform.keysForGeometry)
        .toSet
      set.map(key => SpaceTimeKey(key, date))
    })

    logger.debug(s"Overlapping keys:\n${overlappingKeys.map(_.toString).mkString("\n")}")

    val rasterSources: RDD[(SpaceTimeKey, Seq[RasterSource])] = sc.parallelize(overlappingKeys)
      .map(key => (key, bandFileMaps
      .flatMap(_.get(key.time))
      .map(paths => new BandCompositeRasterSource(NonEmptyList.fromListUnsafe(paths.map(path => GDALRasterSource(path)).toList), crs))))

    //unsafe, don't we need union of cell type?
    val commonCellType = rasterSources.take(1).head._2.head.cellType
    val metadata = layerMetadata(xAlignedBoundingBox, from, to, zoom, commonCellType, layoutScheme, maxSpatialResolution)

    val regions: RDD[(SpaceTimeKey, Seq[RasterRegion])] = rasterSources.map {
      case (key,value) => (key, value.map(rasterSource =>
        rasterSource.reproject(metadata.crs, TargetAlignment(metadata)).tileToLayout(metadata.layout))
          .flatMap(_.rasterRegionForKey(key.spatialKey)))
    }

    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)
    val tiles:RDD[(SpaceTimeKey, Seq[MultibandTile])] = regions.repartitionAndSortWithinPartitions(partitioner).map{ case (key, value) =>
      (key, value.flatMap(_.raster).map(_.tile))
    }

    val cube = tiles.flatMapValues(v => if (v.isEmpty) None else Some(v))

    ContextRDD(cube, metadata)
  }

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, layoutScheme: LayoutScheme): MultibandTileLayerRDD[SpaceTimeKey] = {
    layer(Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs, from , to, zoom, layoutScheme)
  }

  private def xAlign(boundingBox: ProjectedExtent) = {
    def floor(x: Double, precision: Double) = {
      (BigDecimal.valueOf(math.floor(x / precision)) * precision).doubleValue()
    }

    def ceil(x: Double, precision: Double) = {
      (BigDecimal.valueOf(math.ceil(x / precision)) * precision).doubleValue()
    }

    ProjectedExtent(Extent(
      floor(boundingBox.extent.xmin, maxSpatialResolution.width),
      floor(boundingBox.extent.ymin, maxSpatialResolution.height),
      ceil(boundingBox.extent.xmax, maxSpatialResolution.width),
      ceil(boundingBox.extent.ymax, maxSpatialResolution.height)
    ), boundingBox.crs)
  }

  private def getFilePathsFromManifest(path: String) = {
    var gdalPrefix = ""

    val inputStream = if (path.startsWith("https://")) {
      gdalPrefix = "/vsicurl"

      val uri = new URI(path)
      uri.resolve(s"${uri.getPath}/manifest.safe").toURL
        .openConnection.asInstanceOf[HttpsURLConnection]
        .getInputStream
    } else {
      gdalPrefix = "/vsis3"

      new FileInputStream(Paths.get(path, "manifest.safe").toFile)
    }

    val xml = XML.load(inputStream)

    (xml \\ "dataObject" \\ "fileLocation" \\ "@href")
      .map(fileLocation => s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" +
        s"/${Paths.get(fileLocation.toString).normalize().toString}")
  }

  private def listProducts(productPath: String) = {
    val keyPattern =  """.*IMG_DATA.*jp2""".r

    val filePaths =
    if (awsDirect || productPath.startsWith("https://")) {
        getFilePathsFromManifest(productPath)
      } else {
        Files.walk(Paths.get(productPath)).iterator().asScala
          .map(p => p.toString)
      }

    filePaths.flatMap {
      case key@keyPattern(_*) => Some(key)
      case _ => None
    }
  }

  def mapToSingleMultibandTile(tiles: Iterable[MultibandTile]): Option[MultibandTile] = {
    val intCombine = (t1: Int, t2: Int) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)
    val doubleCombine = (t1: Double, t2: Double) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)

    tiles.map(_.toArrayTile()).reduceOption[MultibandTile]((t1,t2) => {
      if (t1.bandCount > t2.bandCount) t1
      else if (t2.bandCount > t1.bandCount) t2
      else t1.mapBands((index,tile) => tile.dualCombine(t2.band(index))(intCombine)(doubleCombine))
    })
  }
}
