package org.openeo.geotrelliss3

import java.io.FileInputStream
import java.lang.System.getenv
import java.net.{URI, URL}
import java.nio.file.{Files, Paths}
import java.time._
import java.util

import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.gdal.config.GDALOptionsConfig.registerOption
import geotrellis.raster.{CellSize, MultibandTile, RasterRegion, TargetAlignment, Tile, isNoData}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import javax.net.ssl.HttpsURLConnection
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.layers.FileLayerProvider.{bestCRS, getLayout, layerMetadata}
import org.openeo.geotrelliscommon.SpaceTimeByMonthPartitioner
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.math.max
import scala.xml.XML

object CreoPyramidFactory {

  private val layoutScheme = FloatingLayoutScheme(256)
  private val maxSpatialResolution = CellSize(10, 10)
  private val maxZoom = 14
  private val endpoint = "http://data.cloudferro.com"
  private val region = "RegionOne"
  private val awsDirect = "TRUE".equals(getenv("AWS_DIRECT"))
  private val logger = LoggerFactory.getLogger(classOf[CreoPyramidFactory])

  private implicit val dateOrdering: Ordering[ZonedDateTime] = new Ordering[ZonedDateTime] {
    override def compare(a: ZonedDateTime, b: ZonedDateTime): Int =
      a.withZoneSameInstant(ZoneId.of("UTC")) compareTo b.withZoneSameInstant(ZoneId.of("UTC"))
  }
}

class CreoPyramidFactory(productPaths: Seq[String], bands: Seq[String]) extends Serializable {

  import CreoPyramidFactory._

  def this(productPaths: util.List[String], bands: util.List[String]) =
    this(productPaths.asScala, bands.asScala)

  if (awsDirect) registerGdalOptions()

  private def registerGdalOptions() {
    registerOption("AWS_S3_ENDPOINT", URI.create(endpoint).getAuthority)
    registerOption("AWS_DEFAULT_REGION", region)
    registerOption("AWS_SECRET_ACCESS_KEY", getenv("AWS_SECRET_ACCESS_KEY"))
    registerOption("AWS_ACCESS_KEY_ID", getenv("AWS_ACCESS_KEY_ID"))
    registerOption("AWS_VIRTUAL_HOSTING", "FALSE")
    registerOption("AWS_HTTPS", "NO")
  }

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

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

  private def mapToSingleTile(tiles: Iterable[Tile]): Option[Tile] = {
    val intCombine = (t1: Int, t2: Int) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)
    val doubleCombine = (t1: Double, t2: Double) => if (isNoData(t1)) t2 else if (isNoData(t2)) t1 else max(t1, t2)

    tiles.map(_.toArrayTile()).reduceOption[Tile](_.dualCombine(_)(intCombine)(doubleCombine))
  }

  private def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val sc: SparkContext = SparkContext.getOrCreate()

    val xAlignedBoundingBox = xAlign(boundingBox)

    val crs = bestCRS(xAlignedBoundingBox, layoutScheme)

    val reprojectedBoundingBox = xAlignedBoundingBox.reproject(crs)

    val layout = getLayout(layoutScheme, xAlignedBoundingBox, zoom, maxSpatialResolution)

    val productKeys = productPaths.flatMap(listProducts)

    if (productKeys.isEmpty) throw new IllegalArgumentException("no files found for given product paths")

    logger.debug(s"Products keys:\n${productKeys.mkString("\n")}")

    def extractDate(key: String): ZonedDateTime = {
      val date = raw"\/(\d{4})\/(\d{2})\/(\d{2})\/".r.unanchored
      key match {
        case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      }
    }

    val bandFileMaps: Seq[Map[ZonedDateTime, Seq[String]]] = bands.map(b =>
      productKeys.filter(_.contains(b))
        .map(pk => extractDate(pk) -> pk)
        .groupBy(_._1)
        .map { case (k, v) => (k, v.map(_._2)) }
    )

    val dates = bandFileMaps.flatMap(_.keys).distinct

    val overlappingKeys = dates.flatMap(date =>
      layout.mapTransform.keysForGeometry(reprojectedBoundingBox.toPolygon())
        .map(key => SpaceTimeKey(key, date)))

    logger.debug(s"Overlapping keys:\n${overlappingKeys.map(_.toString).mkString("\n")}")

    val rasterSources: RDD[(SpaceTimeKey,Seq[Seq[GDALRasterSource]])] = sc.parallelize(overlappingKeys)
      .map(key => (key, bandFileMaps
        .flatMap(_.get(key.time))
        .map(_.map(path => GDALRasterSource(path)))))

    //unsafe, don't we need union of cell type?
    val commonCellType = rasterSources.take(1).head._2.head.head.cellType
    val metadata = layerMetadata(xAlignedBoundingBox, from, to, zoom min maxZoom, commonCellType,layoutScheme, maxSpatialResolution)

    val regions: RDD[(SpaceTimeKey, Seq[Seq[RasterRegion]])] = rasterSources.map {
      case (key,value) =>
        if (awsDirect) registerGdalOptions()

        (key, value.map(_.map(rasterSource =>
          rasterSource.reproject(metadata.crs, TargetAlignment(metadata)).tileToLayout(metadata.layout))
          .flatMap(_.rasterRegionForKey(key.spatialKey))))
    }

    val partitioner = SpacePartitioner(metadata.bounds)
    assert(partitioner.index == SpaceTimeByMonthPartitioner)
    val tiles:RDD[(SpaceTimeKey,Seq[Tile])] = regions.repartitionAndSortWithinPartitions(partitioner).map{ case (key,value) =>
      (key,value.map(_.flatMap(_.raster).map(_.tile.band(0))).flatMap(mapToSingleTile(_)))
    }

    val cube = tiles.flatMapValues(v => if (v.isEmpty) None else Some(MultibandTile(v)))

    ContextRDD(cube, metadata)
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

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom)
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    pyramid_seq_internal(projectedExtent, from_date, to_date)
  }

  private def pyramid_seq_internal(projectedExtent: ProjectedExtent, from_date: String, to_date: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
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
    pyramid_seq_internal(ProjectedExtent(polygons.polygons.toSeq.extent,polygons.crs),from_date, to_date)
  }


}
