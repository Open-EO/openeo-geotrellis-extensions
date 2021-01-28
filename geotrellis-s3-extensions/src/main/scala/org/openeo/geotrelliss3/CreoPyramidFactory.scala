package org.openeo.geotrelliss3

import cats.data.NonEmptyList
import geotrellis.layer._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.gdal.GDALRasterSource
import geotrellis.raster.{CellSize, MultibandTile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.file.AbstractPyramidFactory
import org.openeo.geotrellis.layers.FileLayerProvider
import org.openeo.geotrellis.layers.FileLayerProvider.{bestCRS, layerMetadata}
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.lang.System.getenv
import java.net.URI
import java.nio.file.Paths
import java.time._
import java.util
import javax.net.ssl.HttpsURLConnection
import scala.collection.JavaConverters._
import scala.xml.XML

object CreoPyramidFactory {

  private val maxSpatialResolution = CellSize(10, 10)
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
    val boundingBox = xAlign(ProjectedExtent(bbox, polygons_crs))
    val crs = bestCRS(boundingBox, layoutScheme)

    val overlappingRasterSources = loadRasterSources(crs)

    //unsafe, don't we need union of cell type?
    val commonCellType = overlappingRasterSources.head.cellType
    val metadata = layerMetadata(boundingBox, from, to, zoom, commonCellType, layoutScheme, maxSpatialResolution)

    val rasterSources = FileLayerProvider.rasterSourceRDD(overlappingRasterSources, metadata, maxSpatialResolution, "Creo")(sc)

    FileLayerProvider.readMultibandTileLayer(rasterSources, metadata, polygons, polygons_crs, sc)
  }

  private def loadRasterSources(crs: CRS) = {
    val productKeys = productPaths.map(listProducts)

    if (productKeys.flatten.isEmpty) throw new IllegalArgumentException("no files found for given product paths")

    logger.debug(s"Products keys:\n${productKeys.mkString("\n")}")

    def extractDate(key: String): ZonedDateTime = {
      val date = raw"\/(\d{4})\/(\d{2})\/(\d{2})\/".r.unanchored
      key match {
        case date(year, month, day) => ZonedDateTime.of(LocalDate.of(year.toInt, month.toInt, day.toInt), LocalTime.MIDNIGHT, ZoneOffset.UTC)
      }
    }

    productKeys.map(_.filter(p => bands.exists(b => p.contains(b))))
      .map(paths => new BandCompositeRasterSource(NonEmptyList.fromListUnsafe(paths.map(path => GDALRasterSource(path)).toList), crs, Predef.Map("date" -> extractDate(paths.toIterator.next()).toString)))

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
      gdalPrefix = if (getAwsDirect()) "/vsis3" else ""

      new FileInputStream(Paths.get(path, "manifest.safe").toFile)
    }

    val xml = XML.load(inputStream)

    (xml \\ "dataObject" \\ "fileLocation" \\ "@href")
      .map(fileLocation => s"$gdalPrefix${if (path.startsWith("/")) "" else "/"}$path" +
        s"/${Paths.get(fileLocation.toString).normalize().toString}")
  }

  private def listProducts(productPath: String) = {
    val keyPattern =  """.*IMG_DATA.*jp2""".r

    val filePaths = getFilePathsFromManifest(productPath)

    filePaths.flatMap {
      case key@keyPattern(_*) => Some(key)
      case _ => None
    }
  }

  private def getAwsDirect() = {
    "TRUE".equals(getenv("AWS_DIRECT"))
  }
}
