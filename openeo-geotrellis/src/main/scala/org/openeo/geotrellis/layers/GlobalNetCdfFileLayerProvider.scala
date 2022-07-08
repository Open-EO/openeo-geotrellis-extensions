package org.openeo.geotrellis.layers

import be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchResponses.Link
import be.vito.eodata.gwcgeotrellis.opensearch.{OpenSearchClient, OpenSearchResponses}
import geotrellis.raster.RasterSource
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.net.URI
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
import scala.util.matching.Regex

class GlobalNetCdfFileLayerProvider(override protected val dataGlob: String, bandName: String,
                                    override protected val dateRegex: Regex)
  extends AbstractGlobFileLayerProvider {

  override protected def queryAll(): Array[(ZonedDateTime, RasterSource)] = {
    val datedPaths = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath)
      .groupBy { case (date, _) => date }
      .mapValues(_.map { case (_, path) => path }.max) // take RTxs into account

    datedPaths
      .toArray
      .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }
      .map { case (date, path) => date -> GDALRasterSource(s"""NETCDF:"$path":$bandName""",GDALWarpOptions(alignTargetPixels = false)) }
  }
}




// TODO: This Client should be moved to the gwc-geotrellis project when it's complete.
class GlobalNetCDFSearchClient(val dataGlob: String, val variables: util.List[String], val dateRegex: Regex) extends OpenSearchClient {

  protected def deriveDate(filename: String, date: Regex): ZonedDateTime = filename match {
    case date(year, month, day) => LocalDate.of(year.toInt, month.toInt, day.toInt).atStartOfDay(ZoneId.of("UTC"))
  }

  // TODO: This load method was encapsulated by a cache. Ensure we use the FileLayerProvider cache for this.
  protected def load(dataGlob: String): List[Path] =
    HdfsUtils.listFiles(new Path(s"file:$dataGlob"), new Configuration)
  protected def paths: List[Path] = load(dataGlob)

  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
    val datedPaths: Map[ZonedDateTime, String] = paths
      .map(path => deriveDate(path.toUri.getPath, dateRegex) -> path.toUri.getPath)
      .groupBy { case (date, _) => date }
      .mapValues(_.map { case (_, path) => path }.max) // take RTxs into account

    var sortedDates = datedPaths
      .toArray
      .sortWith { case ((d1, _), (d2, _)) => d1 isBefore d2 }

    if (dateRange.isDefined) {
      val from = dateRange.get._1
      val to = dateRange.get._2
      sortedDates = sortedDates
        .dropWhile { case (date, _) => date isBefore from }
        .takeWhile { case (date, _) => !(date isAfter to) }
    }

    // TODO: There is currently no filtering based on bbox.

    // TODO: We're currently using RasterSources to get the extent which is duplicating work from the FileLayerProvider.
    // Find a way to get the extent without loading in RasterSources.
    val datedRasterSources: Array[(ZonedDateTime, String, GDALRasterSource)] = sortedDates
      .flatMap { case (date, path) => variables.asScala.map(v=>(date, path, GDALRasterSource(s"""NETCDF:"$path":$v""",GDALWarpOptions(alignTargetPixels = false)))) }

    // TODO: Extract Id from somewhere, currently it is "SomeId".
    // TODO: Extract resolution from somewhere, currently it is a random number.
    // TODO: Extract TileId from somewhere, currently it is the empty string.
    val features: Array[OpenSearchResponses.Feature] = datedRasterSources.map{ case (date: ZonedDateTime, path: String, source: GDALRasterSource) =>
      OpenSearchResponses.Feature(s"${path}", source.extent, date, variables.asScala.map(v=>Link(URI.create(s"""NETCDF:$path:$v"""), Some(v))).toArray, Some(5), Some(""))
    }

    features.toSeq
  }

  // This is used by FileLayerProvider.fetchExtentFromOpenSearch()
  override def getCollections(correlationId: String): Seq[OpenSearchResponses.Feature] = ???

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???
}