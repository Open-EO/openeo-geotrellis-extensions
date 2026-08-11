package org.openeo.geotrellis.layers

import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, LayoutDefinition, LayoutScheme, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, CellType, ConvertTargetCellType, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods, _}
import geotrellis.spark.tiling.Tiler
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.vector._
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.ProjectedPolygons
import org.openeo.geotrellis.GeneralUtils.cellTypeUnionWithNoData
import org.openeo.geotrelliscommon.{ByTileSpacetimePartitioner, DataCubeParameters, DatacubeSupport}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.{Logger, LoggerFactory}
import ucar.nc2.dataset.NetcdfDatasets

import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import java.util
import scala.jdk.CollectionConverters._
import scala.collection.immutable
import org.openeo.geotrellis.layers.raster_source.NetCDFRasterSource

object NetCDFCollection {

  private implicit val logger: Logger = LoggerFactory.getLogger("NetCDFCollection")

  def datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String,
                   metadata_properties: util.Map[String, Any], correlationId: String, dataCubeParameters: DataCubeParameters, osClient: OpenSearchClient): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    val sc = SparkContext.getOrCreate()
    val cube: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = loadCollection(osClient, sc)
    Seq((0, cube))
  }

  //noinspection ScalaUnusedSymbol
  def empty_datacube_seq(polygons: ProjectedPolygons, from_date: String, to_date: String,
                         dataCubeParameters: DataCubeParameters, cellType: CellType, cellSize: CellSize) : Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    Seq(
      emptyCollection(ProjectedExtent(polygons.polygons.toSeq.extent, polygons.crs), from_date, to_date, zoom = 0,
        FloatingLayoutScheme(dataCubeParameters.tileSize), cellType, cellSize
      )
    )
  }

  def loadCollection(osClient: OpenSearchClient, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val stacItems = osClient.getProducts("", None, null, Map[String, Any](), "", "")
    val items = sc.parallelize(stacItems)

    sc.setJobDescription(s"load_stac from netCDFs - ${stacItems.head.id} - ${stacItems.head.links.head.href}")
    val resolutions = items.flatMap(_.resolution).distinct().collect()
    if (resolutions.length > 1) {
      logger.warn(s"Multiple resolutions found in netCDF collection: ${resolutions.mkString("(", ", ", ")")}. We expect all assets to have the same resolution.")
    }

    val crs = items.flatMap(_.crs).distinct().collect()
    if (crs.length > 1) {
      throw new IllegalArgumentException("All items in a collection must have the same CRS")
    }

    val bboxWGS84: Extent = items.map(_.bbox).reduce((a, b) => (a.combine(b)))


    val features: RDD[(TemporalProjectedExtent, MultibandTile)] = items.repartition(stacItems.length).flatMap(f => {
      val allTiles = f.links.flatMap(l => {
        val targetCellType = if (l.datatype.isDefined){
          Some(ConvertTargetCellType(l.datatype.get.withNoData(l.nodata)))
        }else None
        l.bandNames.get.flatMap(b => {
          val source = {
            val href = l.href
            val hrefString = href.toString
            if (hrefString.startsWith("NETCDF:")) {
              s"$hrefString:$b"
            } else if ("file" == href.getScheme) {
              val localPath = java.nio.file.Paths.get(href).toString
              s"""NETCDF:"$localPath":$b"""
            } else {
              s"""NETCDF:"$hrefString":$b"""
            }
          }
          try {
            val rs = NetCDFRasterSource.fromSource(source)
            val (bandCount, timeValues) = {
              val ds = NetcdfDatasets.openDataset(rs.path, true, null)
              try {
                val variable = Option(ds.findVariable(rs.variableName)).getOrElse(
                  throw new IllegalArgumentException(s"Variable '${rs.variableName}' not found in ${rs.path}.")
                )
                val conventions: String = Option(ds.findGlobalAttributeIgnoreCase("Conventions"))
                  .map(_.getStringValue)
                  .getOrElse("")
                val dimensions = variable.getDimensions.asScala.map(_.getShortName).toList
                val extraDim = dimensions.filterNot(d => d == "x" || d == "y")
                val tVar = Option(ds.findVariable("t"))
                  .getOrElse(throw new IllegalArgumentException(s"Time dimension variable 't' not found in ${rs.path}."))
                val units = Option(tVar.findAttributeIgnoreCase("units")).map(_.getStringValue).orNull

                if (!conventions.startsWith("CF-1")) {
                  throw new IllegalArgumentException(s"Only netCDF files with CF-1.x conventions are supported by this openEO backend, but found ${conventions}.")
                }
                if (extraDim != List("t")) {
                  throw new IllegalArgumentException("Only netCDF files with a time dimension named 't' are supported by this openEO backend.")
                }
                if (units != "days since 1990-01-01") {
                  throw new IllegalArgumentException("Only netCDF files with a time dimension in 'days since 1990-01-01' are supported by this openEO backend.")
                }

                val bandCount: Int = if (variable.getRank == 3) variable.getDimension(0).getLength else 1
                val timeArray = tVar.read()
                val timeValues = (0 until bandCount).map(i => timeArray.getDouble(i).toInt)
                (bandCount, timeValues)
              } finally {
                ds.close()
              }
            }

            val timestamps = timeValues.map(t => {
              LocalDate.of(1990, 1, 1).atStartOfDay(ZoneId.of("UTC")).plusDays(t)
            })
            val raster: Raster[MultibandTile] = rs.read().get
            val temporalRasters: immutable.Seq[(ZonedDateTime, (String, ProjectedExtent, Tile))] = raster.tile.bands.zip(timestamps).map(rasterBand_time => {
              (rasterBand_time._2, (b, ProjectedExtent(raster.extent, rs.crs), rasterBand_time._1))
            })
            temporalRasters
          } catch {
            case e: Exception => {
              throw new IllegalArgumentException(s"load_stac/load_collection: Error while reading ${source} with band $b. Error message: ${e.getMessage}", e)
            }
          }

        })
      })
      val byTime: Map[ZonedDateTime, Array[(String, ProjectedExtent, Tile)]] = allTiles.groupBy(_._1).mapValues(_.map(_._2)).toMap
      byTime.map(t => {
        val sortedBands = t._2.sortBy(_._1)
        (TemporalProjectedExtent(t._2.head._2, t._1), MultibandTile(sortedBands.map(_._3)))
      })
    })

    val cellTypes = features.map(_._2.cellType)
    val cellType = cellTypes.reduce((CurrentCellType, nextCellType) => cellTypeUnionWithNoData(CurrentCellType,nextCellType))

    val extent = bboxWGS84.reproject(LatLng, crs(0))
    val layout = LayoutDefinition(RasterExtent(extent, CellSize(resolutions(0), resolutions(0))), 128)

    val spatialBounds = KeyBounds(layout.mapTransform(extent))
    val temporalBounds = KeyBounds(SpaceTimeKey(spatialBounds.minKey, TemporalKey(LocalDate.of(1990, 1, 1).atStartOfDay(ZoneId.of("UTC")))), SpaceTimeKey(spatialBounds.maxKey, TemporalKey(LocalDate.now().atStartOfDay(ZoneId.of("UTC")))))

    val keys: Array[SpatialKey] = items.map(i => i.geometry.getOrElse(i.bbox.toPolygon())).map(_.reproject(LatLng, crs(0))).clipToGrid(layout).map(_._1).distinct().collect()
    val partitioner: Partitioner = new SpacePartitioner(temporalBounds)(implicitly, implicitly, new ByTileSpacetimePartitioner(Some(keys)))

    val metadata = TileLayerMetadata[SpaceTimeKey](cellType, layout, extent, crs(0), temporalBounds)
    val retiled: RDD[(SpaceTimeKey, MultibandTile)] = features.tileToLayout(metadata, Tiler.Options(partitioner = partitioner))
    logger.info(s"Created cube for netCDF samples with metadata ${metadata} and partitioner ${partitioner.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index}")
    val cRDD = ContextRDD(retiled, metadata)
    cRDD.name = s"load_stac netCDFCollection ${items.first().id} "
    cRDD
  }

  private def emptyCollection(projectedExtent: ProjectedExtent, from_date: String, to_date: String, zoom: Int,
                              layoutScheme: LayoutScheme, cellType: CellType, cellSize: CellSize): (Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    val sc = SparkContext.getOrCreate()

    val metadata = DatacubeSupport.layerMetadata(
      projectedExtent,
      ZonedDateTime.parse(from_date, ISO_OFFSET_DATE_TIME),
      ZonedDateTime.parse(to_date, ISO_OFFSET_DATE_TIME),
      zoom,
      cellType,
      layoutScheme,
      cellSize,
      globalBounds = None,
    )

    zoom -> MultibandTileLayerRDD(sc.emptyRDD, metadata)
  }

}
