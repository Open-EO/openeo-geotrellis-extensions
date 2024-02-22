package org.openeo.geotrellis.layers

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, MultibandTile, Raster, RasterExtent, Tile, TileLayout}
import geotrellis.raster.gdal.{DefaultDomain, GDALRasterSource}
import geotrellis.spark.{ContextRDD, withTilerMethods}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.opensearch.OpenSearchClient

import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.collection.immutable

object NetCDFCollection {

  def loadCollection(osClient:OpenSearchClient,sc: SparkContext) = {
    val stacItems = osClient.getProducts("", None, null, Map[String, Any](), "", "")
    val items = sc.parallelize(stacItems)

    val resolutions = items.flatMap(_.resolution).distinct().collect()
    if(resolutions.length != 1) {
      throw new IllegalArgumentException("All items in a collection must have the same resolution")
    }

    val crs = items.flatMap(_.crs).distinct().collect()
    if(crs.length != 1) {
      throw new IllegalArgumentException("All items in a collection must have the same CRS")
    }

    val bboxWGS84: Extent = items.map(_.bbox).reduce((a, b)=>(a.combine(b)))



    val features: RDD[(TemporalProjectedExtent, MultibandTile)] = items.flatMap(f=>{
      val allTiles = f.links.flatMap(l=>{
        l.bandNames.get.flatMap(b=> {
          val rs = GDALRasterSource(s"${l.href.toString.replace("file:", "NETCDF:")}:${b}")
          val units = rs.metadata.baseMetadata(DefaultDomain)("t#units")
          val time_values = rs.metadata.baseMetadata(DefaultDomain)("NETCDF_DIM_t_VALUES")
          val extraDim = rs.metadata.baseMetadata(DefaultDomain)("NETCDF_DIM_EXTRA")
          val conventions: String = rs.metadata.baseMetadata(DefaultDomain).getOrElse("NC_GLOBAL#Conventions", "")

          if (!conventions.startsWith("CF-1")) {
            throw new IllegalArgumentException("Only netCDF files with CF-1.x conventions are supported by this openEO backend.")
          }
          if (extraDim != "{t}") {
            throw new IllegalArgumentException("Only netCDF files with a time dimension named 't' are supported by this openEO backend.")
          }
          if( units != "days since 1990-01-01") {
            throw new IllegalArgumentException("Only netCDF files with a time dimension in 'days since 1990-01-01' are supported by this openEO backend.")
          }
          val timestamps = time_values.substring(1,time_values.length-1).split(",").map(t=>{LocalDate.of(1990,1,1).atStartOfDay(ZoneId.of("UTC")).plusDays(t.toInt)})
          val raster: Raster[MultibandTile] = rs.read().get
          val temporalRasters: immutable.Seq[(ZonedDateTime, (String, ProjectedExtent, Tile))] = raster.tile.bands.zip(timestamps).map(rasterBand_time=>{
            (rasterBand_time._2,(b,ProjectedExtent(raster.extent,rs.crs),rasterBand_time._1))
          })
          temporalRasters

        }
          )
        })
        val byTime: Map[ZonedDateTime, Array[(String, ProjectedExtent, Tile)]] = allTiles.groupBy(_._1).mapValues(_.map(_._2))
        byTime.map(t=>{
          val sortedBands = t._2.sortBy(_._1)
          (TemporalProjectedExtent(t._2.head._2,t._1),MultibandTile(sortedBands.map(_._3)))
        })
      })

    val first = features.first()

    val cellType = first._2.cellType
    val extent = bboxWGS84.reproject(LatLng,crs(0))
    val layout = LayoutDefinition(RasterExtent(extent, CellSize(resolutions(0), resolutions(0))), 128)

    val retiled = features.tileToLayout(cellType,layout)
    ContextRDD(retiled,TileLayerMetadata[SpaceTimeKey](cellType,layout,extent,crs(0),null))


  }
}
