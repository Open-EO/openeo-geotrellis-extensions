package org.openeo.geotrelliscommon

import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, LayoutDefinition, LayoutLevel, LayoutScheme, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType}
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime
import scala.reflect.ClassTag

object DatacubeSupport {

  private val logger = LoggerFactory.getLogger(classOf[OpenEORasterCubeMetadata])

  /**
   * Find best CRS, can be location dependent (UTM)
   *
   * @param boundingBox
   * @return
   */
  def bestCRS(boundingBox: ProjectedExtent,layoutScheme:LayoutScheme):CRS = {
    layoutScheme match {
      case scheme: ZoomedLayoutScheme => scheme.crs
      case scheme: FloatingLayoutScheme => boundingBox.crs //TODO determine native CRS based on collection metadata, not bbox?
    }
  }

  def targetBoundingBox(boundingBox: ProjectedExtent, layoutScheme: LayoutScheme) = {
    val crs = DatacubeSupport.bestCRS(boundingBox, layoutScheme)
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)
    reprojectedBoundingBox
  }

  // note: make sure to express boundingBox and maxSpatialResolution in the same units
  def getLayout(layoutScheme: LayoutScheme, boundingBox: ProjectedExtent, zoom: Int, maxSpatialResolution: CellSize, globalBounds:Option[ProjectedExtent] = Option.empty, multiple_polygons_flag: Boolean = false) = {
    val LayoutLevel(_, worldLayout) = layoutScheme match {
      case scheme: ZoomedLayoutScheme => scheme.levelForZoom(zoom)
      case scheme: FloatingLayoutScheme => {
        //Giving the layout a deterministic extent simplifies merging of data with spatial partitioner
        val layoutExtent: Extent = {
          val p = boundingBox.crs.proj4jCrs.getProjection
          if (p.getName == "utm") {
            if(globalBounds.isDefined) {
              var reprojected = globalBounds.get.reproject(boundingBox.crs)
              if (multiple_polygons_flag) {
                reprojected = globalBounds.get.extent.buffer(0.1).reprojectAsPolygon(globalBounds.get.crs, boundingBox.crs, 0.01).getEnvelopeInternal
              }
              if (!reprojected.covers(boundingBox.extent)) {
                logger.error(f"Trying to construct a datacube with a bounds ${boundingBox.extent} that is not entirely inside the global bounds: ${reprojected}. ")
              }
              val x = maxSpatialResolution.width
              val y = maxSpatialResolution.height
              Extent(x*Math.floor(reprojected.xmin/x),y*Math.floor(reprojected.ymin/y),x*Math.ceil(reprojected.xmax/x),y*Math.ceil(reprojected.ymax/y))
            }else{
              //for utm, we return an extent that goes beyond the utm zone bounds, to avoid negative spatial keys
              if (p.getSouthernHemisphere)
              //official extent: Extent(166021.4431, 1116915.0440, 833978.5569, 10000000.0000) -> round to 10m + extend
                Extent(0.0, 1000000.0, 833970.0 + 100000.0, 10000000.0000 + 100000.0)
              else {
                //official extent: Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825) -> round to 10m + extend
                Extent(0.0, -1000000.0000, 833970.0 + 100000.0, 9329000.0 + 100000.0)
              }
            }
          } else {
            val extent = boundingBox.extent
            if(extent.width < maxSpatialResolution.width || extent.height < maxSpatialResolution.height) {
              Extent(extent.xmin,extent.ymin,Math.max(extent.xmax,extent.xmin + maxSpatialResolution.width),Math.max(extent.ymax,extent.ymin + maxSpatialResolution.height))
            }else{
              extent
            }
          }
        }

        scheme.levelFor(layoutExtent, maxSpatialResolution)
      }
    }
    worldLayout
  }

  def layerMetadata(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, cellType: CellType,
                    layoutScheme:LayoutScheme, maxSpatialResoluton: CellSize, globalBounds:Option[ProjectedExtent] = Option.empty, multiple_polygons_flag: Boolean = false) = {

    val worldLayout: LayoutDefinition = DatacubeSupport.getLayout(layoutScheme, boundingBox, zoom, maxSpatialResoluton, globalBounds = globalBounds, multiple_polygons_flag = multiple_polygons_flag)

    val reprojectedBoundingBox: ProjectedExtent = DatacubeSupport.targetBoundingBox(boundingBox, layoutScheme)

    val metadata: TileLayerMetadata[SpaceTimeKey] = tileLayerMetadata(worldLayout, reprojectedBoundingBox, from, to, cellType)
    metadata
  }

  def tileLayerMetadata(layout: LayoutDefinition, projectedExtent: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, cellType: CellType): TileLayerMetadata[SpaceTimeKey] = {
    val gridBounds = layout.mapTransform.extentToBounds(projectedExtent.extent)

    TileLayerMetadata(
      cellType,
      layout,
      projectedExtent.extent,
      projectedExtent.crs,
      KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
    )
  }

  def applyDataMask[T](datacubeParams:Option[DataCubeParameters], rdd:RDD[(SpaceTimeKey,T)])(implicit vt: ClassTag[T]): RDD[(SpaceTimeKey,T)] = {
    if (datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject = datacubeParams.get.maskingCube.get
      maskObject match {
        case spacetimeMask: MultibandTileLayerRDD[SpaceTimeKey] =>
          if (spacetimeMask.metadata.bounds.get._1.isInstanceOf[SpaceTimeKey]) {
            if (logger.isDebugEnabled) {
              logger.debug(s"Spacetime mask is used to reduce input.")
            }
            val theFilteredMask = spacetimeMask.filter(_._2.band(0).toArray().exists(pixel => pixel == 0))
            return rdd.join(theFilteredMask).map((tuple => (tuple._1, tuple._2._1)))
          }
        case _ => return rdd
      }
    }
    return rdd
  }
}
