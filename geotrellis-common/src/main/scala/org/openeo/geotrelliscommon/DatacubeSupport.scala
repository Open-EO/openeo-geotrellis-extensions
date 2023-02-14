package org.openeo.geotrelliscommon

import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, LayoutDefinition, LayoutLevel, LayoutScheme, SpaceTimeKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType}
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.{MultibandTileLayerRDD, _}
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
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
          if (globalBounds.isDefined) {
            var reprojected: Extent = globalBounds.get.reproject(boundingBox.crs)
            if (multiple_polygons_flag) {
              reprojected = globalBounds.get.extent.buffer(0.1).reprojectAsPolygon(globalBounds.get.crs, boundingBox.crs, 0.01).getEnvelopeInternal
            }
            if (!reprojected.covers(boundingBox.extent)) {
              logger.error(f"Trying to construct a datacube with a bounds ${boundingBox.extent} that is not entirely inside the global bounds: ${reprojected}. ")
              reprojected = reprojected.expandToInclude(boundingBox.extent)
            }
            if (p.getName == "utm") {
              //this forces utm projection to always round to 10m, which is fine for sentinel-2, but perhaps not generally desired?
              val x = maxSpatialResolution.width
              val y = maxSpatialResolution.height
              Extent(x * Math.floor(reprojected.xmin / x), y * Math.floor(reprojected.ymin / y), x * Math.ceil(reprojected.xmax / x), y * Math.ceil(reprojected.ymax / y))
            }else{
              if (reprojected.width < maxSpatialResolution.width || reprojected.height < maxSpatialResolution.height) {
                Extent(reprojected.xmin, reprojected.ymin, Math.max(reprojected.xmax, reprojected.xmin + maxSpatialResolution.width), Math.max(reprojected.ymax, reprojected.ymin + maxSpatialResolution.height))
              } else {
                reprojected
              }
            }

          }else{
            if (p.getName == "utm") {
              //for utm, we return an extent that goes beyond the utm zone bounds, to avoid negative spatial keys
              if (p.getSouthernHemisphere)
              //official extent: Extent(166021.4431, 1116915.0440, 833978.5569, 10000000.0000) -> round to 10m + extend
                Extent(0.0, 1000000.0, 833970.0 + 100000.0, 10000000.0000 + 100000.0)
              else {
                //official extent: Extent(166021.4431, 0.0000, 833978.5569, 9329005.1825) -> round to 10m + extend
                Extent(0.0, -1000000.0000, 833970.0 + 100000.0, 9329000.0 + 100000.0)
              }
            } else {
              val extent = boundingBox.extent
              if (extent.width < maxSpatialResolution.width || extent.height < maxSpatialResolution.height) {
                Extent(extent.xmin, extent.ymin, Math.max(extent.xmax, extent.xmin + maxSpatialResolution.width), Math.max(extent.ymax, extent.ymin + maxSpatialResolution.height))
              } else {
                extent
              }
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

  def optimizeChunkSize(metadata: TileLayerMetadata[SpaceTimeKey], polygons: Array[MultiPolygon], datacubeParams: Option[DataCubeParameters], spatialKeyCount: Long) = {

    val criterium = {
      if(polygons.length>1) {
        spatialKeyCount <= 1.1 * polygons.length
      }else{
        metadata.extent.width < metadata.layout.extent.width / 2.0 || metadata.extent.height < metadata.layout.extent.height / 2.0
      }
    }
    if (datacubeParams.isDefined && datacubeParams.get.layoutScheme != "ZoomedLayoutScheme" && criterium && metadata.tileRows == 256) {
      //it seems that polygons fit entirely within chunks, so chunks are too large
      logger.info(s"${metadata} resulted in ${spatialKeyCount} for ${polygons.length} polygons, trying to reduce tile size to 128.")
      val newLayout = LayoutDefinition(metadata, 128)

      //spatialKeyCount = requiredSpatialKeys.map(_._1).countApproxDistinct()
      val gridBounds = newLayout.mapTransform.extentToBounds(metadata.extent)
      Some(metadata.copy(layout = newLayout, bounds = KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, metadata.bounds.get.minKey.time), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, metadata.bounds.get.maxKey.time))))
    } else {
      None
    }
  }


  def createPartitioner(datacubeParams: Option[DataCubeParameters], requiredSpacetimeKeys: RDD[SpaceTimeKey],  metadata: TileLayerMetadata[SpaceTimeKey]): Some[SpacePartitioner[SpaceTimeKey]] = {
    // The sparse partitioner will split the final RDD into a single partition for every SpaceTimeKey.
    val reduction: Int = datacubeParams.map(_.partitionerIndexReduction).getOrElse(SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION)
    val partitionerIndex: PartitionerIndex[SpaceTimeKey] = {
      val cached = requiredSpacetimeKeys.cache()
      val spatialCount = cached.map(_.spatialKey).countApproxDistinct()
      val spatialBounds = metadata.bounds.get.toSpatial
      val maxKeys = (spatialBounds.maxKey.col - spatialBounds.minKey.col + 1) * (spatialBounds.maxKey.row - spatialBounds.minKey.row + 1)
      val isSparse = spatialCount < 0.5 * maxKeys
      logger.info(s"Datacube is sparse: $isSparse, requiring $spatialCount keys out of $maxKeys. ")


      if(isSparse) {
        val keys = cached.distinct().collect()

        if (datacubeParams.isDefined && datacubeParams.get.partitionerTemporalResolution != "ByDay") {
          val indices = keys.map(SparseSpaceOnlyPartitioner.toIndex(_, indexReduction = reduction)).distinct.sorted
          new SparseSpaceOnlyPartitioner(indices, reduction, theKeys = Some(keys))
        } else {
          val indices = keys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = reduction)).distinct.sorted
          new SparseSpaceTimePartitioner(indices, reduction, theKeys = Some(keys))
        }
      }else{
        if (datacubeParams.isDefined && datacubeParams.get.partitionerTemporalResolution != "ByDay") {
          val indices = cached.map(SparseSpaceOnlyPartitioner.toIndex(_, indexReduction = reduction)).distinct.collect().sorted
          new SparseSpaceOnlyPartitioner(indices, reduction)
        }else if (reduction != SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION) {
          val indices = cached.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = reduction)).distinct.collect().sorted
          new SparseSpaceTimePartitioner(indices, reduction)
        }
        else{
          SpaceTimeByMonthPartitioner
        }
      }


    }
    Some(SpacePartitioner(metadata.bounds)(SpaceTimeKey.Boundable,
      ClassTag(classOf[SpaceTimeKey]), partitionerIndex))
  }

  def applyDataMask[T](datacubeParams:Option[DataCubeParameters], rdd:RDD[(SpaceTimeKey,T)],metadata: TileLayerMetadata[SpaceTimeKey])(implicit vt: ClassTag[T]): RDD[(SpaceTimeKey,T)] = {
    if (datacubeParams.exists(_.maskingCube.isDefined)) {
      val maskObject = datacubeParams.get.maskingCube.get
      maskObject match {
        case spacetimeMask: MultibandTileLayerRDD[SpaceTimeKey] =>
          if (spacetimeMask.metadata.bounds.get._1.isInstanceOf[SpaceTimeKey]) {
            if (logger.isDebugEnabled) {
              logger.debug(s"Spacetime mask is used to reduce input.")
            }
            val filtered = spacetimeMask.withContext{_.filter(_._2.band(0).toArray().exists(pixel => pixel == 0))}
            val alignedMask: MultibandTileLayerRDD[SpaceTimeKey] =
            if(spacetimeMask.metadata.crs.equals(metadata.crs) && spacetimeMask.metadata.layout.equals(metadata.layout)) {
              filtered
            }else{
              logger.debug(s"mask: automatically resampling mask to match datacube: ${spacetimeMask.metadata}")
              filtered.reproject(metadata.crs,metadata.layout,16,rdd.partitioner)._2
            }
            return rdd.join(alignedMask).mapValues(_._1)
          }
        case _ => return rdd
      }
    }
    return rdd
  }
}
