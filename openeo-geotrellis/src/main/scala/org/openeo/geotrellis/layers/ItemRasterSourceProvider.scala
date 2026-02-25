package org.openeo.geotrellis.layers

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, RasterExtent, RasterSource}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.{healthCheckExtentWarn, isCrsCoveredInHealthCheck, isExtentValidInCrs, safeReproject}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.{Logger, LoggerFactory}

trait ItemRasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[ItemRasterSourceProvider])

  def canProcess(item: Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean

  def getRasterSource(item: Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters] = Option.empty): Option[RasterSource]

  def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
    Extent(
      extent.xmin,
      extent.ymin,
      math.max(extent.xmax, extent.xmin + cellSize.width),
      math.max(extent.ymax, extent.ymin + cellSize.height),
    )

  def computeItemExtentInTargetLayout(item: Feature, re: RasterExtent, targetExtent: ProjectedExtent, datacubeParams: Option[DataCubeParameters]) = {
    logger.debug(s"computeItemExtentInTargetLayout() -> item: $item, rasterExtent: $re, targetExtent: $targetExtent, datacubeParams: $datacubeParams")
    if (item.rasterExtent.isDefined && item.crs.isDefined) {
      val useNewFeatureExtentIntersectionPossible = isCrsCoveredInHealthCheck(item.crs.get) && isCrsCoveredInHealthCheck(targetExtent.crs)
      val alignedToTargetExtent = if (!datacubeParams.exists(_.useNewFeatureExtentIntersection) || !useNewFeatureExtentIntersectionPossible) {
        // logger.info("Using old intersection method between Feature/Item and target extent.")
        // TODO: Remove this after it has been deployed for a while
        /**
         * Several edge cases to cover:
         *  - if feature extent is whole world, it may be invalid in target crs
         *  - if feature is in utm, target extent may be invalid in feature crs
         *    this is why we take intersection
         */
        val targetExtentInLatLon = targetExtent.reproject(item.crs.get)
        val featureExtentInLatLon = item.rasterExtent.get.reproject(item.crs.get, LatLng)

        val intersection = featureExtentInLatLon.intersection(targetExtentInLatLon).map(_.buffer(1.0)).getOrElse(featureExtentInLatLon)
        val tmp = expandToCellSize(intersection.reproject(LatLng, targetExtent.crs), re.cellSize)
        re.createAlignedRasterExtent(tmp)
      } else {
        val featureProjectedExtent = ProjectedExtent(item.rasterExtent.get, item.crs.get)
        healthCheckExtentWarn(featureProjectedExtent, s"Feature/Item extent should be valid: ")
        healthCheckExtentWarn(targetExtent, s"Target extent should be valid: ")

        /**
         * Several edge cases to cover:
         *  - if feature extent is whole world, it may be invalid in target crs (tested in readDataCubeWithOpensearchClientUTM)
         *  - if feature is in utm, target extent may be invalid in feature crs
         *    this is why we take intersection.
         *    We convert both extents to a common CRS before taking the intersection.
         *    We give priority to use the target CRS as common CRS, because the intersection will be converted to it anyway
         *    In case the feature extent is invalid in the target CRS, we use the feature CRS as common CRS
         */
        val commonCrs = if (isExtentValidInCrs(featureProjectedExtent, targetExtent.crs)) targetExtent.crs
        else if (isExtentValidInCrs(targetExtent, item.crs.get)) item.crs.get
        else {
          logger.warn(s"Feature/Item and target extent are not valid within each others range. Using LatLng as fallback.")
          LatLng
        }

        val featureExtentInCommonCRS = safeReproject(featureProjectedExtent, commonCrs)
        val targetExtentInCommonCRS = safeReproject(targetExtent, commonCrs)
        healthCheckExtentWarn(featureExtentInCommonCRS, s"Item extent (${item.id}) should be valid in common CRS: ")

        val intersection = featureExtentInCommonCRS.extent.intersection(targetExtentInCommonCRS.extent)
        val intersectionTargetCrs = intersection match {
          case None =>
            // Item, Asset and Feature mean the same thing in this context.
            logger.warn(s"Item extent $featureExtentInCommonCRS and target extent $targetExtentInCommonCRS do not intersect. (${item.id})")
            // return None // Discard the feature
            // TODO: feature.rasterExtent is not accurate when going over the antimeridian.
            // TODO: Fall back to feature.geometry? Now the fallback is to load the whole tile (Just like old intersection code)
            targetExtent.extent
          case Some(value) => value.reproject(commonCrs, targetExtent.crs)
        }
        var tmp = expandToCellSize(intersectionTargetCrs, re.cellSize)
        val dcp = datacubeParams.getOrElse(new DataCubeParameters())
        val p = math.max(1, dcp.maskingStrategyParameters
          .getOrDefault("erosion_kernel_size", 0.asInstanceOf[Object]).asInstanceOf[Integer]) * 1.0
        val pixelBuffer = (math.max(p, dcp.pixelBufferX), math.max(p, dcp.pixelBufferY))
        tmp = Extent(
          tmp.xmin - re.cols * pixelBuffer._1, tmp.ymin - re.rows * pixelBuffer._2,
          tmp.xmax + re.cols * pixelBuffer._1, tmp.ymax + re.rows * pixelBuffer._2,
        )
        healthCheckExtentWarn(ProjectedExtent(tmp, targetExtent.crs), s"Item extent (${item.id}) should be valid in target CRS: ")
        re.createAlignedRasterExtent(tmp)
      }
      Some(alignedToTargetExtent.toGridType[Long])
    } else {
      Some(re.toGridType[Long])
    }
  }

}
