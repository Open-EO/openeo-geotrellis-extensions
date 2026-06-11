package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType, Tile}
import geotrellis.vector.Extent
import org.openeo.geotrellis.udf.SharedInterpreterFactory
import org.slf4j.LoggerFactory

object SyntheticDataRasterSource {
  private val logger = LoggerFactory.getLogger(SyntheticDataRasterSource.getClass)
}

case class SyntheticDataRasterSource(itemId:String, cellTypeName: String, gridExtent: GridExtent[Long], override val crs: CRS, udf: Option[String] = None) extends RasterSource {
  import SyntheticDataRasterSource._

  val targetCellType: Option[TargetCellType] = None

  private val DEFAULT_IMPORTS =
    """
      |import collections
      |import datetime
      |import numpy as np
      |import xarray as xr
      |import openeo.metadata
      |from openeo.udf import UdfData
      |from openeo.udf.xarraydatacube import XarrayDataCube
      |from openeo_driver.errors import OpenEOApiException
      |""".stripMargin

  private val FILL_ARRAY =
    """
      |for row in range(rows):
      |  for col in range(cols):
      |    tile_array[row*col] = generate(row, col)
      |""".stripMargin

  override def metadata: RasterMetadata = this

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    SyntheticDataRasterSource(itemId, cellTypeName, gridExtent.reproject(crs, targetCRS), targetCRS, udf)

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    SyntheticDataRasterSource(itemId, cellTypeName, resampleTarget(gridExtent), crs, udf)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    logger.info(s"Loading synthetic data for ${itemId}")

    extent.intersection(gridExtent.extent)
      .map { intersection =>
        val intersectionGridBounds = gridExtent.gridBoundsFor(intersection).toGridType[Int]
        val dataTile = syntheticDataTile(intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(dataTile), intersection)
      }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    logger.info(s"Loading synthetic data for ${itemId}")

    bounds.intersection(gridExtent.dimensions)
      .map { intersection =>
        val intersectionGridBounds = intersection.toGridType[Int]
        val dataTile = syntheticDataTile(intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(dataTile), gridExtent.extentFor(intersection))
      }
  }

  private def syntheticDataTile(rows: Int, cols: Int): Tile = {
    cellTypeName match {
      case "byte" | "int8" | "uint8" | "int8raw" | "uint8raw" => ArrayTile(syntheticData(cols, rows, new Array[Byte](rows*cols), 1.toByte), rows, cols)
      case "short" | "int16" | "int16raw" | "uint16raw" => ArrayTile(syntheticData(cols, rows, new Array[Short](rows*cols), 1.toShort), rows, cols)
      case "int" | "int32" => ArrayTile(syntheticData(cols, rows, new Array[Int](rows*cols), 1), rows, cols)
      case "float" | "float32" | "float32raw" => ArrayTile(syntheticData(cols, rows, new Array[Float](rows*cols), 1f), rows, cols)
      case "double" | "float64" => ArrayTile(syntheticData(cols, rows, new Array[Double](rows*cols), 1d), rows, cols)
      case _ => throw new IllegalArgumentException("Unsupported CellType for synthetic data")
    }
  }

  private def syntheticData[T <: AnyVal: scala.reflect.ClassTag](cols: Int, rows: Int, arr: Array[T], defaultValue: T): Array[T] = {
    val f = {
      if (udf.isDefined) {
        val ip = SharedInterpreterFactory.create()
        try {
          ip.exec(DEFAULT_IMPORTS)
          ip.set("rows", rows)
          ip.set("cols", cols)
          ip.set("tile_array", arr)
          ip.exec(udf.get)
          ip.exec(FILL_ARRAY)
          ip.getValue("tile_array").asInstanceOf[arr.type]
        } finally {
          if (ip != null) {
            ip.close()
          }
        }
      } else {
        logger.warn(s"No UDF defined for synthetic data override, using default value ($defaultValue) instead")
        arr.map(_ => defaultValue)
      }
    }
    f
  }


  override def convert(targetCellType: TargetCellType): RasterSource =
    new SyntheticDataRasterSource(itemId, cellTypeName, gridExtent, crs, udf)

  override def name: SourceName = itemId

  override def bandCount: Int = 1

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map()

  override def attributesForBand(band: Int): Map[String, String] = Map()

  override def toString: String = f"${getClass.getName}($cellType, $gridExtent, $crs)"

  override def cellType: CellType = CellType.fromName(cellTypeName)
}
