package org.openeo.geotrellis.udf

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.{ArrayTile, MultibandTile, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import jep.{DirectNDArray, SharedInterpreter}

import java.nio.ByteBuffer
import java.util

object Udf {

  private val defaultImports =
    """
      |import collections
      |import numpy as np
      |import xarray as xr
      |import openeo.metadata
      |from openeo.udf import UdfData
      |from openeo.udf.xarraydatacube import XarrayDataCube
      |""".stripMargin

  case class SpatialExtent(xmin : Double, val ymin : Double, val xmax : Double, ymax: Double, tileCols: Int, tileRows: Int)

  private def _createExtentFromSpatialKey(layoutDefinition: LayoutDefinition,
                                          key: SpatialKey
                                         ): SpatialExtent = {
    val ex = layoutDefinition.extent
    val tileLayout = layoutDefinition.tileLayout
    val xrange = ex.xmax - ex.xmin
    val xinc = xrange / tileLayout.layoutCols
    val yrange = ex.ymax - ex.ymin
    val yinc = yrange / tileLayout.layoutRows
    SpatialExtent(
      ex.xmin + xinc * key.col,
      ex.ymax - yinc * (key.row + 1),
      ex.xmin + xinc * (key.col + 1),
      ex.ymax - yinc * key.row,
      tileCols=tileLayout.tileCols,
      tileRows=tileLayout.tileRows
    )
  }

  /**
   * Converts a DirectNDArray to an XarrayDataCube by adding coordinates and dimension labels.
   * Note: Variable 'extent' has been created in Python by _createExtentFromSpatialKey method.
   *
   * @param interp
   * @param directTile
   * @param tileShape
   *  Shape of the tile as an array [a,b,c,d].
   *    With,
   *      a: time     (#dates) (if exists)
   *      b: bands    (#bands) (if exists)
   *      c: y-axis   (#cells)
   *      d: x-axis   (#cells)
   * @param spatialExtent The extent of the tile, with number of cols/rows in the tile.
   * @param bandCoordinates A list of band names to act as coordinates for the band dimension (if exists).
   * @param timeCoordinates A list of dates to act as coordinates for the time dimension (if exists).
   */
  private def _set_xarraydatacube(interp: SharedInterpreter,
                                       directTile: DirectNDArray[ByteBuffer],
                                       tileShape: Array[Int],
                                       spatialExtent: SpatialExtent,
                                       bandCoordinates: util.ArrayList[String],
                                       timeCoordinates: Array[String] = Array()
                                      ): Unit = {

    // Note: This method is a scala implementation of geopysparkdatacube._tile_to_datacube.
    interp.set("tile_shape", tileShape)
    interp.set("extent", spatialExtent)
    interp.set("start_times", timeCoordinates)
    interp.set("band_names", bandCoordinates)

    // Initialize coordinates and dimensions for the final xarray datacube.
    // TODO: Add message to OpenEOApiException:
    // \"\"\"In run_udf, the data has {b} bands, while the 'bands' dimension has {len_dim} labels. These labels were set on the dimension: {labels}. Please investigate if dimensions and labels are correct.\"\"\".format(b=band_count, len_dim = len(band_names), labels=str(band_names))
    interp.exec(
      """
        |coords = {}
        |dims = ('bands','y', 'x')
        |
        |# time coordinates if exists
        |if len(tile_shape) == 4:
        |    #we have a temporal dimension
        |    coords = {'t':start_times}
        |    dims = ('t' ,'bands','y', 'x')
        |
        |# band names if exists
        |if band_names:
        |    coords['bands'] = band_names
        |    band_count = tile_shape[dims.index('bands')]
        |    if band_count != len(band_names):
        |        raise OpenEOApiException(status_code=400,message='')
        |
        |if extent is not None:
        |    gridx=(extent.xmax() - extent.xmin())/extent.tileRows()
        |    gridy=(extent.ymax() - extent.ymin())/extent.tileCols()
        |    xdelta=gridx*0.5*(tile_shape[-1]-extent.tileRows())
        |    ydelta=gridy*0.5*(tile_shape[-2]-extent.tileCols())
        |    xmin=extent.xmin()-xdelta
        |    xmax=extent.xmax()+xdelta
        |    ymin=extent.ymin()-ydelta
        |    ymax=extent.ymax()+ydelta
        |    coords['x']=np.linspace(xmin+0.5*gridx,xmax-0.5*gridx,tile_shape[-1],dtype=np.float32)
        |    coords['y']=np.linspace(ymax-0.5*gridy,ymin+0.5*gridy,tile_shape[-2],dtype=np.float32)
        |""".stripMargin)

    // Create a Datacube using the same area in memory as the Scala tile.
    interp.set("npCube", directTile)
    interp.exec(
      """
        |the_array = xr.DataArray(npCube, coords=coords, dims=dims, name="openEODataChunk")
        |datacube = XarrayDataCube(the_array)
        |""".stripMargin)
  }

  def runUserCode(code: String, layer: MultibandTileLayerRDD[SpatialKey],
                  bandNames: util.ArrayList[String], context: util.HashMap[String, Any]): MultibandTileLayerRDD[SpatialKey] = {
    // TODO: Also implement for SpaceTimeKey.
    // Map a python function to every tile of the RDD.
    // Map will serialize + send partitions to worker nodes
    // Worker nodes will receive partitions in JVM
    //  * We can use JEP to start a python interpreter in JVM
    //  * Then transfer the partition from the JVM to the interpreter using JEP

    // TODO: AllocateDirect is an expensive operation, we should create one buffer for the entire partition
    // and then slice it!
    val result = layer.mapPartitions(iter => {
      // TODO: Start an interpreter for every partition
      // TODO: Currently this fails because per tile processing cannot access the interpreter
      // TODO: This is because every tile in a partition is handled in a separate thread.
      iter.map(tuple => {
        val interp: SharedInterpreter = new SharedInterpreter
        val multiBandTile: MultibandTile = tuple._2
        var resultMultiBandTile = multiBandTile
        try {
          // Convert tile to DirectNDArray
          var bytes: Array[Byte] = Array()
          multiBandTile.bands.foreach((tile: Tile) => { bytes ++= tile.toBytes() })
          val buffer = ByteBuffer.allocateDirect(bytes.length) // Allocating a direct buffer is expensive.
          buffer.put(bytes)
          val tileShape = Array(multiBandTile.bandCount, multiBandTile.bands(0).rows, multiBandTile.bands(0).cols)
          val directTile = new DirectNDArray(buffer, tileShape: _*)

          // Setup the xarray datacube
          interp.exec(defaultImports)
          val spatialExtent = _createExtentFromSpatialKey(layer.metadata.layout, tuple._1)
          _set_xarraydatacube(interp, directTile, tileShape, spatialExtent, bandNames, Array())

          interp.set("context", context)
          interp.exec("data = UdfData(proj={\"EPSG\": 900913}, datacube_list=[datacube], user_context=context)")
          interp.exec(code)
          interp.exec("result_cube = apply_datacube(data.get_datacube_list()[0], data.user_context)")

          // Copy the result back to java heap memory.
          // In the future we can hopefully keep it in native memory from deserialization to serialization.
          val resultData: Array[Byte] = Array.fill(bytes.length)(0)
          buffer.rewind()
          for (i <- bytes.indices) {
            resultData(i) = buffer.get
          }

          // Convert the result back to a MultibandTile.
          resultMultiBandTile = multiBandTile.mapBands((bandNumber, tile) => {
            val tileSize = tile.dimensions.cols * tile.dimensions.rows
            val tileOffset = bandNumber * tileSize
            val tileData = resultData.slice(tileOffset, tileOffset + tileSize)
            ArrayTile(tileData, tile.dimensions.cols, tile.dimensions.rows)
          })
        } finally if (interp != null) interp.close()

        (tuple._1, resultMultiBandTile)
      })
    }, preservesPartitioning = true)

    ContextRDD(result, layer.metadata)
  }


}
