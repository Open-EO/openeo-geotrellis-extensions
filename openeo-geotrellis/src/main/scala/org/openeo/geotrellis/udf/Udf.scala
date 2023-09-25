package org.openeo.geotrellis.udf

import geotrellis.layer.{LayoutDefinition, SpaceTimeKey, SpatialKey, TemporalProjectedExtent}
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{ArrayMultibandTile, CellSize, FloatArrayTile, FloatConstantNoDataCellType, MultibandTile, RasterExtent}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods}
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import jep.{DirectNDArray, JepConfig, NDArray, SharedInterpreter}
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import org.slf4j.LoggerFactory

import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Udf {

  private val logger = LoggerFactory.getLogger("Python-Jep-Udf")

  private val SIZE_OF_FLOAT = 4

  private val DEFAULT_IMPORTS =
    """
      |import collections
      |import datetime
      |import numpy as np
      |import xarray as xr
      |import openeo.metadata
      |from openeo.udf import UdfData
      |from openeo.udf.xarraydatacube import XarrayDataCube
      |""".stripMargin

  private var isInterpreterInitialized = false

  case class SpatialExtent(xmin : Double, val ymin : Double, val xmax : Double, ymax: Double, tileCols: Int, tileRows: Int)

  private def _createSharedInterpreter(): SharedInterpreter = {
    if (!isInterpreterInitialized) {
      val config = new JepConfig()
      config.redirectStdErr(System.err)
      config.redirectStdout(System.out)
      SharedInterpreter.setConfig(config)
      isInterpreterInitialized = true
    }
    new SharedInterpreter
  }

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
   *
   * @param interp
   * @param directTile
   * @param tileShape
   *  Shape of the tile as a list [a,b,c,d].
   *    With,
   *      a: time     (#dates) (if exists)
   *      b: bands    (#bands) (if exists)
   *      c: y-axis   (#rows)
   *      d: x-axis   (#cols)
   * @param spatialExtent The extent of the tile + the number of cols and rows.
   * @param bandCoordinates A list of band names to act as coordinates for the band dimension (if exists).
   * @param timeCoordinates A list of dates to act as coordinates for the time dimension (if exists).
   */
  private def _setXarraydatacubeInPython(interp: SharedInterpreter,
                                         directTile: DirectNDArray[FloatBuffer],
                                         tileShape: List[Int],
                                         spatialExtent: SpatialExtent,
                                         bandCoordinates: util.ArrayList[String],
                                         timeCoordinates: List[Long] = List()
                                      ): Unit = {
    // Note: This method is a scala implementation of geopysparkdatacube._tile_to_datacube.
    interp.set("tile_shape", new util.ArrayList[Int](tileShape.asJava))
    interp.set("extent", spatialExtent)
    interp.set("band_names", bandCoordinates)
    interp.set("start_times", new util.ArrayList[Long](timeCoordinates.asJava))

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
        |    # There is a temporal dimension.
        |    time_coordinates = [datetime.datetime.utcfromtimestamp(start_time / 1000) for start_time in start_times]
        |    coords = {'t':time_coordinates}
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

  private def _setContextInPython(interp: SharedInterpreter, context: util.HashMap[String, Any]): Unit = {
    interp.exec(
      """def pyjmap_to_dict(pyjmap):
        |  new_dict = {}
        |  pyjmap_str = "jep.PyJMap"
        |  for key in pyjmap:
        |    value = pyjmap[key]
        |    if pyjmap_str in str(type(value)):
        |      value = pyjmap_to_dict(value)
        |    new_dict[key] = value
        |  return new_dict""".stripMargin)

    interp.set("pyjmap_context", context)
    interp.exec("context = pyjmap_to_dict(pyjmap_context)")
  }

  private def _checkOutputDtype(dtype: String): Unit = {
    if (!dtype.equals("float32"))
      throw new IllegalArgumentException("UDF returned a datacube that does not have dtype == np.float32.")
  }

  private def _checkOutputSpatialDimensions(resultDimensions: Seq[Int], tileRows: Int, tileCols: Int): Unit = {
    if (resultDimensions(resultDimensions.length - 2) != tileRows || resultDimensions.last != tileCols) {
      throw new IllegalArgumentException((
        "UDF returned a datacube that does not have the same rows and columns as the input cube. " +
          "Actual spatial dimensions: (%d, %d). Expected spatial dimensions: (%d, %d).")
        .format(resultDimensions(2), resultDimensions(3), tileRows, tileCols)
      )
    }
  }

  private def _extractMultibandTileFromBuffer(resultBuffer: FloatBuffer, newNumberOfBands: Int,
                                    tileSize: Int, tileCols: Int, tileRows: Int): MultibandTile = {
    val newBands = new ListBuffer[FloatArrayTile]()
    for (_b <- 1 to newNumberOfBands) {
      // Tile size remains the same because #cols and #rows are not changed by UDF.
      val tileData: Array[Float] = Array.fill(tileSize)(0)
      resultBuffer.get(tileData, 0, tileSize) // Copy buffer data to tile.
      newBands += FloatArrayTile(tileData, tileCols, tileRows)
    }
    new ArrayMultibandTile(newBands.toArray)
  }

  def runChunkPolygonUserCode(code: String,
                              layer: MultibandTileLayerRDD[SpaceTimeKey],
                              projectedPolygons: ProjectedPolygons,
                              bandNames: util.ArrayList[String],
                              context: util.HashMap[String, Any],
                              maskValue: java.lang.Double = null
                             ): MultibandTileLayerRDD[SpaceTimeKey] = {
    val projectedPolygonsNativeCRS = ProjectedPolygons.reproject(projectedPolygons, layer.metadata.crs);

    // Group all tiles by geometry and mask them with that geometry.
    // Key: MultiPolygon, Value: One tile for each date (combined with the polygon's extent and SpaceTimeKey).
    val processes = new OpenEOProcesses()
    val groupedAndMaskedRdd: RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])] =
      processes.groupAndMaskByGeometry(layer, projectedPolygonsNativeCRS, maskValue)

    val resultkeyedByTemporalExtent: RDD[(TemporalProjectedExtent, MultibandTile)] = groupedAndMaskedRdd.mapPartitions(iter => {
      // TODO: Do one allocateDirect for every partition.
      iter.flatMap(tuple => {
        val tiles: Iterable[(Extent, Long, MultibandTile)] = tuple._2

        // Sort tiles by date.
        val sortedtiles = tiles.toList.sortBy(_._2)
        val dates: List[Long] = sortedtiles.map(_._2)
        val multibandTiles: Seq[MultibandTile] = sortedtiles.map(_._3)

        // Initialize spatial extent.
        val tileRows = multibandTiles.head.bands(0).rows
        val tileCols = multibandTiles.head.bands(0).cols
        val tileShape = List(dates.size, multibandTiles.head.bandCount, tileRows, tileCols)
        val tileSize = tileRows * tileCols
        val multiDateMultiBandTileSize = dates.size *  multibandTiles.head.bandCount * tileSize
        val polygonExtent: Extent = sortedtiles.head._1
        val spatialExtent = SpatialExtent(
          polygonExtent.xmin, polygonExtent.ymin, polygonExtent.xmax, polygonExtent.ymax, tileCols, tileRows
        )

        val resultTiles = ListBuffer[(TemporalProjectedExtent, MultibandTile)]()
        val interp: SharedInterpreter = _createSharedInterpreter
        try {
          interp.exec(DEFAULT_IMPORTS)

          // Convert multi-band tiles to one DirectNDArray with shape (#dates, #bands, #y-cells, #x-cells).
          val buffer = ByteBuffer.allocateDirect(multiDateMultiBandTileSize * SIZE_OF_FLOAT).order(ByteOrder.nativeOrder()).asFloatBuffer()
          multibandTiles.foreach(_.bands.foreach(tile => {
            val tileFloats: Array[Float] =  tile.asInstanceOf[FloatArrayTile].array
            buffer.put(tileFloats, 0, tileFloats.length)
          }))
          val directTile = new DirectNDArray[FloatBuffer](buffer, tileShape: _*)

          // Convert DirectNDArray to XarrayDatacube.
          _setXarraydatacubeInPython(interp, directTile, tileShape, spatialExtent, bandNames, dates)
          // Convert context from jep.PyJMap to dict.
          _setContextInPython(interp, context)

          // Execute the UDF in python.
          interp.exec("data = UdfData(proj={\"EPSG\": 900913}, datacube_list=[datacube], user_context=context)")
          interp.exec(code)
          interp.exec("result_cube = apply_datacube(data.get_datacube_list()[0], data.user_context)")

          // Convert the result back to a list of multi-band tiles.
          val resultDimensions = interp.getValue("result_cube.get_array().values.shape").asInstanceOf[java.util.List[Long]].asScala.toList.map(_.toInt)
          val resultCube = interp.getValue("result_cube.get_array().values")
          var resultBuffer: FloatBuffer = null
          resultCube match {
            case cube: DirectNDArray[FloatBuffer] =>
              // The datacube was modified inplace.
              resultBuffer = cube.getData
            case cube: NDArray[Array[Float]] =>
              // UDF created a new datacube.
              if (resultDimensions.length != 4) {
                throw new IllegalArgumentException((
                  "UDF returned a datacube that does not have dimensions (#dates, #bands, #rows, #cols). " +
                    "Actual dimensions: (%s).").format(resultDimensions.mkString(", "))
                )
              }
              val dtype = interp.getValue("str(result_cube.get_array().values.dtype)").asInstanceOf[String]
              _checkOutputDtype(dtype)
              _checkOutputSpatialDimensions(resultDimensions, tileRows, tileCols)
              resultBuffer = FloatBuffer.wrap(cube.getData)
          }

          // Note: xarray instants are in nanoseconds, divide by 10**6 for milliseconds.
          interp.exec("result_dates = result_cube.get_array().coords['t'].values.tolist()")
          val resultDates = interp
            .getValue("result_dates if isinstance(result_dates, list) else [result_dates]")
            .asInstanceOf[java.util.ArrayList[Long]].asScala.transform(l => (l / scala.math.pow(10,6)).longValue)

          // UDFs can add/remove bands or dates from the original datacube but not rows, cols.
          val newNumberOfBands = resultDimensions(1)
          resultBuffer.rewind()
          for (resultDate <- resultDates) {
            val multibandTile = _extractMultibandTileFromBuffer(resultBuffer, newNumberOfBands, tileSize, tileCols, tileRows)
            val projectedExtent: ProjectedExtent = ProjectedExtent(polygonExtent, layer.metadata.crs)
            resultTiles += ((TemporalProjectedExtent(projectedExtent, resultDate), multibandTile))
          }
        } finally { if (interp != null) interp.close() }
        resultTiles
      })
    }, preservesPartitioning = true)

    val resultGroupedBySpaceTimeKey: MultibandTileLayerRDD[SpaceTimeKey] = resultkeyedByTemporalExtent.tileToLayout(layer.metadata)
    ContextRDD(resultGroupedBySpaceTimeKey, layer.metadata)
  }

  def runUserCode(code: String, layer: MultibandTileLayerRDD[SpaceTimeKey],
                  bandNames: util.ArrayList[String], context: util.HashMap[String, Any]): MultibandTileLayerRDD[SpaceTimeKey] = {
    // TODO: Implement apply_timeseries, apply_hypercube.
    // Map a python function to every tile of the RDD.
    // Map will serialize + send partitions to worker nodes
    // Worker nodes will receive partitions in JVM
    //  * We can use JEP to start a python interpreter in JVM
    //  * Then transfer the partition from the JVM to the interpreter using JEP

    // TODO: AllocateDirect is an expensive operation, we should create one buffer for the entire partition
    // and then slice it!

    val newLayout = {
      if (code.contains("apply_metadata")) {
        val newResolution = 5.0 //TODO determine based on convert_dimensions
        val crsCode = layer.metadata.crs.epsgCode.get
        val stepSize = layer.metadata.layout.cellSize
        val cubeMetadata =
          s"""
            |import openeo.metadata
            |metadata = {
            |   "cube:dimensions": {
            |      "x": {"type": "spatial", "axis": "x", "step": ${stepSize.width}, "reference_system": $crsCode},
            |      "y": {"type": "spatial", "axis": "y", "step": ${stepSize.height}, "reference_system": $crsCode},
            |      "t": {"type": "temporal"}
            |   }
            |}
            |
            |""".stripMargin
        val resultMetadata = layer.sparkContext.parallelize(Seq(1)).map(t=>{
          val interp = _createSharedInterpreter()
          try {
            interp.exec(DEFAULT_IMPORTS)
            _setContextInPython(interp, context)
            interp.exec(code)
            interp.exec(cubeMetadata)
            interp.exec("result_metadata = apply_metadata(openeo.metadata.CollectionMetadata(metadata), context)")
            val targetResolutionX:Double = interp.getValue("[d for d in result_metadata.spatial_dimensions if d.name == \"x\"][0].step").asInstanceOf[Double]
            val targetResolutionY:Double = interp.getValue("[d for d in result_metadata.spatial_dimensions if d.name == \"y\"][0].step").asInstanceOf[Double]
            CellSize(targetResolutionX,targetResolutionY)
          } finally if (interp != null) interp.close()
        }).collect()

        Some(LayoutDefinition(RasterExtent(layer.metadata.layout.extent, resultMetadata.apply(0)), layer.metadata.layout.tileRows))
      } else {
        None
      }

    }
    val oldLayout = layer.metadata.layout

    val result = layer.mapPartitions(iter => {
      // TODO: Start an interpreter for every partition
      // TODO: Currently this fails because per tile processing cannot access the interpreter
      // TODO: This is because every tile in a partition is handled in a separate thread.
      iter.flatMap(key_and_tile => {
        val multiBandTile: MultibandTile = key_and_tile._2
        val tileRows = multiBandTile.bands(0).rows
        val tileCols = multiBandTile.bands(0).cols
        val tileShape = List(multiBandTile.bandCount, tileRows, tileCols)
        val tileSize = tileRows * tileCols
        val multiBandTileSize = multiBandTile.bandCount * tileSize

        var resultMultiBandTile = multiBandTile
        val interp = _createSharedInterpreter()
        try {
          interp.exec(DEFAULT_IMPORTS)

          // Convert multiBandTile to DirectNDArray
          // Allocating a direct buffer is expensive.
          val buffer = ByteBuffer.allocateDirect(multiBandTileSize * SIZE_OF_FLOAT).order(ByteOrder.nativeOrder()).asFloatBuffer()
          multiBandTile.bands.foreach(tile => {
            val tileFloats: Array[Float] = tile.toArrayDouble.map(_.toFloat)
            buffer.put(tileFloats, 0, tileFloats.length)
          })
          val directTile = new DirectNDArray[FloatBuffer](buffer, tileShape: _*)

          // Setup the xarray datacube.
          val spatialExtent = _createExtentFromSpatialKey(layer.metadata.layout, key_and_tile._1.spatialKey)
          _setXarraydatacubeInPython(interp, directTile, tileShape, spatialExtent, bandNames)
          // Convert context from jep.PyJMap to python dict.
          _setContextInPython(interp, context)

          // Execute the UDF in python.
          interp.exec("data = UdfData(proj={\"EPSG\": 900913}, datacube_list=[datacube], user_context=context)")
          interp.exec(code)
          interp.exec("result_cube = apply_datacube(data.get_datacube_list()[0], data.user_context)")

          // Convert the result back to a MultibandTile.
          val resultDimensions = interp.getValue("result_cube.get_array().values.shape").asInstanceOf[java.util.List[Long]].asScala.toList.map(_.toInt)
          val resultCube = interp.getValue("result_cube.get_array().values")
          val resultBuffer: FloatBuffer =
            resultCube match {
              case cube: DirectNDArray[FloatBuffer] =>
                // The datacube was modified inplace.
                cube.getData
              case cube: NDArray[Array[Float]] =>
                // UDF created a new datacube.
                if (resultDimensions.length < 2) {
                  throw new IllegalArgumentException((
                    "UDF returned a datacube that has less than 2 dimensions. " +
                      "Actual dimensions: (%s).").format(resultDimensions.mkString(", "))
                  )
                }
                val dtype = interp.getValue("str(result_cube.get_array().values.dtype)").asInstanceOf[String]
                _checkOutputDtype(dtype)
                if(newLayout.isEmpty)
                  _checkOutputSpatialDimensions(resultDimensions, tileRows, tileCols)
                println(cube.getData)

                FloatBuffer.wrap(cube.getData)
            }

          // UDFs can
          //  * add/remove band coordinates but not rows, cols.
          //  * remove the band or date dimension
          // UDFs can not
          //  * add/remove time coordinates (Since map returns one SpaceTimeKey and MultiBandTile)
          // TODO: This is how it is done in apply_tiles (python), check if this meets user requirements.
          val resultHasBandDimension = interp.getValue("'bands' in result_cube.get_array().dims").asInstanceOf[Boolean]

          var newNumberOfBands = 1
          if (resultHasBandDimension) {
            newNumberOfBands = interp
              .getValue("result_cube.get_array().coords['bands'].values.size")
              .asInstanceOf[Long].toInt
          }
          resultBuffer.rewind()
          resultMultiBandTile = _extractMultibandTileFromBuffer(resultBuffer, newNumberOfBands, tileSize, tileCols, tileRows)
        } finally if (interp != null) interp.close()

        if (newLayout.isDefined) {
          logger.info(s"UDF created this spatial layout for the raster data cube: $newLayout")
          var newExtent: Extent = key_and_tile._1.spatialKey.extent(oldLayout) //TODO: don't assume that extent stays the same, but determine extent of the output based on result XArray Coords
          newLayout.get.mapTransform(newExtent)
            .coordsIter
            .map { spatialComponent =>
              val outKey: SpatialKey = spatialComponent

              val newTile = multiBandTile.prototype(FloatConstantNoDataCellType, tileCols, tileRows)
              (SpaceTimeKey(outKey,key_and_tile._1.time), newTile.merge(
                newLayout.get.mapTransform.keyToExtent(outKey),
                newExtent,
                resultMultiBandTile,
                NearestNeighbor
              ))
            }
        }else{
          Some((key_and_tile._1, resultMultiBandTile))
        }

      })
    }, preservesPartitioning = newLayout.isEmpty)

    ContextRDD(result, layer.metadata.copy(layout=newLayout.getOrElse(layer.metadata.layout)))
  }
}
