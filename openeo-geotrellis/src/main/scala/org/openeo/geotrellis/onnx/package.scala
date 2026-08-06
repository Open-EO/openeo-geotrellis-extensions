package org.openeo.geotrellis

import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import geotrellis.layer.{Bounds, SpatialComponent}
import geotrellis.raster.{ByteArrayTile, ByteCells, DoubleArrayTile, DoubleCells, FloatArrayTile, FloatCells, IntArrayTile, IntCells, MultibandTile, ShortArrayTile, ShortCells}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD}
import geotrellis.util.Component
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import java.net.URL
import scala.reflect.ClassTag

package object onnx {

  private val logger = LoggerFactory.getLogger(getClass)

  private def flattenNestedArray(multiArray: Array[_], outputShape: Array[Long], onnxType:OnnxJavaType): MultibandTile = {
    val shapeDimension = outputShape.length
    onnxType match {
      // TODO check if the multiArray contains the right type (same as the onnx type) and throw clear error if not.
      case OnnxJavaType.FLOAT =>
        val resultArray = shapeDimension match {
          case 4 => multiArray.asInstanceOf[Array[Array[Array[Array[Float]]]]].flatten
          case 3 => multiArray.asInstanceOf[Array[Array[Array[Float]]]]
          case 2 => Array(multiArray.asInstanceOf[Array[Array[Float]]])
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(x => FloatArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))
        MultibandTile(flattenBands)
      case OnnxJavaType.DOUBLE =>
        val resultArray = shapeDimension match {
          case 4 => multiArray.asInstanceOf[Array[Array[Array[Array[Double]]]]].flatten
          case 3 => multiArray.asInstanceOf[Array[Array[Array[Double]]]]
          case 2 => Array(multiArray.asInstanceOf[Array[Array[Double]]])
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(x => DoubleArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))
        MultibandTile(flattenBands)
      case OnnxJavaType.INT32 =>
        val resultArray = shapeDimension match {
          case 4 => multiArray.asInstanceOf[Array[Array[Array[Array[Int]]]]].flatten
          case 3 => multiArray.asInstanceOf[Array[Array[Array[Int]]]]
          case 2 => Array(multiArray.asInstanceOf[Array[Array[Int]]])
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(x => IntArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))
        MultibandTile(flattenBands)
      case OnnxJavaType.INT16 =>
        val resultArray = shapeDimension match {
          case 4 => multiArray.asInstanceOf[Array[Array[Array[Array[Short]]]]].flatten
          case 3 => multiArray.asInstanceOf[Array[Array[Array[Short]]]]
          case 2 => Array(multiArray.asInstanceOf[Array[Array[Short]]])
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(x => ShortArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))
        MultibandTile(flattenBands)
      case OnnxJavaType.INT8 =>
        val resultArray = shapeDimension match {
          case 4 => multiArray.asInstanceOf[Array[Array[Array[Array[Byte]]]]].flatten
          case 3 => multiArray.asInstanceOf[Array[Array[Array[Byte]]]]
          case 2 => Array(multiArray.asInstanceOf[Array[Array[Byte]]])
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(x => ByteArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))
        MultibandTile(flattenBands)
      case onnxType => throw new IllegalArgumentException(f"ONNX: Unsupported output type of ONNX model : $onnxType")
    }
  }

  private def flattenNestedArray(multiArray: Array[_], outputShape: Array[Long], dimOrder:Seq[String], onnxType:OnnxJavaType): Seq[MultibandTile] = {
    val shapeDimension = outputShape.length
    onnxType match {
      case OnnxJavaType.FLOAT =>
        val resultArray = dimOrder.sorted match {
          case Seq("bands", "batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Float]]]]]
          case Seq("batch", "class", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Float]]]]]
          case Seq("bands", "height", "width") => Array(multiArray.asInstanceOf[Array[Array[Array[Float]]]])
          case Seq("batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Float]]]].map(x => Array(x))
          case Seq("height", "width") => Array(Array(multiArray.asInstanceOf[Array[Array[Float]]]))
          case Seq("bands", "batch") => multiArray.asInstanceOf[Array[Array[Float]]].map(_.map(x => Array(Array(x))))
          case Seq("batch", "class") => multiArray.asInstanceOf[Array[Array[Float]]].map(_.map(x => Array(Array(x))))
          case Seq("bands") => Array(multiArray.asInstanceOf[Array[Float]]).map(x => Array(Array(x)))
          case Seq("class") => Array(multiArray.asInstanceOf[Array[Float]]).map(x => Array(Array(x)))
          case Seq("batch") => multiArray.asInstanceOf[Array[Float]].map(x => Array(Array(Array(x))))
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")}, with dimOrder: ${dimOrder.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(batch => MultibandTile(batch.map(x => FloatArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))))
        flattenBands
      case OnnxJavaType.DOUBLE =>
        val resultArray = dimOrder.sorted match {
          case Seq("bands", "batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Double]]]]]
          case Seq("batch", "class", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Double]]]]]
          case Seq("bands", "height", "width") => Array(multiArray.asInstanceOf[Array[Array[Array[Double]]]])
          case Seq("batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Double]]]].map(x => Array(x))
          case Seq("height", "width") => Array(Array(multiArray.asInstanceOf[Array[Array[Double]]]))
          case Seq("bands", "batch") => multiArray.asInstanceOf[Array[Array[Double]]].map(x => Array(Array(x)))
          case Seq("batch", "class") => multiArray.asInstanceOf[Array[Array[Double]]].map(x => Array(Array(x)))
          case Seq("bands") => Array(multiArray.asInstanceOf[Array[Double]]).map(x => Array(Array(x)))
          case Seq("class") => Array(multiArray.asInstanceOf[Array[Double]]).map(x => Array(Array(x)))
          case Seq("batch") => multiArray.asInstanceOf[Array[Double]].map(x => Array(Array(Array(x))))
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(batch => MultibandTile(batch.map(x => DoubleArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))))
        flattenBands
      case OnnxJavaType.INT32 =>
        val resultArray = dimOrder.sorted match {
          case Seq("bands", "batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Int]]]]]
          case Seq("batch", "class", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Int]]]]]
          case Seq("bands", "height", "width") => Array(multiArray.asInstanceOf[Array[Array[Array[Int]]]])
          case Seq("batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Int]]]].map(x => Array(x))
          case Seq("height", "width") => Array(Array(multiArray.asInstanceOf[Array[Array[Int]]]))
          case Seq("bands", "batch") => multiArray.asInstanceOf[Array[Array[Int]]].map(x => Array(Array(x)))
          case Seq("batch", "class") => multiArray.asInstanceOf[Array[Array[Int]]].map(x => Array(Array(x)))
          case Seq("bands") => Array(multiArray.asInstanceOf[Array[Int]]).map(x => Array(Array(x)))
          case Seq("class") => Array(multiArray.asInstanceOf[Array[Int]]).map(x => Array(Array(x)))
          case Seq("batch") => multiArray.asInstanceOf[Array[Int]].map(x => Array(Array(Array(x))))
        }
        val flattenBands = resultArray.map(batch => MultibandTile(batch.map(x => IntArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))))
        flattenBands
      case OnnxJavaType.INT16 =>
        val resultArray = dimOrder.sorted match {
          case Seq("bands", "batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Short]]]]]
          case Seq("batch", "class", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Short]]]]]
          case Seq("bands", "height", "width") => Array(multiArray.asInstanceOf[Array[Array[Array[Short]]]])
          case Seq("batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Short]]]].map(x => Array(x))
          case Seq("height", "width") => Array(Array(multiArray.asInstanceOf[Array[Array[Short]]]))
          case Seq("bands", "batch") => multiArray.asInstanceOf[Array[Array[Short]]].map(x => Array(Array(x)))
          case Seq("batch", "class") => multiArray.asInstanceOf[Array[Array[Short]]].map(x => Array(Array(x)))
          case Seq("bands") => Array(multiArray.asInstanceOf[Array[Short]]).map(x => Array(Array(x)))
          case Seq("class") => Array(multiArray.asInstanceOf[Array[Short]]).map(x => Array(Array(x)))
          case Seq("batch") => multiArray.asInstanceOf[Array[Short]].map(x => Array(Array(Array(x))))
        }
        val flattenBands = resultArray.map(batch => MultibandTile(batch.map(x => ShortArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))))
        flattenBands
      case OnnxJavaType.INT8 =>
        val resultArray = dimOrder.sorted match {
          case Seq("bands", "batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Byte]]]]]
          case Seq("batch", "class", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Array[Byte]]]]]
          case Seq("bands", "height", "width") => Array(multiArray.asInstanceOf[Array[Array[Array[Byte]]]])
          case Seq("batch", "height", "width") => multiArray.asInstanceOf[Array[Array[Array[Byte]]]].map(x => Array(x))
          case Seq("height", "width") => Array(Array(multiArray.asInstanceOf[Array[Array[Byte]]]))
          case Seq("bands", "batch") => multiArray.asInstanceOf[Array[Array[Byte]]].map(x => Array(Array(x)))
          case Seq("batch", "class") => multiArray.asInstanceOf[Array[Array[Byte]]].map(x => Array(Array(x)))
          case Seq("bands") => Array(multiArray.asInstanceOf[Array[Byte]]).map(x => Array(Array(x)))
          case Seq("class") => Array(multiArray.asInstanceOf[Array[Byte]]).map(x => Array(Array(x)))
          case Seq("batch") => multiArray.asInstanceOf[Array[Byte]].map(x => Array(Array(Array(x))))
          case _ => throw new IllegalArgumentException(s"ONNX: unsupported output shape:${outputShape.mkString("Array(", ", ", ")")} ")
        }
        val flattenBands = resultArray.map(batch => MultibandTile(batch.map(x => ByteArrayTile(x.flatten, outputShape(shapeDimension-2).toInt,outputShape(shapeDimension-1).toInt))))
        flattenBands
      case onnxType => throw new IllegalArgumentException(f"ONNX: Unsupported output type of ONNX model : $onnxType")
    }
  }

  private def checkShape(shape: Array[Long], rows:Int, cols:Int, bandcount:Option[Int]=None): String = {
    val len = shape.length
    val correctXY =
      if (rows==shape(len-2) && cols==shape(len-1)) ""
      else s"shape of the onnx model should have same dimensions as tile, but got shape ${shape.mkString("Array(", ", ", ")")} and rows and cols are $rows and $cols"
    len match {
      case 2 => correctXY
      case 3 =>
        if (bandcount.isEmpty || bandcount.get==shape(len-3)) correctXY
        else s"band count of model is ${shape(len-3)}, but actual band count is $bandcount "
      case 4 =>
        if (bandcount.isEmpty || bandcount.get==shape(len-3)) correctXY
        else s"band count of model is ${shape(len-3)}, but actual band count is $bandcount "
      case x =>
        if (x<2) s"shape should have at least length 2, but got shape ${shape.mkString("Array(", ", ", ")")}"
        else s"shape should have at most length 4, but got shape ${shape.mkString("Array(", ", ", ")")}"
    }
  }

  def reshape(inputType:OnnxJavaType, tiles:Array[MultibandTile], inputShape:Array[Long]): AnyRef = {
    val inputArray = inputType match {
      case OnnxJavaType.FLOAT =>
        if (!tiles.head.cellType.isInstanceOf[FloatCells])
          throw new IllegalArgumentException(s"ONNX: onnx type float does not match celltype ${tiles.head.cellType}.")
        val flat  = tiles.flatMap(_.bands.flatMap(x => x.toArrayTile().asInstanceOf[FloatArrayTile].array).toArray)
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.DOUBLE =>
        if (!tiles.head.cellType.isInstanceOf[DoubleCells])
          throw new IllegalArgumentException(s"ONNX: onnx type double does not match celltype ${tiles.head.cellType}.")
        val flat  = tiles.flatMap(_.bands.flatMap(x => x.toArrayTile().asInstanceOf[DoubleArrayTile].array).toArray)
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT32 =>
        if (!tiles.head.cellType.isInstanceOf[IntCells])
          throw new IllegalArgumentException(s"ONNX: onnx type int does not match celltype ${tiles.head.cellType}.")
        val flat  = tiles.flatMap(_.bands.flatMap(x => x.toArrayTile().asInstanceOf[IntArrayTile].array).toArray)
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT16 =>
        if (!tiles.head.cellType.isInstanceOf[ShortCells])
          throw new IllegalArgumentException(s"ONNX: onnx type short does not match celltype ${tiles.head.cellType}.")
        val flat  = tiles.flatMap(_.bands.flatMap(x => x.toArrayTile().asInstanceOf[ShortArrayTile].array).toArray)
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT8 =>
        if (!tiles.head.cellType.isInstanceOf[ByteCells])
          throw new IllegalArgumentException(s"ONNX: onnx type byte does not match celltype ${tiles.head.cellType}.")
        val flat  = tiles.flatMap(_.bands.flatMap(x => x.toArrayTile().asInstanceOf[ByteArrayTile].array).toArray)
        OrtUtil.reshape(flat, inputShape)
      case onnxType => throw new IllegalArgumentException(f"ONNX: Unsupported input type of ONNX model : $onnxType")
    }
    inputArray
  }

  def transformOutputShapeWithDimOrder(outputShapeSTAC: Array[Long], outputShapeModel: Seq[Long], dimOrder: Seq[String]): Array[Long] = {
    if (!(outputShapeModel sameElements outputShapeSTAC)) {
      throw new IllegalArgumentException(s"ONNX: output shape of model and STAC are different, outputShape of model is ${outputShapeModel.mkString("Array(", ", ", ")")} and from STAC ${outputShapeSTAC.mkString("Array(", ", ", ")")}")
    }
    (dimOrder.indexOf("batch"), dimOrder.indexOf("bands"), dimOrder.indexOf("height"), dimOrder.indexOf("width")) match {
      case (p, b, h, w) if p >= 0 && b >= 0 && h >= 0 && w >= 0 =>
        Array(outputShapeSTAC(p), outputShapeSTAC(b), outputShapeSTAC(h), outputShapeSTAC(w))
      case (-1, b, h, w) if b >= 0 && h >= 0 && w >= 0  =>
        Array(1, outputShapeSTAC(b), outputShapeSTAC(h), outputShapeSTAC(w))
      case (p, -1, h, w) if p >= 0 && h >= 0 && w >= 0 =>
        dimOrder.indexOf("class") match {
          case -1 => Array(outputShapeSTAC(p), 1, outputShapeSTAC(h), outputShapeSTAC(w))
          case c if c >= 0 => Array(outputShapeSTAC(p), outputShapeSTAC(c), outputShapeSTAC(h), outputShapeSTAC(w))
        }
      case (-1, -1, h, w) if h >= 0 && w >= 0 =>
        dimOrder.indexOf("class") match {
          case -1 => Array(1, 1, outputShapeSTAC(h), outputShapeSTAC(w))
          case c if c >= 0 => Array(1, outputShapeSTAC(c), outputShapeSTAC(h), outputShapeSTAC(w))
        }
      case (p, b, -1, -1) if p >= 0 && b >= 0 =>
        Array(outputShapeSTAC(p), outputShapeSTAC(b), 1, 1)
      case (p, -1, -1, -1) if p >= 0 =>
        dimOrder.indexOf("class") match {
          case -1 => Array(outputShapeSTAC(p), 1, 1, 1)
          case c if c >= 0 => Array(outputShapeSTAC(p), outputShapeSTAC(c), 1, 1)
        }
      case _ =>
        throw new IllegalArgumentException(s"ONNX: unsupported output shape: ${outputShapeSTAC.mkString("Array(", ", ", ")")} with dimOrder: $dimOrder")
    }
  }

  def predictOnnx(tile: MultibandTile, model: String): MultibandTile = {
    val modelPath = Paths.get(model)
    val (modelFile, isTemp) = if (Files.exists(modelPath)) {
      (modelPath,false)
    } else {
      val tempFileName = Files.createTempFile(null, ".onnx")
      FileUtils.copyURLToFile(new URL(model), tempFileName.toFile)
      (tempFileName,true)
    }
    val bandCount = tile.bandCount
    val env = OrtEnvironment.getEnvironment()
    val session = env.createSession(modelFile.toString, new OrtSession.SessionOptions())
    val inputNames = session.getInputNames
    val outputNames = session.getOutputNames

    val inputName = inputNames.toArray()(0).asInstanceOf[String]
    val inputInfo = session.getInputInfo.get(inputName).getInfo.asInstanceOf[TensorInfo]
    val outputName = outputNames.toArray()(0).asInstanceOf[String]
    val outputInfo = session.getOutputInfo.get(outputName).getInfo.asInstanceOf[TensorInfo]

    val inputType = inputInfo.`type`
    val outputType = outputInfo.`type`
    val inputShape = inputInfo.getShape
    val outputShape = outputInfo.getShape

    val errorMessageInput = checkShape(inputShape, tile.cols, tile.rows, Some(bandCount))
    if (errorMessageInput.nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported input shape: $errorMessageInput.")
    val errorMessageOutput = checkShape(outputShape, tile.cols, tile.rows)
    if (errorMessageOutput.nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported output shape: $errorMessageOutput.")
    val inputArray = reshape(inputType, Array(tile), inputShape)
    val tensor = OnnxTensor.createTensor(env, inputArray)
    val inputs = java.util.Map.of(inputName, tensor)
    val onnxResults = session.run(inputs)
    val resultValue = onnxResults.get(0).getValue.asInstanceOf[Array[_]]
    val flattenedResult = flattenNestedArray(resultValue, outputShape, outputType)
    if (isTemp) Files.delete(modelFile)
    flattenedResult
  }

  def predictOnnxBatch(tiles: Seq[MultibandTile], model: String): Seq[MultibandTile] = {
    val parsedModel = StacModelParser.parse(model)
    val modelPathStr = parsedModel.modelAssetHref
    val modelPath = Paths.get(modelPathStr)
    val (modelFile, isTemp) = if (Files.exists(modelPath)) {
      (modelPath,false)
    } else {
      val tempFileName = Files.createTempFile(null, ".onnx")
      FileUtils.copyURLToFile(new URL(modelPathStr), tempFileName.toFile)
      (tempFileName,true)
    }
    val tile = tiles.head
    val bandCount = tile.bandCount
    val env = OrtEnvironment.getEnvironment()
    val session = env.createSession(modelFile.toString, new OrtSession.SessionOptions())
    val inputNames = session.getInputNames
    val outputNames = session.getOutputNames

    val inputName = inputNames.toArray()(0).asInstanceOf[String]
    val inputInfo = session.getInputInfo.get(inputName).getInfo.asInstanceOf[TensorInfo]
    val outputName = outputNames.toArray()(0).asInstanceOf[String]
    val outputInfo = session.getOutputInfo.get(outputName).getInfo.asInstanceOf[TensorInfo]

    val inputType = inputInfo.`type`
    val inputShape = inputInfo.getShape
    val errorMessageInput = checkShape(inputShape, tile.cols, tile.rows, Some(bandCount))
    if (errorMessageInput.nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported input shape: $errorMessageInput.")

    val outputType = outputInfo.`type`
    val outputShape = outputInfo.getShape
    val outputDimOrder = parsedModel.outputs.head.dimOrder
    val outputShapeSTAC = parsedModel.outputs.head.shape
    val newOutputShape = transformOutputShapeWithDimOrder(outputShape, outputShapeSTAC, outputDimOrder)
    val groupedTiles = tiles.toArray.grouped(newOutputShape(0).toInt).toSeq
    val result = groupedTiles.flatMap(tiles => {
      val inputArray = reshape(inputType, tiles, inputShape)
      val tensor = OnnxTensor.createTensor(env, inputArray)
      val inputs = java.util.Map.of(inputName, tensor)
      val onnxResults = session.run(inputs)
      val resultValue = onnxResults.get(0).getValue.asInstanceOf[Array[_]]
      val flattenedResult = flattenNestedArray(resultValue, newOutputShape, outputDimOrder, outputType)
      flattenedResult
    })
    if (isTemp) Files.delete(modelFile)
    result

  }


  def predictONNXModel[K: SpatialComponent: ClassTag, M: Component[*, Bounds[K]]](datacube: MultibandTileLayerRDD[K], model:String): MultibandTileLayerRDD[K] = {
    val modelPath = Paths.get(model)
    val (modelFile, isTemp) = if (Files.exists(modelPath)) {
      (modelPath,false)
    } else {
      val tempFileName = Files.createTempFile(null, ".onnx")
      FileUtils.copyURLToFile(new URL(model), tempFileName.toFile)
      (tempFileName,true)
    }
    val env = OrtEnvironment.getEnvironment()
    val session = env.createSession(modelFile.toString, new OrtSession.SessionOptions())
    val inputNames = session.getInputNames
    val outputNames = session.getOutputNames

    if (inputNames.size() > 1)
      // TODO support the case for multiple inputs
      throw new IllegalArgumentException(
        s"ONNX: Only supports one input, but got ${inputNames.size()}: $inputNames.")
    if (outputNames.size() > 1)
      // TODO support the case for multiple outputs
      throw new IllegalArgumentException(
        s"ONNX: Only supports one output, but got ${outputNames.size()}: $outputNames.")


    val inputName = inputNames.toArray()(0).asInstanceOf[String]
    val inputInfo = session.getInputInfo.get(inputName).getInfo.asInstanceOf[TensorInfo]
    val outputName = outputNames.toArray()(0).asInstanceOf[String]
    val outputInfo = session.getOutputInfo.get(outputName).getInfo.asInstanceOf[TensorInfo]

    val inputType = inputInfo.`type`
    val outputType = outputInfo.`type`
    if (inputType != outputType)
      throw new IllegalArgumentException(s"ONNX: only supports models with the same input type as output types, but got input type $inputType and output type $outputType.")

    val inputShape = inputInfo.getShape
    val tileCols = datacube.metadata.tileLayout.tileCols
    val tileRows = datacube.metadata.tileLayout.tileRows
    val retiled = if (tileCols != inputShape(inputShape.length-1) || tileRows != inputShape(inputShape.length-2)) {
      logger.info(f"ONNX: retile datacube for ($tileCols,$tileRows) to (${inputShape(inputShape.length-1)},${inputShape(inputShape.length-2)})")
      val processes = new OpenEOProcesses()
      processes.retileGeneric(datacube,inputShape(inputShape.length-2).toInt,inputShape(inputShape.length-1).toInt,0,0)
    } else datacube
    if (isTemp) Files.delete(modelFile)
    ContextRDD(
      retiled.mapValues(x => onnx.predictOnnx(x,model)),
      retiled.metadata
    )
  }

  def predictONNXSTAC[K: SpatialComponent: ClassTag, M: Component[*, Bounds[K]]](datacube: MultibandTileLayerRDD[K], model:String): MultibandTileLayerRDD[K] = {
    val parsedModel = StacModelParser.parse(model)
    val modelName = parsedModel.modelName
    val modelUrl = parsedModel.modelAssetHref
    val inputs = parsedModel.inputs
    if (inputs.length > 1)
      throw new IllegalArgumentException(s"ONNX: Only supports one input, but got ${inputs.length}.")
    val outputs = parsedModel.outputs
    if (outputs.length > 1)
      throw new IllegalArgumentException(s"ONNX: Only supports one output, but got ${outputs.length}.")

    val input = inputs.head
    val inputDataType = input.dataType
    val output = outputs.head
    val outputDataType = output.dataType

    if (inputDataType != outputDataType)
      throw new IllegalArgumentException(s"ONNX: only supports models with the same input type as output types, but got input type $inputDataType and output type $outputDataType.")

    val inputDimOrder = input.dimOrder
    val inputShape = input.shape
    if (inputShape.length != inputDimOrder.length)
      throw new IllegalArgumentException(s"ONNX: input shape length (${inputShape.length}) does not match input dim order length (${inputDimOrder.length}), shape is $inputShape and dim order is $inputDimOrder.")
    val outputDimOrder = output.dimOrder
    val outputShape = output.shape
    if (outputShape.length != outputDimOrder.length)
      throw new IllegalArgumentException(s"ONNX: output shape length (${outputShape.length}) does not match output dim order length (${outputDimOrder.length}), shape is $outputShape and dim order is $outputDimOrder.")
    val (inputCols, inputRows, batchSize)= inputShape.length match {
      case 1 =>
        throw new IllegalArgumentException(s"ONNX: only supports models with 2 or more dimensions, but got ${inputShape.length}.")
      case 2 =>
        if (inputDimOrder(0) != "height")
          logger.warn(s"ONNX: input dim order does not have 'height' as the first dimension, but got ${inputDimOrder(0)}.")
        if (inputDimOrder(1) != "width")
          logger.warn(s"ONNX: input dim order does not have 'width' as the second dimension, but got ${inputDimOrder(1)}.")
        (inputShape(1), inputShape(0), 0)
      case 3 =>
        if (inputDimOrder(0) != "bands")
          logger.warn(s"ONNX: input dim order does not have 'bands' as the first dimension, but got ${inputDimOrder(0)}.")
        if (inputDimOrder(1) != "height")
          logger.warn(s"ONNX: input dim order does not have 'height' as the second dimension, but got ${inputDimOrder(1)}.")
        if (inputDimOrder(2) != "width")
          logger.warn(s"ONNX: input dim order does not have 'width' as the third dimension, but got ${inputDimOrder(2)}.")

        val tileCols = datacube.metadata.tileLayout.tileCols
        val tileRows = datacube.metadata.tileLayout.tileRows
        (inputShape(2), inputShape(1), 0)
      case 4 =>
        if (inputDimOrder(0) != "batch")
          throw new IllegalArgumentException(s"ONNX: input dim order does not have 'batch' as the first dimension, but got ${inputDimOrder(0)}.")
        if (inputDimOrder(1) != "bands")
          logger.warn(s"ONNX: input dim order does not have 'bands' as the second dimension, but got ${inputDimOrder(1)}.")
        if (inputDimOrder(2) != "height")
          logger.warn(s"ONNX: input dim order does not have 'height' as the third dimension, but got ${inputDimOrder(2)}.")
        if (inputDimOrder(3) != "width")
          logger.warn(s"ONNX: input dim order does not have 'width' as the fourth dimension, but got ${inputDimOrder(3)}.")
        val tileCols = datacube.metadata.tileLayout.tileCols
        val tileRows = datacube.metadata.tileLayout.tileRows
        (inputShape(3), inputShape(2), inputShape(0).toInt)
      case _ =>
        throw new IllegalArgumentException(s"ONNX: only supports models with 3 or less dimensions, but got ${inputShape.length}.")
    }
    val tileLayout = datacube.metadata.tileLayout
    val tileCols = tileLayout.tileCols
    val tileRows = tileLayout.tileRows
    val retiled = if (tileCols != inputCols || tileRows != inputRows) {
      logger.info(f"ONNX: retile datacube for ($tileCols,$tileRows) to (${inputCols},${inputRows})")
      val processes = new OpenEOProcesses()
      processes.retileGeneric(datacube, inputRows.toInt, inputCols.toInt, 0, 0)
    } else datacube
    val result = retiled.mapPartitions(x => {
      if (x.isEmpty) x
      else {
        val seq = x.toSeq
        val (keys,multiTiles) = seq.unzip
        val predicted = onnx.predictOnnxBatch(multiTiles, model)
        val zipped = keys.zip(predicted)
        zipped.iterator
      }

    }, preservesPartitioning = true)
    ContextRDD(
      result,
      retiled.metadata
    )
  }

}
