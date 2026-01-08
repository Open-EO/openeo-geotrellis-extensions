package org.openeo.geotrellis

import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import geotrellis.raster.{ByteArrayTile, ByteCells, DoubleArrayTile, DoubleCells, FloatArrayTile, FloatCells, IntArrayTile, IntCells, MultibandTile, ShortArrayTile, ShortCells}
import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Paths}
import org.slf4j.LoggerFactory

import java.net.URL

package object onnx {
  private val logger = LoggerFactory.getLogger(getClass)

  private def flattenNestedArray(multiArray: Array[_], outputShape: Array[Long], onnxType:OnnxJavaType): MultibandTile = {
    val shapeDimension = outputShape.length
    logger.info(s"ONNX: flatten array from shape ${outputShape.mkString("(", ", ", ")")}")
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
        if (bandcount.isEmpty || bandcount.get==shape(len-3))
          if (shape(0) != 1) s"first element should be 1 when length is 4, but got ${shape.mkString("Array(", ", ", ")")}"
          else correctXY
        else s"band count of model is ${shape(len-3)}, but actual band count is $bandcount "
      case x =>
        if (x<2) s"shape should have at least length 2, but got shape ${shape.mkString("Array(", ", ", ")")}"
        else s"shape should have at most length 4, but got shape ${shape.mkString("Array(", ", ", ")")}"
    }
  }

  def reshape(inputType:OnnxJavaType, tile:MultibandTile, inputShape:Array[Long]): AnyRef = {
    logger.info(s"input type of the data is ${tile.cellType}")
    val inputArray = inputType match {
      case OnnxJavaType.FLOAT =>
        if (!tile.cellType.isInstanceOf[FloatCells])
          throw new IllegalArgumentException(s"ONNX: onnx type float does not match celltype ${tile.cellType}.")
        val flat = tile.bands.flatMap(x => x.asInstanceOf[FloatArrayTile].array).toArray
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.DOUBLE =>
        if (!tile.cellType.isInstanceOf[DoubleCells])
          throw new IllegalArgumentException(s"ONNX: onnx type double does not match celltype ${tile.cellType}.")
        val flat = tile.bands.flatMap(x => x.asInstanceOf[DoubleArrayTile].array).toArray
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT32 =>
        if (!tile.cellType.isInstanceOf[IntCells])
          throw new IllegalArgumentException(s"ONNX: onnx type int does not match celltype ${tile.cellType}.")
        val flat = tile.bands.flatMap(x => x.asInstanceOf[IntArrayTile].array).toArray
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT16 =>
        if (!tile.cellType.isInstanceOf[ShortCells])
          throw new IllegalArgumentException(s"ONNX: onnx type short does not match celltype ${tile.cellType}.")
        val flat = tile.bands.flatMap(x => x.asInstanceOf[ShortArrayTile].array).toArray
        OrtUtil.reshape(flat, inputShape)
      case OnnxJavaType.INT8 =>
        if (!tile.cellType.isInstanceOf[ByteCells])
          throw new IllegalArgumentException(s"ONNX: onnx type byte does not match celltype ${tile.cellType}.")
        val flat = tile.bands.flatMap(x => x.asInstanceOf[ByteArrayTile].array).toArray
        OrtUtil.reshape(flat, inputShape)
      case onnxType => throw new IllegalArgumentException(f"ONNX: Unsupported input type of ONNX model : $onnxType")
    }
    inputArray
  }

  def predictOnnx(tile: MultibandTile, model: String): MultibandTile = {
    val modelPath = Paths.get(model)
    logger.info(s"ONNX: get model from $modelPath")
    val (modelFile, isTemp) = if (Files.exists(modelPath)) {
      (modelPath,false)
    } else {
      val tempFileName = Files.createTempFile(null, ".onnx")
      logger.info(s"ONNX: copy file from $modelPath to $tempFileName")
      FileUtils.copyURLToFile(new URL(model), tempFileName.toFile)
      (tempFileName,true)
    }
    logger.info("ONNX: start predictOnnx")
    val bandCount = tile.bandCount
    val env = OrtEnvironment.getEnvironment()
    val session = env.createSession(modelFile.toString, new OrtSession.SessionOptions())
    logger.info(s"ONNX: created OrtEnvironment")
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

    val outputShape = outputInfo.getShape

    val errorMessageInput = checkShape(inputShape, tile.cols, tile.rows, Some(bandCount))
    logger.info(s"number of bands of the data is $bandCount")
    if (errorMessageInput.nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported input shape: $errorMessageInput.")
    val errorMessageOutput = checkShape(outputShape, tile.cols, tile.rows)
    if (errorMessageOutput.nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported output shape: $errorMessageOutput.")

    logger.info(s"input type of the model $inputType, output type of the model $outputType")
    val inputArray = reshape(inputType, tile, inputShape)
    val tensor = OnnxTensor.createTensor(env, inputArray)
    val inputs = java.util.Map.of(inputName, tensor)
    val onnxResults = session.run(inputs)
    val resultValue = onnxResults.get(0).getValue.asInstanceOf[Array[_]]
    val flattenedResult = flattenNestedArray(resultValue, outputShape, outputType)
    if (isTemp) Files.delete(modelFile)
    flattenedResult
  }

}
