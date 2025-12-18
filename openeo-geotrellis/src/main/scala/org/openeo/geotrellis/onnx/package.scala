package org.openeo.geotrellis

import ai.onnxruntime.{OnnxJavaType, OnnxTensor, OrtEnvironment, OrtSession, OrtUtil, TensorInfo}
import geotrellis.raster.{ByteArrayTile, ByteCells, DoubleArrayTile, DoubleCells, FloatArrayTile, FloatCells, IntArrayTile, IntCells, MultibandTile, ShortArrayTile, ShortCells}

package object onnx {

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

  private def checkShape(shape: Array[Long], rows:Int, cols:Int): String = {
    val len = shape.length
    val correctXY =
      if (rows==shape(len-2) && cols==shape(len-1)) ""
      else s"shape of the onnx model should have same dimensions as tile, but got shape ${shape.mkString("Array(", ", ", ")")} and rows and cols are $rows and $cols"
    len match {
      case 2 => correctXY
      case 3 => correctXY
      case 4 =>
        if (shape(0) != 1) s"first element should be 1 when length is 4, but got ${shape.mkString("Array(", ", ", ")")}"
        else correctXY
      case x =>
        if (x<2) s"shape should have at least length 2, but got shape ${shape.mkString("Array(", ", ", ")")}"
        else s"shape should have at most length 4, but got shape ${shape.mkString("Array(", ", ", ")")}"
    }
  }

  def reshape(inputType:OnnxJavaType, tile:MultibandTile, inputShape:Array[Long]): AnyRef = {
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
    val env = OrtEnvironment.getEnvironment()
    val session = env.createSession(model, new OrtSession.SessionOptions())
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
    val inputShape = inputInfo.getShape

    val outputName = outputNames.toArray()(0).asInstanceOf[String]
    val outputInfo = session.getOutputInfo.get(outputName).getInfo.asInstanceOf[TensorInfo]
    val outputShape = outputInfo.getShape

    if (checkShape(inputShape, tile.cols, tile.rows).nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported input shape: ${checkShape(inputShape, tile.cols, tile.rows)}.")
    if (checkShape(outputShape, tile.cols, tile.rows).nonEmpty)
      throw new IllegalArgumentException(s"ONNX: unsupported input shape: ${checkShape(outputShape, tile.cols, tile.rows)}.")

    val inputType = inputInfo.`type`
    val outputType = outputInfo.`type`

    if (inputType != outputType)
      throw new IllegalArgumentException(s"ONNX: only supports models with the same input type as output types, but got input type $inputType and output type $outputType.")

    val inputArray = reshape(inputType, tile, inputShape)
    val tensor = OnnxTensor.createTensor(env, inputArray)
    val inputs = java.util.Map.of(inputName, tensor)
    val results = session.run(inputs)
    val resultValue = results.get(0).getValue.asInstanceOf[Array[_]]
    flattenNestedArray(resultValue, outputShape, outputType)
  }

}
