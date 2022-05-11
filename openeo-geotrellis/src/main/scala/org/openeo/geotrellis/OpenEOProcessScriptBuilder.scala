package org.openeo.geotrellis

import ai.catboost.CatBoostModel
import ai.catboost.spark.CatBoostClassificationModel
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{ArrayTile, ByteUserDefinedNoDataCellType, CellType, Dimensions, DoubleConstantTile, FloatConstantNoDataCellType, FloatConstantTile, IntConstantNoDataCellType, IntConstantTile, MultibandTile, MutableArrayTile, NODATA, ShortConstantTile, Tile, UByteConstantTile, UByteUserDefinedNoDataCellType, UShortUserDefinedNoDataCellType, isData, isNoData}
import org.apache.commons.math3.exception.NotANumberException
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.ranking.NaNStrategy
import org.apache.spark.ml
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.openeo.geotrellis.mapalgebra.{AddIgnoreNodata, LogBase}
import org.slf4j.LoggerFactory
import spire.math.UShort
import spire.syntax.cfor.cfor

import java.util
import scala.Double.NaN
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{immutable, mutable}
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

object OpenEOProcessScriptBuilder{

  private val logger = LoggerFactory.getLogger(classOf[OpenEOProcessScriptBuilder])

  type OpenEOProcess =  Map[String,Any] => (Seq[Tile]  => Seq[Tile] )

  private def wrapSimpleProcess(operator: Seq[Tile] => Seq[Tile]): OpenEOProcess = {
    def wrapper(context:Map[String,Any])(tiles: Seq[Tile]): Seq[Tile] = operator(tiles)
    return wrapper
  }

  private def wrapProcessWithDefaultContext(process: OpenEOProcess): Seq[Tile] => Seq[Tile]  ={
    (tiles:Seq[Tile]) => process(Map("x"->tiles, "data"-> tiles ))(tiles)
  }

  /**
   * Do f(g(x))
   * @param f
   * @param g
   * @return
   */
  private def composeFunctions(f: Seq[Tile] => Seq[Tile], g: Option[OpenEOProcess]): OpenEOProcess = {
    if (g.isDefined && g.get != null) {
      def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
        f(g.get(context)(tiles))
      }

      composed
    } else
      wrapSimpleProcess(f)
  }



  private def ifElseProcess(value: OpenEOProcess, accept: OpenEOProcess, reject: OpenEOProcess) = {
    val ifElseProcess = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val value_input: Seq[Tile] = evaluateToTiles(value, context, tiles)

      def makeSameLength(tiles: Seq[Tile]): Seq[Tile] = {
        if (tiles.size == 1 && value_input.length > 1) {
          Seq.fill(value_input.length)(tiles(0))
        } else {
          tiles
        }
      }

      val accept_input: Seq[Tile] = makeSameLength(evaluateToTiles(accept, context, tiles))

      val reject_input: Seq[Tile] =
        if (reject != null) {
          reject.apply(context)(tiles)
        } else {
          logger.debug("If process without reject clause.")
          Seq.fill(accept_input.length)(null)
        }

      def ifElse(value: Tile, acceptTile: Tile, rejectTile: Tile): Tile = {
        val outputCellType = if (rejectTile == null) acceptTile.cellType else acceptTile.cellType.union(rejectTile.cellType)
        val resultTile = ArrayTile.empty(outputCellType, acceptTile.cols, acceptTile.rows)

        def setResult(col: Int, row: Int, fromTile: Tile): Unit = {
          if (fromTile == null) {
            if (outputCellType.isFloatingPoint) resultTile.setDouble(col, row, Double.NaN) else resultTile.set(col, row, NODATA)
          } else {
            if (outputCellType.isFloatingPoint) resultTile.setDouble(col, row, fromTile.getDouble(col, row)) else resultTile.set(col, row, fromTile.get(col, row))
          }
        }

        value.foreach { (col, row, value) => {
          if (value == 0) {
            //reject
            setResult(col, row, rejectTile)
          } else {
            //accept
            setResult(col, row, acceptTile)
          }
        }
        }
        resultTile
      }


      if (value_input.size == accept_input.size) {
        value_input.zip(accept_input).zip(reject_input).map { t => ifElse(t._1._1, t._1._2, t._2) }
      } else if (value_input.size == 1) {
        accept_input.zip(reject_input).map { t => ifElse(value_input.head, t._1, t._2) }
      }
      else {
        throw new IllegalArgumentException("Incompatible numbers of tiles in this if process.")
      }

    }
    ifElseProcess
  }

  private def multibandMap(tile: MultibandTile ,f: Array[Double] => Seq[Double]): Seq[Tile] = {
    val mutableResult: immutable.Seq[MutableArrayTile] = tile.bands.map(_.mutable)
    var i = 0
    cfor(0)(_ < tile.cols, _ + 1) { col =>
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        val bandValues = Array.ofDim[Double](tile.bandCount)
        cfor(0)(_ < tile.bandCount, _ + 1) { band =>
          bandValues(band) = mutableResult(band).getDouble(col, row)
        }
        val resultValues = f(bandValues)
        cfor(0)(_ < tile.bandCount, _ + 1) { band =>
          mutableResult(band).setDouble(col, row,resultValues(band))
        }
        i += 1
      }
    }
    return mutableResult
  }

  private def multibandMapToNewTiles(tile: MultibandTile ,f: Seq[Double] => Seq[Double], ignoreNoData: Boolean = true): Seq[Tile] = {
    val mutableResult: ListBuffer[MutableArrayTile] = ListBuffer[MutableArrayTile]()
    var i = 0
    cfor(0)(_ < tile.cols, _ + 1) { col =>
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        val bandValues = new ArrayBuffer[Double](tile.bandCount)
        cfor(0)(_ < tile.bandCount, _ + 1) { band =>
          val d = tile.bands(band).getDouble(col, row)
          if(!ignoreNoData || !d.isNaN){
            bandValues.append(d)
          }
        }
        val resultValues = f(bandValues)

        if(mutableResult.size == 0) {
          mutableResult.appendAll( (0 until resultValues.size).map( d => ArrayTile.empty(tile.cellType,tile.cols,tile.rows)))
        }

        cfor(0)(_ < resultValues.length, _ + 1) { band =>
          mutableResult(band).setDouble(col, row,resultValues(band))
        }
        i += 1
      }
    }
    return mutableResult
  }

  private def multibandReduce(tile: MultibandTile ,f: Seq[Double] => Double, ignoreNoData: Boolean = true): Seq[Tile] = {
    val mutableResult:MutableArrayTile = tile.bands(0).mutable
    var i = 0
    cfor(0)(_ < tile.cols, _ + 1) { col =>
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        val bandValues = new ArrayBuffer[Double](tile.bandCount)
        cfor(0)(_ < tile.bandCount, _ + 1) { band =>
          val d = tile.bands(band).getDouble(col, row)
          if(!ignoreNoData || !d.isNaN){
            bandValues.append(d)
          }
        }
        if(!bandValues.isEmpty) {
          val resultValues = f(bandValues)
          mutableResult.setDouble(col, row,resultValues)
        }
        i += 1
      }
    }
    return Seq(mutableResult)
  }

  // Get `Seq[Tile]` by evaluating given `OpenEOProcess` (if any) on given `Seq[Tile]`
  private def evaluateToTiles(function: OpenEOProcess, context: Map[String, Any], tiles: Seq[Tile]): Seq[Tile] = {
    if (function != null) {
      function.apply(context)(tiles)
    } else {
      tiles
    }
  }


  private def median(tiles:Seq[Tile]) : Seq[Tile] = {
    multibandReduce(MultibandTile(tiles),ts => {
      medianOfDoubles(ts)
    },true)
  }

  private def medianOfDoubles(ts: Seq[Double]) = {
    val (lower, upper) = ts.sortWith(_ < _).splitAt(ts.size / 2)
    if (ts.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  private def medianWithNodata(tiles:Seq[Tile]) : Seq[Tile] = {
    multibandReduce(MultibandTile(tiles),ts => {if(ts.exists(_.isNaN)) Double.NaN else medianOfDoubles(ts)},false)
  }

  private def linearInterpolation(tiles:Seq[Tile]) : Seq[Tile] = {

    multibandMap(MultibandTile(tiles),ts => {
      var previousValid = -1
      var nextValid = -1
      var current = 0
      while(current < ts.length) {

        if(isNoData(ts(current))){
          if(previousValid>=0){
            nextValid = current+1
            while(nextValid< ts.length && isNoData(ts(nextValid))) {
              nextValid +=1
            }
            if(nextValid < ts.length){
              val y0 = ts(previousValid)
              //found a valid point
              //var y = Math.fma( (current-previousValid), (ts(nextValid)-y0)/(nextValid-previousValid), y0.floatValue())
              var y = y0 + (current-previousValid)*(ts(nextValid)-y0)/(nextValid-previousValid)
              ts(current)=y
              previousValid = current
              nextValid = -1
            }
          }
          current +=1

        }else{
          previousValid = current
          nextValid = -1
          current +=1
        }
      }
      ts
    })
  }

  private def countValid(tiles:Seq[Tile]) : Seq[Tile] = {
    multibandReduce(MultibandTile(tiles),ts => ts.length)
  }

  private def countAll(tiles:Seq[Tile]) : Seq[Tile] = {
    multibandReduce(MultibandTile(tiles), ts => ts.length, ignoreNoData = false)
  }

  private def countCondition(tiles:Seq[Tile]) : Seq[Tile] = {
    multibandReduce(MultibandTile(tiles).convert(IntConstantNoDataCellType), ts => ts.count(_ != 0), ignoreNoData = false)
  }

  def getQuantilesProbabilities(arguments: util.Map[String, Object]): Seq[Double] = {
    val qRaw = arguments.get("q")
    val probabilities: Seq[Double] =
      if (qRaw == null) {
        arguments.get("probabilities") match {
          case doubles: util.List[Double] =>
            doubles.asScala.toArray.toSeq
          case doubles: Array[Double] =>
            doubles.asInstanceOf[Array[Double]].toSeq
          case any => throw new IllegalArgumentException(s"Unsupported probabilities parameter in quantiles: ${any} ")
        }
      } else {
        val q = qRaw.asInstanceOf[Int].toDouble

        (1 to (q.intValue() - 1)).map(d => d.doubleValue() / q)
      }
    return probabilities
  }

  object MinIgnoreNoData extends LocalTileBinaryOp {
    def combine(z1:Int,z2:Int) =
      if( isNoData(z1) && isNoData(z2)) NODATA
      else if( isNoData(z1) ) z2
      else if( isNoData(z2) ) z1
      else math.min(z1, z2)

    def combine(z1:Double,z2:Double) =
      if( isNoData(z1) && isNoData(z2)) NaN
      else if( isNoData(z1) ) z2
      else if( isNoData(z2) ) z1
      else math.min(z1, z2)
  }

  object MaxIgnoreNoData extends LocalTileBinaryOp {
    def combine(z1:Int,z2:Int) =
      if( isNoData(z1) && isNoData(z2)) NODATA
      else if( isNoData(z1) ) z2
      else if( isNoData(z2) ) z1
      else math.max(z1, z2)

    def combine(z1:Double,z2:Double) =
      if( isNoData(z1) && isNoData(z2)) NaN
      else if( isNoData(z1) ) z2
      else if( isNoData(z2) ) z1
      else math.max(z1, z2)
  }
}
/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

  import OpenEOProcessScriptBuilder._


  val processStack: mutable.Stack[String] = new mutable.Stack[String]()
  val arrayElementStack: mutable.Stack[Integer] = new mutable.Stack[Integer]()
  val argNames: mutable.Stack[String] = new mutable.Stack[String]()
  val contextStack: mutable.Stack[mutable.Map[String,OpenEOProcess]] = new mutable.Stack[mutable.Map[String, OpenEOProcess]]()
  var arrayCounter : Int =  0
  var inputFunction:  OpenEOProcess = null
  var resultingDataType: CellType = FloatConstantNoDataCellType

  def generateFunction(context: Map[String,Any] = Map.empty): Seq[Tile] => Seq[Tile] = {
    inputFunction(context)
  }

  def generateFunction(): Seq[Tile] => Seq[Tile] = {
    wrapProcessWithDefaultContext(inputFunction)
  }

  /**
   * Return the expected cell type of the output
   * @return
   */
  def getOutputCellType(): CellType = {
    return resultingDataType
  }

  private def unaryFunction(argName: String, operator: Seq[Tile] => Seq[Tile]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction: Option[OpenEOProcess] = storedArgs.get(argName)
    composeFunctions(operator,inputFunction)
  }

  private def mapFunction(argName: String, operator: Tile => Tile): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => tiles.map(operator))
  }

  private def reduceFunction(argName: String, operator: (Tile, Tile) => Tile): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(tiles.reduce(operator)))
  }

  private def applyListFunction(argName: String, operator: Seq[Tile] => Seq[Tile]): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => operator(tiles))
  }

  private def mapListFunction(listArgName: String, mapArgName:String, operator: Seq[Tile] => Seq[Tile]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val mapFunction: Option[OpenEOProcess] = storedArgs.get(mapArgName)
    val listFunction: Option[OpenEOProcess] = storedArgs.get(listArgName)

    (context: Map[String,Any]) => (tiles: Seq[Tile]) => {
      val mapTiles = if (mapFunction.isDefined) mapFunction.get(context)(tiles) else tiles
      composeFunctions((tiles: Seq[Tile]) => operator(tiles), listFunction)(context)(mapTiles)
    }
  }

  private def reduceListFunction(argName: String, operator: Seq[Tile] => Tile): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(operator(tiles)))
  }

  private def ifProcess(arguments:java.util.Map[String,Object]): OpenEOProcess ={
    val storedArgs = contextStack.head
    val value = storedArgs.get("value").getOrElse(throw new IllegalArgumentException("If process expects a value argument. These arguments were found: " + arguments.keys.mkString(", ")))
    val accept = storedArgs.get("accept").getOrElse(throw new IllegalArgumentException("If process expects an accept argument. These arguments were found: " + arguments.keys.mkString(", ")))
    val reject: OpenEOProcess = storedArgs.get("reject").getOrElse(null)
    ifElseProcess(value, accept, reject)
  }


  private def xyFunction(operator:(Tile,Tile) => Tile, xArgName:String = "x", yArgName:String = "y" ,convertBitCells: Boolean = true): OpenEOProcess = {
    val storedArgs = contextStack.head
    val processString = processStack.reverse.mkString("->")
    if (!storedArgs.contains(xArgName)) {
      throw new IllegalArgumentException("This function expects an '" + xArgName + "' argument, function tree: " + processString + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    if (!storedArgs.contains(yArgName)) {
      throw new IllegalArgumentException("This function expects an '" + yArgName + "' argument, function tree: " + processString + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    val x_function: OpenEOProcess = storedArgs.get(xArgName).get
    val y_function: OpenEOProcess = storedArgs.get(yArgName).get
    val bandFunction = (context: Map[String,Any]) => (tiles: Seq[Tile]) => {

      def convertBitCellsOp(aTile: Tile):Tile ={
        if(convertBitCells && aTile.cellType.bits == 1) {
          aTile.convert(ByteUserDefinedNoDataCellType(127.byteValue()))
        }else{
          aTile
        }
      }
      val x_input: Seq[Tile] = evaluateToTiles(x_function, context, tiles).map(convertBitCellsOp)
      val y_input: Seq[Tile] = evaluateToTiles(y_function, context, tiles).map(convertBitCellsOp)
      if(x_input.size == y_input.size) {
        x_input.zip(y_input).map(t=>operator(t._1,t._2))
      }else if(x_input.size == 1) {
        y_input.map(operator(x_input.head,_))
      }else if(y_input.size == 1) {
        x_input.map(operator(_,y_input.head))
      }else{
        throw new IllegalArgumentException("Incompatible numbers of tiles in this XY operation '"+processString+"' " + xArgName + " has:" + x_input.size +", "+yArgName+" has: " + y_input.size+ "\n We expect either equal counts, are one of them should be 1.")
      }

    }
    bandFunction
  }

  def constantArgument(name:String,value:Number): Unit = {
    var scope = contextStack.head
    scope.put(name,wrapSimpleProcess(createConstantTileFunction(value)))
  }

  def constantArguments(args: java.util.Map[String,Object]): Unit = {
    for (elem <- mapAsScalaMap(args)) {
      if(elem._2.isInstanceOf[Number]){
        constantArgument(elem._1,elem._2.asInstanceOf[Number])
      }else if(elem._2.isInstanceOf[Boolean]){
        constantArgument(elem._1,elem._2.asInstanceOf[Boolean])
      }
    }
  }

  def constantArgument(name:String,value:Boolean): Unit = {
    //can be skipped, will simply be available when executing function
  }

  def argumentStart(name:String): Unit = {
    argNames.push(name)
  }

  def fromParameter(parameterName:String): Unit = {
    inputFunction = (context:Map[String,Any]) => (tiles: Seq[Tile]) => {
      if(context.contains(parameterName)) {
        context.getOrElse(parameterName,tiles).asInstanceOf[Seq[Tile]]
      }else{
        logger.debug("Parameter with name: " + parameterName  + "not found. Available parameters: " + context.keys.mkString(","))
        tiles
      }
    }
  }

  def argumentEnd(): Unit = {
    var name = argNames.pop()
    var scope = contextStack.head
    scope.put(name,inputFunction)
    inputFunction = null
  }

  /**
    * Called for each element in the array.
    * @param name
    */
  def arrayStart(name:String): Unit = {

    //save current arrayCounter
    arrayElementStack.push(arrayCounter)
    argNames.push(name)
    contextStack.push(mutable.Map[String,OpenEOProcess]())
    processStack.push("array")
    arrayCounter = 0
  }

  def arrayElementDone():Unit = {
    val scope = contextStack.head
    scope.put(arrayCounter.toString,inputFunction)
    arrayCounter += 1
    inputFunction = null
  }

  private def createConstantTileFunction(value:Number): Seq[Tile] => Seq[Tile] = {
    val constantTileFunction:Seq[Tile] => Seq[Tile] = (tiles:Seq[Tile]) => {
      if(tiles.isEmpty) {
        tiles
      }else{
        val rows= tiles.head.rows
        val cols = tiles.head.cols
        value match {
          case x: java.lang.Byte => Seq(UByteConstantTile(value.byteValue(),cols,rows))
          case x: java.lang.Short => Seq(ShortConstantTile(value.byteValue(),cols,rows))
          case x: Integer => Seq(IntConstantTile(value.intValue(),cols,rows))
          case x: java.lang.Float => Seq(FloatConstantTile(value.floatValue(),cols,rows))
          case _ => Seq(DoubleConstantTile(value.doubleValue(),cols,rows))
        }
      }

    }
    return constantTileFunction
  }

  def constantArrayElement(value: Number):Unit = {
    val constantTileFunction:OpenEOProcess = wrapSimpleProcess(createConstantTileFunction(value))
    val scope = contextStack.head
    scope.put(arrayCounter.toString,constantTileFunction)
    arrayCounter += 1
  }

  def arrayEnd():Unit = {
    val name = argNames.pop()
    val scope = contextStack.pop()
    processStack.pop()

    val nbElements = arrayCounter
    inputFunction = (context:Map[String,Any]) => (tiles:Seq[Tile]) => {
      var results = Seq[Tile]()
      for( i <- 0 until nbElements) {
        val tileFunction = scope.get(i.toString).get
        results = results ++ tileFunction(context)(tiles)
      }
      results
    }
    arrayCounter = arrayElementStack.pop()

    contextStack.head.put(name,inputFunction)

  }


  def expressionStart(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    processStack.push(operator)
    contextStack.push(mutable.Map[String,OpenEOProcess]())
  }

  def expressionEnd(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    // TODO: this is not only about expressions anymore. Rename it to e.g. "leaveProcess" to be more in line with graph visitor in Python?
    logger.debug(operator + " process with arguments: " + contextStack.head.mkString(",") + " direct args: " + arguments.mkString(","))
    // Bit of argument sniffing to support multiple versions/variants of processes
    val hasXY = arguments.containsKey("x") && arguments.containsKey("y")
    val hasX = arguments.containsKey("x")
    val hasExpression = arguments.containsKey("expression")
    val hasExpressions = arguments.containsKey("expressions")
    val hasData = arguments.containsKey("data")
    val ignoreNoData = !(arguments.getOrDefault("ignore_nodata",Boolean.box(true).asInstanceOf[Object]) == Boolean.box(false) || arguments.getOrDefault("ignore_nodata",None) == "false" )
    val hasTrueCondition = Try(arguments.get("condition").toString.toBoolean).getOrElse(false)
    val hasConditionExpression = arguments.get("condition") != null && !arguments.get("condition").isInstanceOf[Boolean.type]

    val operation: OpenEOProcess = operator match {
      case "if" => ifProcess(arguments)
      // Comparison operators
      case "gt" if hasXY => xyFunction(Greater.apply,convertBitCells = false)
      case "lt" if hasXY => xyFunction(Less.apply,convertBitCells = false)
      case "gte" if hasXY => xyFunction(GreaterOrEqual.apply,convertBitCells = false)
      case "lte" if hasXY => xyFunction(LessOrEqual.apply,convertBitCells = false)
      case "between" if hasX => betweenFunction(arguments)
      case "eq" if hasXY => xyFunction(Equal.apply,convertBitCells = false)
      case "neq" if hasXY => xyFunction(Unequal.apply,convertBitCells = false)
      // Boolean operators
      case "not" if hasX => mapFunction("x", Not.apply)
      case "not" if hasExpression => mapFunction("expression", Not.apply) // legacy 0.4 style
      case "and" if hasXY => xyFunction(And.apply,convertBitCells = false)
      case "and" if hasExpressions => reduceFunction("expressions", And.apply) // legacy 0.4 style
      case "or" if hasXY => xyFunction(Or.apply,convertBitCells = false)
      case "or" if hasExpressions => reduceFunction("expressions", Or.apply) // legacy 0.4 style
      case "xor" if hasXY => xyFunction(Xor.apply,convertBitCells = false)
      case "xor" if hasExpressions => reduceFunction("expressions", Xor.apply) // legacy 0.4 style
      // Mathematical operations
      case "sum" if hasData && !ignoreNoData => reduceFunction("data", Add.apply)
      case "sum" if hasData && ignoreNoData => reduceFunction("data", AddIgnoreNodata.apply)
      case "add" if hasXY => xyFunction(Add.apply)
      case "subtract" if hasXY => xyFunction(Subtract.apply)
      case "subtract" if hasData => reduceFunction("data", Subtract.apply) // legacy 0.4 style
      case "product" if hasData => reduceFunction("data", Multiply.apply)
      case "multiply" if hasXY => xyFunction(Multiply.apply)
      case "multiply" if hasData => reduceFunction("data", Multiply.apply) // legacy 0.4 style
      case "divide" if hasXY => xyFunction(Divide.apply)
      case "divide" if hasData => reduceFunction("data", Divide.apply) // legacy 0.4 style
      case "power" => xyFunction(Pow.apply,xArgName = "base",yArgName = "p")
      case "normalized_difference" if hasXY => xyFunction((x, y) => Divide(Subtract(x, y), Add(x, y)))
      case "clip" => clipFunction(arguments)
      // Statistics
      case "max" if hasData && !ignoreNoData => reduceFunction("data", Max.apply)
      case "max" if hasData && ignoreNoData => reduceFunction("data", MaxIgnoreNoData.apply)
      case "min" if hasData && !ignoreNoData => reduceFunction("data", Min.apply)
      case "min" if hasData && ignoreNoData => reduceFunction("data", MinIgnoreNoData.apply)
        //TODO take ignorenodata into account!
      case "mean" if hasData => reduceListFunction("data", Mean.apply)
      case "variance" if hasData => reduceListFunction("data", Variance.apply)
      case "sd" if hasData => reduceListFunction("data", Sqrt.apply _ compose Variance.apply)
      case "median" if ignoreNoData => applyListFunction("data",median)
      case "median" => applyListFunction("data",medianWithNodata)
      case "count" if hasTrueCondition => applyListFunction("data", countAll)
      case "count" if hasConditionExpression => mapListFunction("data", "condition", countCondition)
      case "count" => applyListFunction("data", countValid)
      // Unary math
      case "abs" if hasX => mapFunction("x", Abs.apply)
      case "absolute" if hasX => mapFunction("x", Abs.apply)
      //TODO: "int" integer part of a number
      //TODO "arccos" -> Acosh.apply,
      //TODO: arctan 2 is not unary! "arctan2" -> Atan2.apply,
      case "log" => xyFunction(LogBase.apply,xArgName = "x",yArgName = "base")
      case "ln" if hasX => mapFunction( "x", Log.apply)
      case "sqrt" if hasX => mapFunction("x", Sqrt.apply)
      case "ceil" if hasX => mapFunction("x", Ceil.apply)
      case "floor" if hasX => mapFunction("x", Floor.apply)
      case "round" if hasX => mapFunction("x", Round.apply)
      case "arccos" if hasX => mapFunction("x", Acos.apply)
      case "arcsin" if hasX => mapFunction("x", Asin.apply)
      case "arctan" if hasX => mapFunction("x", Atan.apply)
      case "cos" if hasX => mapFunction("x", Cos.apply)
      case "cosh" if hasX => mapFunction("x", Cosh.apply)
      case "sin" if hasX => mapFunction("x", Sin.apply)
      case "sinh" if hasX => mapFunction("x", Sinh.apply)
      case "tan" if hasX => mapFunction("x", Tan.apply)
      case "tanh" if hasX => mapFunction("x", Tanh.apply)
      // Other
      case "first" if ignoreNoData =>  applyListFunction("data", firstFunctionIgnoreNoData)
      case "first" => applyListFunction("data", firstFunctionWithNodata)
      case "last" if ignoreNoData => applyListFunction("data", lastFunctionIgnoreNoData)
      case "last" => applyListFunction("data", lastFunctionWithNoData)
      case "is_nodata" if hasX => mapFunction("x", Undefined.apply)
      case "is_nan" if hasX => mapFunction("x", Undefined.apply)
      case "array_element" => arrayElementFunction(arguments)
      case "array_modify" => arrayModifyFunction(arguments)
      case "array_interpolate_linear" => applyListFunction("data",linearInterpolation)
      case "linear_scale_range" => linearScaleRangeFunction(arguments)
      case "quantiles" => quantilesFunction(arguments,ignoreNoData)
      case "array_concat" => arrayConcatFunction(arguments)
      case "array_append" => arrayAppendFunction(arguments)
      case "array_create" => arrayCreateFunction(arguments)
      case "predict_random_forest" if hasData => predictRandomForestFunction(arguments)
      case "predict_catboost" if hasData => predictCatBoostFunction(arguments)
      case "predict_probabilities" if hasData => predictCatBoostProbabilitiesFunction(arguments)
      case _ => throw new IllegalArgumentException(s"Unsupported operation: $operator (arguments: ${arguments.keySet()})")
    }

    if(operator != "linear_scale_range") {
      //TODO: generalize to other operations that result in a specific datatype?
      resultingDataType = FloatConstantNoDataCellType
    }

    val expectedOperator = processStack.pop()
    assert(expectedOperator.equals(operator))
    contextStack.pop()
    inputFunction = operation
  }

  private def linearScaleRangeFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs: mutable.Map[String, OpenEOProcess] = contextStack.head
    val inMin = arguments.get("inputMin").asInstanceOf[Number].doubleValue()
    val inMax = arguments.get("inputMax").asInstanceOf[Number].doubleValue()
    val outMinRaw = arguments.getOrDefault("outputMin", 0.0.asInstanceOf[Object])
    val outMin: Double = outMinRaw.asInstanceOf[Number].doubleValue()
    val outMaxRaw = arguments.getOrDefault("outputMax", 1.0.asInstanceOf[Object])
    val outMax:Double = outMaxRaw.asInstanceOf[Number].doubleValue()
    val inputFunction = storedArgs.get("x").get
    val output_range = outMax - outMin
    val doTypeCast = output_range > 1 && (!outMinRaw.isInstanceOf[Double] && !outMinRaw.isInstanceOf[Float]) && (!outMaxRaw.isInstanceOf[Double] && !outMaxRaw.isInstanceOf[Float])
    val targetType: Option[CellType] =
    if(doTypeCast){
      if(output_range < 254 && outMin >= 0) {
        Some(UByteUserDefinedNoDataCellType(255.byteValue()))
      }else if(output_range < 65535 && outMin >= 0){
        Some(UShortUserDefinedNoDataCellType(UShort.MaxValue.toShort))
      }else{
        Option.empty
      }
    }else{
      Option.empty
    }

    resultingDataType =
      if(targetType.isDefined) {
        targetType.get
      }else{
        FloatConstantNoDataCellType
      }


    val scaleFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val input: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val normalizedTiles = input.map(_.normalize(inMin,inMax,outMin,outMax).mapIfSetDouble(p=> {
        if(p<outMin) {
          outMin
        }else if(p>outMax) {
          outMax
        }else{
          p
        }
      }))
      if(targetType.isDefined) {
        normalizedTiles.map(_.convert(targetType.get))
      }else{
        normalizedTiles
      }
    }
    scaleFunction

  }

  private def quantilesFunction(arguments:java.util.Map[String,Object], ignoreNoData:Boolean = true): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("data").get
    val probabilities = getQuantilesProbabilities(arguments)


    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val data: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)

      multibandMapToNewTiles(MultibandTile(data),ts => {

        val p = if(ignoreNoData){
          new Percentile()
        }else{
          new Percentile().withNaNStrategy(NaNStrategy.FAILED)
        }
        p.setData(ts.toArray)

        try{
          probabilities.map(quantile => p.evaluate(quantile*100.0))
        }catch {
          case e: NotANumberException => probabilities.map(d => Double.NaN)
          case t: Exception => throw t
        }

      }, ignoreNoData)

    }
    bandFunction
  }


  private def arrayModifyFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("data").get
    val valuesFunction = storedArgs.get("values").get
    val index = arguments.getOrDefault("index",null)
    val length = arguments.getOrDefault("length",null)
    if(index == null) {
      throw new IllegalArgumentException("Missing 'index' argument in array_element.")
    }
    if(length!=null) {
      throw new UnsupportedOperationException("Geotrellis backend only supports inserting in array-modify")
    }
    if(!index.isInstanceOf[Integer]){
      throw new IllegalArgumentException("The 'index' argument should be an integer, but got: " + index)
    }
    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val data: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val values: Seq[Tile] = evaluateToTiles(valuesFunction, context, tiles)
      if(length == null) {
        //in this case, we need to insert
        data.take(index.asInstanceOf[Integer]) ++ values ++ data.drop(index.asInstanceOf[Integer])
      }else{
        throw new UnsupportedOperationException("Geotrellis backend only supports inserting in array-modify")
      }

    }
    bandFunction
  }

  private def arrayAppendFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("data").get
    val valueFunction = storedArgs.get("value").get

    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val data: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val values: Seq[Tile] = evaluateToTiles(valueFunction, context, tiles)
      data ++ values
    }
    bandFunction
  }

  private def arrayConcatFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val array1Function = storedArgs("array1")
    val array2Function = storedArgs("array2")

    val bandFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val array1 = evaluateToTiles(array1Function, context, tiles)
      val array2 = evaluateToTiles(array2Function, context, tiles)

      val combined = array1 ++ array2
      if(combined.size>0) {
        val unionCelltype = combined.map(_.cellType).reduce(_.union(_))
        combined.map(_.convert(unionCelltype))
      }else{
        combined
      }

    }
    bandFunction
  }

  private def arrayCreateFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val dataFunction:OpenEOProcess = storedArgs("data")
    val repeat: Int = arguments.getOrDefault("repeat", 1.asInstanceOf[Object]).asInstanceOf[Int]

    val bandFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val data = evaluateToTiles(dataFunction, context, tiles)
      Seq.fill(repeat)(data).flatten
    }
    bandFunction
  }

  private def predictRandomForestFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    checkMlArguments(arguments)
    val operator = (rs: Seq[Tile], context: Map[String, Any]) => {
      val modelCheck = context.getOrElse("context", null)
      if (!modelCheck.isInstanceOf[RandomForestModel])
        throw new IllegalArgumentException(
          s"The 'model' argument should contain a valid random forest model, but got: $modelCheck.")
      val model = context("context").asInstanceOf[RandomForestModel]

      rs.assertEqualDimensions()
      val layerCount = rs.length
      if (layerCount == 0) sys.error(s"No features provided for predict_random_forest.")
      else {
        val newCellType = rs.map(_.cellType).reduce(_.union(_))
        val Dimensions(cols, rows) = rs.head.dimensions
        val tile = ArrayTile.alloc(newCellType, cols, rows)
        var numberOfNoValuePredictions = 0;

        cfor(0)(_ < rows, _ + 1) { row =>
          cfor(0)(_ < cols, _ + 1) { col =>
            val features = new Array[Double](layerCount)
            var featureIsNoData = false;
            breakable {
              cfor(0)(_ < layerCount, _ + 1) { i =>
                val v = rs(i).getDouble(col, row)
                if (isData(v)) {
                  features(i) = v
                } else {
                  // SoftError: Use NoData value as prediction.
                  featureIsNoData = true;
                  numberOfNoValuePredictions += 1;
                  tile.setDouble(col, row, v)
                  break
                }
              }
            }
            if (!featureIsNoData) {
              try {
                val featuresVector = linalg.Vectors.dense(features)
                val prediction = model.predict(featuresVector)
                tile.setDouble(col, row, prediction)
              }
              catch {
                case e: ArrayIndexOutOfBoundsException =>
                  throw new IllegalArgumentException(s"The data to predict only contains ${layerCount} features per row, " +
                    s"but the model was trained on more features.")
              }
            }
          }
        }
        if (numberOfNoValuePredictions != 0) {
          println(s"PredictRandomForest Warning! " +
            s"${numberOfNoValuePredictions}/${rows*cols} cells contained at least one NoData feature. " +
            s"The prediction for those cells has been set to NoData.")
        }
        Seq(tile)
      }
    }

    val storedArgs = contextStack.head
    val inputFunction: Option[OpenEOProcess] = storedArgs.get("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction.get(context)(tiles), context)
    }
    composed
  }

  private def predictCatBoostFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    checkMlArguments(arguments)
    // This operator reduces all layers in rs down to one layer using CatboostModel.predict().
    // For each cell it creates a feature vector from the layers and then generates one prediction for that vector.
    val operator = (rs: Seq[Tile], context: Map[String, Any]) => {
      // 1. Checks.
      val modelCheck = context.getOrElse("context", null)
      if (!modelCheck.isInstanceOf[CatBoostClassificationModel] && !modelCheck.isInstanceOf[CatBoostModel])
        throw new IllegalArgumentException(
          s"The 'model' argument should contain a valid Catboost model, but got: $modelCheck.")
      rs.assertEqualDimensions()
      val layerCount = rs.length
      if (layerCount == 0) sys.error(s"No features provided for predict_catboost.")

      if(context("context").isInstanceOf[CatBoostModel]){

        val model = context("context").asInstanceOf[CatBoostModel]

        def sigmoid(x: Double) = 1. / (1 + Math.pow(Math.E, -x))
        multibandReduce(MultibandTile(rs),ts => {
          val numericalFeatures = ts.map(_.floatValue()).toArray
          val rawPrediction = model.predict(numericalFeatures,null.asInstanceOf[Array[Int]])
          val sigmoids: Array[Double] = rawPrediction.copyRowMajorPredictions().map(sigmoid)
          val theClass = sigmoids.indices.maxBy(sigmoids)
          theClass

        },true)
      }else{
        val model = context("context").asInstanceOf[CatBoostClassificationModel]

        def sigmoid(x: Double) = 1. / (1 + Math.pow(Math.E, -x))
        multibandReduce(MultibandTile(rs),ts => {
          val featureArray: Array[Double] = ts.map(_.doubleValue()).toArray
          val numericalFeatures = ml.linalg.Vectors.dense(featureArray)
          val rawPrediction = model.predictRaw(numericalFeatures)
          val sigmoids: Array[Double] = rawPrediction.toArray.map(sigmoid)
          val theClass = sigmoids.indices.maxBy(sigmoids)
          theClass
        },true)
      }
    }
    // Return our operator in a composed function.
    val storedArgs = contextStack.head
    val inputFunction: Option[OpenEOProcess] = storedArgs.get("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction.get(context)(tiles), context)
    }
    composed
  }

  private def predictCatBoostProbabilitiesFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    checkMlArguments(arguments)
    // This operator reduces all layers in rs down to one layer using CatboostModel.predict().
    // For each cell it creates a feature vector from the layers and then generates one prediction for that vector.
    val operator = (rs: Seq[Tile], context: Map[String, Any]) => {
      val modelCheck = context.getOrElse("context", null)
      if (!modelCheck.isInstanceOf[CatBoostClassificationModel] && !modelCheck.isInstanceOf[CatBoostModel])
        throw new IllegalArgumentException(
          s"The 'model' argument should contain a valid Catboost model, but got: $modelCheck.")
      rs.assertEqualDimensions()
      val layerCount = rs.length
      if (layerCount == 0) sys.error(s"No features provided for predict_catboost.")

      val model = context("context").asInstanceOf[CatBoostClassificationModel]
      multibandMapToNewTiles(MultibandTile(rs),ts => {
        val featureArray: Array[Double] = ts.map(_.doubleValue()).toArray
        val numericalFeatures = ml.linalg.Vectors.dense(featureArray)
        val probabilityPrediction = model.predictProbability(numericalFeatures)
        probabilityPrediction.toArray
      })
    }
    // Return our operator in a composed function.
    val storedArgs = contextStack.head
    val inputFunction: Option[OpenEOProcess] = storedArgs.get("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction.get(context)(tiles), context)
    }
    composed
  }

  private def checkMlArguments(arguments: util.Map[String, Object]) = {
    // TODO: Currently we explicitly check for 'model': {'from_parameter': 'context'} as it is never dereferenced.
    // Later this should be dereferenced correctly in fromParameter() so we can access the model via 'arguments'.
    // For now we will pass the model via the Map[String, Any] input of OpenEOProcess.
    val modelArgument = arguments.getOrDefault("model", null)
    if (!modelArgument.isInstanceOf[util.Map[String, Object]])
      throw new IllegalArgumentException(s"The 'model' argument should contain {'from_parameter': 'context'}, but got: $modelArgument.")
    val fromParamArgument = modelArgument.asInstanceOf[util.Map[String, Object]].getOrElse("from_parameter", null)
    if (!fromParamArgument.isInstanceOf[String] || fromParamArgument.asInstanceOf[String] != "context")
      throw new IllegalArgumentException(s"The from_parameter argument in 'model' should refer to 'context', but got: $fromParamArgument.")
  }

  private def firstFunctionIgnoreNoData(tiles:Seq[Tile]) : Seq[Tile] = {
    val tile = MultibandTile(tiles)
    val mutableResult: MutableArrayTile = tiles.head.mutable.prototype(tile.cols, tile.rows)

    def findFirstBandValue(col: Int, row: Int): Any = {
      // Find the first band where the value is not NaN and use that as the value for this (col, row).
      cfor(0)(_ < tile.bandCount, _ + 1) { band =>
        val d = tile.bands(band).getDouble(col, row)
        if (!d.isNaN) {
          mutableResult.setDouble(col, row, d)
          return None; // (col, row) value chosen, break out of band loop.
        }
      }
    }

    cfor(0)(_ < tile.cols, _ + 1) { col =>
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        findFirstBandValue(col, row)
      }
    }
    Seq(mutableResult.convert(tiles.head.cellType));
  }

  private def lastFunctionIgnoreNoData(tiles:Seq[Tile]) : Seq[Tile] = {
    val tile = MultibandTile(tiles)
    val mutableResult: MutableArrayTile = tiles.head.mutable.prototype(tile.cols, tile.rows)

    def findLastBandValue(col: Int, row: Int): Any = {
      // Find the first band where the value is not NaN and use that as the value for this (col, row).
      cfor(tile.bandCount-1)(_ >= 0, _ - 1) { band =>
        val d = tile.bands(band).getDouble(col, row)
        if (!d.isNaN) {
          mutableResult.setDouble(col, row, d)
          return None; // (col, row) value chosen, break out of band loop.
        }
      }
    }

    cfor(0)(_ < tile.cols, _ + 1) { col =>
      cfor(0)(_ < tile.rows, _ + 1) { row =>
        findLastBandValue(col, row)
      }
    }
    Seq(mutableResult.convert(tiles.head.cellType));
  }

  private def firstFunctionWithNodata(tiles:Seq[Tile]) : Seq[Tile] = Seq(tiles.head)
  private def lastFunctionWithNoData(tiles:Seq[Tile]) : Seq[Tile] = Seq(tiles.last)

  private def arrayElementFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("data").get
    val index = arguments.getOrDefault("index",null)
    if(index == null) {
      throw new IllegalArgumentException("Missing 'index' argument in array_element.")
    }
    if(!index.isInstanceOf[Integer]){
      throw new IllegalArgumentException("The 'index' argument should be an integer, but got: " + index)
    }
    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val input: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      if(input.size <= index.asInstanceOf[Integer]) {
        throw new IllegalArgumentException("Invalid band index " + index + ", only " + input.size + " bands available.")
      }
      Seq(input(index.asInstanceOf[Integer]))
    }
    bandFunction
  }

  private def betweenFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("x").get
    val min = arguments.getOrDefault("min",null)
    val max = arguments.getOrDefault("max",null)
    val excludeMax = arguments.getOrDefault("exclude_max",Boolean.box(false))
    if(min == null) {
      throw new IllegalArgumentException("Missing 'min' argument in between.")
    }
    if(max == null) {
      throw new IllegalArgumentException("Missing 'max' argument in between.")
    }

    val minDouble = min.asInstanceOf[Number].doubleValue()
    val maxDouble = max.asInstanceOf[Number].doubleValue()
    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val input: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      input.map(tile => {
        val greater = GreaterOrEqual(tile, minDouble)
        val smallerThanMax = if(excludeMax.asInstanceOf[Boolean]) {
          Less(tile,maxDouble)
        }else{
          LessOrEqual(tile,maxDouble)
        }
        And(greater,smallerThanMax)
      })

    }
    bandFunction
  }

  private def clipFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs("x")
    val min = arguments.get("min").asInstanceOf[Number].doubleValue()
    val max = arguments.get("max").asInstanceOf[Number].doubleValue()

    val doTypeCast = min != min.toInt || max != max.toInt

    val clipFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val input = evaluateToTiles(inputFunction, context, tiles)
      val castedInput =
        if (doTypeCast)
          input.map(_.convert(FloatConstantNoDataCellType))
        else
          input
      castedInput.map(_.mapIfSetDouble(x => {
        if (x < min)
          min
        else if (x > max)
          max
        else
          x
      }))
    }
    clipFunction
  }
}
