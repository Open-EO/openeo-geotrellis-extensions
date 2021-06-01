package org.openeo.geotrellis

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{ArrayTile, DoubleConstantTile, FloatConstantTile, IntConstantTile, MultibandTile, MutableArrayTile, NODATA, ShortConstantTile, Tile, UByteConstantTile, isNoData}
import org.openeo.geotrellis.mapalgebra.{AddIgnoreNodata, LogBase}
import org.slf4j.LoggerFactory
import spire.syntax.cfor.cfor

import scala.Double.NaN
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.{immutable, mutable}

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

  private def multibandMap(tile: MultibandTile ,f: Array[Double] => Array[Double]): Seq[Tile] = {
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
}
/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

  import OpenEOProcessScriptBuilder._

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


  val processStack: mutable.Stack[String] = new mutable.Stack[String]()
  val arrayElementStack: mutable.Stack[Integer] = new mutable.Stack[Integer]()
  val argNames: mutable.Stack[String] = new mutable.Stack[String]()
  val contextStack: mutable.Stack[mutable.Map[String,OpenEOProcess]] = new mutable.Stack[mutable.Map[String, OpenEOProcess]]()
  var arrayCounter : Int =  0
  var inputFunction:  OpenEOProcess = null

  def generateFunction(context: Map[String,Any] = Map.empty): Seq[Tile] => Seq[Tile] = {
    inputFunction(context)
  }

  def generateFunction(): Seq[Tile] => Seq[Tile] = {
    wrapProcessWithDefaultContext(inputFunction)
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

  private def reduceListFunction(argName: String, operator: Seq[Tile] => Tile): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(operator(tiles)))
  }

  private def ifProcess(arguments:java.util.Map[String,Object]): OpenEOProcess ={
    val storedArgs = contextStack.head
    val value = storedArgs.get("value").getOrElse(throw new IllegalArgumentException("If process expects a value argument. These arguments were found: " + arguments.keys.mkString(", ")))
    val accept = storedArgs.get("accept").getOrElse(throw new IllegalArgumentException("If process expects an accept argument. These arguments were found: " + arguments.keys.mkString(", ")))
    val reject: OpenEOProcess = storedArgs.get("reject").getOrElse(null)
    val ifElseProcess = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val value_input: Seq[Tile] =
        if (value != null) {
          value.apply(context)(tiles)
        } else {
          tiles
        }

      def makeSameLength(tiles: Seq[Tile]): Seq[Tile] ={
        if(tiles.size == 1 && value_input.length >1) {
          Seq.fill(value_input.length)(tiles(0))
        }else{
          tiles
        }
      }

      val accept_input: Seq[Tile] =
        if (accept != null) {
          makeSameLength(accept.apply(context)(tiles))
        } else {
          makeSameLength(tiles)
        }


      val reject_input: Seq[Tile] =
        if (reject != null) {
          reject.apply(context)(tiles)
        } else {
          logger.debug("If process without reject clause.")
          Seq.fill(accept_input.length)(null)
        }

      def ifElse(value:Tile, acceptTile:Tile, rejectTile: Tile): Tile = {
        val outputCellType = if(rejectTile==null) acceptTile.cellType else acceptTile.cellType.union(rejectTile.cellType)
        val resultTile = ArrayTile.empty(outputCellType,acceptTile.cols,acceptTile.rows)

        def setResult(col:Int,row:Int,fromTile:Tile): Unit ={
          if(fromTile==null) {
            if(outputCellType.isFloatingPoint) resultTile.setDouble(col,row,Double.NaN) else resultTile.set(col,row,NODATA)
          }else{
            if(outputCellType.isFloatingPoint) resultTile.setDouble(col,row,fromTile.getDouble(col,row)) else resultTile.set(col,row,fromTile.get(col,row))
          }
        }
        value.foreach{ (col,row,value) => {
          if(value==0){
            //reject
            setResult(col,row,rejectTile)
          }else{
            //accept
            setResult(col,row,acceptTile)
          }
        }}
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

  private def xyFunction(operator:(Tile,Tile) => Tile,xArgName:String = "x",yArgName:String = "y" ): OpenEOProcess = {
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
      val x_input: Seq[Tile] =
        if (x_function != null) {
          x_function.apply(context)(tiles)
        } else {
          tiles
        }
      val y_input: Seq[Tile] =
        if (y_function != null) {
          y_function.apply(context)(tiles)
        } else {
          tiles
        }
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

    val operation: OpenEOProcess = operator match {
      case "if" => ifProcess(arguments)
      // Comparison operators
      case "gt" if hasXY => xyFunction(Greater.apply)
      case "lt" if hasXY => xyFunction(Less.apply)
      case "gte" if hasXY => xyFunction(GreaterOrEqual.apply)
      case "lte" if hasXY => xyFunction(LessOrEqual.apply)
      case "eq" if hasXY => xyFunction(Equal.apply)
      case "neq" if hasXY => xyFunction(Unequal.apply)
      // Boolean operators
      case "not" if hasX => mapFunction("x", Not.apply)
      case "not" if hasExpression => mapFunction("expression", Not.apply) // legacy 0.4 style
      case "and" if hasXY => xyFunction(And.apply)
      case "and" if hasExpressions => reduceFunction("expressions", And.apply) // legacy 0.4 style
      case "or" if hasXY => xyFunction(Or.apply)
      case "or" if hasExpressions => reduceFunction("expressions", Or.apply) // legacy 0.4 style
      case "xor" if hasXY => xyFunction(Xor.apply)
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
      // Statistics
      case "max" if hasData && !ignoreNoData => reduceFunction("data", Max.apply)
      case "max" if hasData && ignoreNoData => reduceFunction("data", MaxIgnoreNoData.apply)
      case "min" if hasData && !ignoreNoData => reduceFunction("data", Min.apply)
      case "min" if hasData && ignoreNoData => reduceFunction("data", MinIgnoreNoData.apply)
        //TODO take ignorenodata into account!
      case "mean" if hasData => reduceListFunction("data", Mean.apply)
      case "variance" if hasData => reduceListFunction("data", Variance.apply)
      case "sd" if hasData => reduceListFunction("data", Sqrt.apply _ compose Variance.apply)
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
      case "array_element" => arrayElementFunction(arguments)
      case "array_modify" => arrayModifyFunction(arguments)
      case "array_interpolate_linear" => applyListFunction("data",linearInterpolation)
      case _ => throw new IllegalArgumentException(s"Unsupported operation: $operator (arguments: ${arguments.keySet()})")
    }

    val expectedOperator = processStack.pop()
    assert(expectedOperator.equals(operator))
    contextStack.pop()
    inputFunction = operation
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
      val data: Seq[Tile] =
        if(inputFunction!=null) {
          inputFunction.apply(context)(tiles)
        }else{
          tiles
        }
      val values: Seq[Tile] =
        if(valuesFunction!=null) {
          valuesFunction.apply(context)(tiles)
        }else{
          tiles
        }
      if(length == null) {
        //in this case, we need to insert
        data.take(index.asInstanceOf[Integer]) ++ values ++ data.drop(index.asInstanceOf[Integer])
      }else{
        throw new UnsupportedOperationException("Geotrellis backend only supports inserting in array-modify")
      }

    }
    bandFunction
  }


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
      val input: Seq[Tile] =
        if(inputFunction!=null) {
          inputFunction.apply(context)(tiles)
        }else{
          tiles
        }
      if(input.size <= index.asInstanceOf[Integer]) {
        throw new IllegalArgumentException("Invalid band index, only " + input.size + " bands available.")
      }
      Seq(input(index.asInstanceOf[Integer]))
    }
    bandFunction
  }


}
