package org.openeo.geotrellis

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{DoubleConstantTile, IntConstantTile, NODATA, ShortConstantTile, Tile, UByteConstantTile, d2i, isNoData}
import org.openeo.geotrellis.mapalgebra.{AddIgnoreNodata, LogBase}

import scala.Double.NaN
import scala.collection.mutable

/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

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
  val contextStack: mutable.Stack[mutable.Map[String,Seq[Tile] => Seq[Tile]]] = new mutable.Stack[mutable.Map[String, Seq[Tile] => Seq[Tile]]]()
  var arrayCounter : Int =  0
  var inputFunction: Seq[Tile] => Seq[Tile] = null

  def generateFunction(): Seq[Tile] => Seq[Tile] = inputFunction

  private def unaryFunction(argName: String, operator: Seq[Tile] => Seq[Tile]): Seq[Tile] => Seq[Tile] = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get(argName)

    if(inputFunction.isDefined && inputFunction.get != null)
      operator compose inputFunction.get
    else
      operator
  }

  private def mapFunction(argName: String, operator: Tile => Tile): Seq[Tile] => Seq[Tile] = {
    unaryFunction(argName, (tiles: Seq[Tile]) => tiles.map(operator))
  }

  private def reduceFunction(argName: String, operator: (Tile, Tile) => Tile): Seq[Tile] => Seq[Tile] = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(tiles.reduce(operator)))
  }

  private def reduceListFunction(argName: String, operator: Seq[Tile] => Tile): Seq[Tile] => Seq[Tile] = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(operator(tiles)))
  }

  private def ifProcess() ={
    val storedArgs = contextStack.head
    val value = storedArgs.get("value").get
    val accept = storedArgs.get("accept").get
    val reject: Seq[Tile] => Seq[Tile] = storedArgs.get("reject").getOrElse(null)
    val ifElseProcess = (tiles: Seq[Tile]) => {
      val value_input: Seq[Tile] =
        if (value != null) {
          value.apply(tiles)
        } else {
          tiles
        }
      val accept_input: Seq[Tile] =
        if (accept != null) {
          accept.apply(tiles)
        } else {
          tiles
        }

      val reject_input: Seq[Tile] =
        if (reject != null) {
          reject.apply(tiles)
        } else {
          tiles
        }

      def ifElse(value:Tile,accept:Tile,reject: Tile): Tile ={
        val tile = value.dualCombine(accept) { (z1,z2) =>
          if (z1 !=0) z2 else NODATA
        }
        { (z1,z2) =>if (d2i(z1)!=0) z2 else Double.NaN}

        val tileWithRejects = value.dualCombine(reject){ (z1,z2) => if (z1==0) z2 else NODATA }
        { (z1,z2) => if (d2i(z1)==0) z2 else Double.NaN }
        tile.merge(tileWithRejects)
      }


      if(value_input.size == accept_input.size) {
        value_input.zip(accept_input).zip(reject_input).map{ t => ifElse(t._1._1,t._1._2,t._2)}
      }else if(value_input.size == 1 ) {
        accept_input.zip(reject_input).map{ t => ifElse(value_input.head,t._1,t._2)}
      }
      else{
        throw new IllegalArgumentException("Incompatible numbers of tiles in this if process.")
      }

    }
    ifElseProcess
  }

  private def xyFunction(operator:(Tile,Tile) => Tile,xArgName:String = "x",yArgName:String = "y" ) = {
    val storedArgs = contextStack.head
    val processString = processStack.reverse.mkString("->")
    if (!storedArgs.contains(xArgName)) {
      throw new IllegalArgumentException("This function expects an '" + xArgName + "' argument, function tree: " + processString + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    if (!storedArgs.contains(yArgName)) {
      throw new IllegalArgumentException("This function expects an '" + yArgName + "' argument, function tree: " + processString + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    val x_function = storedArgs.get(xArgName).get
    val y_function = storedArgs.get(yArgName).get
    val bandFunction = (tiles: Seq[Tile]) => {
      val x_input: Seq[Tile] =
        if (x_function != null) {
          x_function.apply(tiles)
        } else {
          tiles
        }
      val y_input: Seq[Tile] =
        if (y_function != null) {
          y_function.apply(tiles)
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
    scope.put(name,createConstantTileFunction(value))
  }

  def constantArgument(name:String,value:Boolean): Unit = {
    //can be skipped, will simply be available when executing function
  }

  def argumentStart(name:String): Unit = {
    argNames.push(name)
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
    contextStack.push(mutable.Map[String,Seq[Tile] => Seq[Tile]]())
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
          case _ => Seq(DoubleConstantTile(value.doubleValue(),cols,rows))
        }
      }

    }
    return constantTileFunction
  }

  def constantArrayElement(value: Number):Unit = {
    val constantTileFunction:Seq[Tile] => Seq[Tile] = createConstantTileFunction(value)
    val scope = contextStack.head
    scope.put(arrayCounter.toString,constantTileFunction)
    arrayCounter += 1
  }

  def arrayEnd():Unit = {
    val name = argNames.pop()
    val scope = contextStack.pop()
    processStack.pop()

    val nbElements = arrayCounter
    inputFunction = (tiles:Seq[Tile]) => {
      var results = Seq[Tile]()
      for( i <- 0 until nbElements) {
        val tileFunction = scope.get(i.toString).get
        results = results ++ tileFunction(tiles)
      }
      results
    }
    arrayCounter = arrayElementStack.pop()

    contextStack.head.put(name,inputFunction)

  }


  def expressionStart(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    processStack.push(operator)
    contextStack.push(mutable.Map[String,Seq[Tile] => Seq[Tile]]())
  }

  def expressionEnd(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    // TODO: this is not only about expressions anymore. Rename it to e.g. "leaveProcess" to be more in line with graph visitor in Python?

    // Bit of argument sniffing to support multiple versions/variants of processes
    val hasXY = arguments.containsKey("x") && arguments.containsKey("y")
    val hasX = arguments.containsKey("x")
    val hasExpression = arguments.containsKey("expression")
    val hasExpressions = arguments.containsKey("expressions")
    val hasData = arguments.containsKey("data")
    val ignoreNoData = !(arguments.getOrDefault("ignore_nodata",Boolean.box(true).asInstanceOf[Object]) == Boolean.box(false) || arguments.getOrDefault("ignore_nodata",None) == "false" )

    val operation: Seq[Tile] => Seq[Tile] = operator match {
      case "if" => ifProcess()
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
      // Mathematical operators
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
        //statistics
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
      case _ => throw new IllegalArgumentException(s"Unsupported operation: $operator (arguments: ${arguments.keySet()})")
    }

    val expectedOperator = processStack.pop()
    assert(expectedOperator.equals(operator))
    contextStack.pop()
    inputFunction = operation
  }


  private def arrayElementFunction(arguments:java.util.Map[String,Object]) = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get("data").get
    val index = arguments.getOrDefault("index",null)
    if(index == null) {
      throw new IllegalArgumentException("Missing 'index' argument in array_element.")
    }
    if(!index.isInstanceOf[Integer]){
      throw new IllegalArgumentException("The 'index argument should be an integer, but got: " + index)
    }
    val bandFunction = (tiles:Seq[Tile]) =>{
      val input: Seq[Tile] =
        if(inputFunction!=null) {
          inputFunction.apply(tiles)
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
