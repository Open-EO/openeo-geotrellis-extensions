package org.openeo.geotrellis

import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{DoubleConstantTile, IntConstantTile, ShortConstantTile, Tile, UByteConstantTile}

import scala.collection.mutable

/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

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

  private def xyFunction(operator:(Tile,Tile) => Tile ) = {
    val storedArgs = contextStack.head
    if(!storedArgs.contains("x")){
      throw new IllegalArgumentException("This function expects an 'x' argument, function tree: " + processStack.reverse.mkString("->") + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    if(!storedArgs.contains("y")){
      throw new IllegalArgumentException("This function expects an 'y' argument, function tree: " + processStack.reverse.mkString("->") + ". These arguments were found: " + storedArgs.keys.mkString(", "))
    }
    val x_function = storedArgs.get("x").get
    val y_function = storedArgs.get("y").get
    val bandFunction = (tiles:Seq[Tile]) =>{
      val x_input: Seq[Tile] =
        if(x_function!=null) {
          x_function.apply(tiles)
        }else{
          tiles
        }
      val y_input: Seq[Tile] =
        if(y_function!=null) {
          y_function.apply(tiles)
        }else{
          tiles
        }
      if (x_input.size != 1) {
        throw new IllegalArgumentException("Expected single tile, but got for x:" + x_input.size)
      }
      if (y_input.size != 1) {
        throw new IllegalArgumentException("Expected single tile, but got for y:" + y_input.size)
      }

      Seq(operator(x_input(0),y_input(0)))
    }
    bandFunction
  }

  def constantArgument(name:String,value:Number): Unit = {
    var scope = contextStack.head
    scope.put(name,createConstantTileFunction(value))
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

    val operation: Seq[Tile] => Seq[Tile] = operator match {
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
      case "sum" if hasData => reduceFunction("data", Add.apply)
      case "add" if hasXY => xyFunction(Add.apply)
      case "subtract" if hasXY => xyFunction(Subtract.apply)
      case "subtract" if hasData => reduceFunction("data", Subtract.apply) // legacy 0.4 style
      case "product" if hasData => reduceFunction("data", Multiply.apply)
      case "multiply" if hasXY => xyFunction(Multiply.apply)
      case "multiply" if hasData => reduceFunction("data", Multiply.apply) // legacy 0.4 style
      case "divide" if hasXY => xyFunction(Divide.apply)
      case "divide" if hasData => reduceFunction("data", Divide.apply) // legacy 0.4 style
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
