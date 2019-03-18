package org.openeo.geotrellis

import geotrellis.raster.{DoubleConstantTile, IntConstantTile, Tile}

import scala.collection.mutable

/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

  val processStack: mutable.Stack[String] = new mutable.Stack[String]()
  val argNames: mutable.Stack[String] = new mutable.Stack[String]()
  val contextStack: mutable.Stack[mutable.Map[String,Seq[Tile] => Seq[Tile]]] = new mutable.Stack[mutable.Map[String, Seq[Tile] => Seq[Tile]]]()
  var arrayCounter : Int =  0
  var inputFunction: Seq[Tile] => Seq[Tile] = null

  private def unaryFunction(argName:String,operator:Seq[Tile] => Seq[Tile] ) = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get(argName).get

    if(inputFunction !=null)
      operator compose inputFunction
    else
      operator

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
    * @param index
    */
  def arrayStart(name:String): Unit = {
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

  def constantArrayElement(value: Number):Unit = {
    val constantTileFunction:Seq[Tile] => Seq[Tile] = (tiles:Seq[Tile]) => {
      if(tiles.isEmpty) {
        tiles
      }else{
        val rows= tiles.head.rows
        val cols = tiles.head.cols
        value match {
          case x: Integer => Seq(IntConstantTile(value.intValue(),cols,rows))
          case _ => Seq(DoubleConstantTile(value.doubleValue(),cols,rows))
        }
      }

    }
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
    arrayCounter = 0

    contextStack.head.put(name,inputFunction)

  }


  def expressionStart(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    processStack.push(operator)
    contextStack.push(mutable.Map[String,Seq[Tile] => Seq[Tile]]())
  }

  def expressionEnd(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    val expectedOperator = processStack.pop()
    assert(expectedOperator.equals(operator))
    val storedArgs = contextStack.head

    val operation = operator match {
      case "sum" => unaryFunction("data", (tiles:Seq[Tile]) =>{
          Seq(tiles.reduce( _.localAdd(_)))
        })
      case "divide" => unaryFunction("data", (tiles:Seq[Tile]) =>{
        Seq(tiles.reduce( _.localDivide(_)))
      })
      case "product" => unaryFunction("data", (tiles:Seq[Tile]) =>{
        Seq(tiles.reduce( _.localMultiply(_)))
      })
      case "subtract" => unaryFunction("data", (tiles:Seq[Tile]) =>{
        Seq(tiles.reduce( _.localSubtract(_)))
      })
      case "array_element" =>{
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
      case _ => throw new IllegalArgumentException("Unsupported operation: " + operator)

    }

    contextStack.pop()
    inputFunction = operation


  }


  def generateFunction() = inputFunction



}
