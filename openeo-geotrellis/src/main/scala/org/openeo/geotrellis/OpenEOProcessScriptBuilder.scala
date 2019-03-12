package org.openeo.geotrellis

import geotrellis.raster.Tile

import scala.collection.mutable

/**
  * Builder to help converting an OpenEO process graph into a transformation of Geotrellis tiles.
  */
class OpenEOProcessScriptBuilder {

  val processStack: mutable.Stack[String] = new mutable.Stack[String]()
  val argNames: mutable.Stack[String] = new mutable.Stack[String]()
  val contextStack: mutable.Stack[mutable.Map[String,Seq[Tile] => Seq[Tile]]] = new mutable.Stack[mutable.Map[String, Seq[Tile] => Seq[Tile]]]()

  var inputFunction: Seq[Tile] => Seq[Tile] = null

  private def unaryFunction(argName:String,operator:Seq[Tile] => Seq[Tile] ) = {
    val storedArgs = contextStack.head
    val inputFunction = storedArgs.get(argName).get

    if(inputFunction !=null)
      inputFunction compose operator
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
  }
  def expressionStart(operator:String,arguments:Seq[String]): Unit = {
    processStack.push(operator)
    contextStack.push(mutable.Map[String,Seq[Tile] => Seq[Tile]]())
  }

  def expressionEnd(operator:String,arguments:Seq[String]): Unit = {
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
      case _ => throw new IllegalArgumentException("Unsupported operation: " + operator)

    }

    contextStack.pop()
    inputFunction = operation


  }


  def generateFunction() = inputFunction

}
