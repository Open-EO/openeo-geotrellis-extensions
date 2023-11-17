package org.openeo.geotrellis

import ai.catboost.CatBoostModel
import ai.catboost.spark.CatBoostClassificationModel
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.{ArrayTile, BitCellType, ByteUserDefinedNoDataCellType, CellType, Dimensions, DoubleConstantTile, FloatConstantNoDataCellType, FloatConstantTile, IntConstantNoDataCellType, IntConstantTile, MultibandTile, MutableArrayTile, NODATA, ShortConstantNoDataCellType, ShortConstantTile, Tile, UByteConstantTile, UByteUserDefinedNoDataCellType, UShortUserDefinedNoDataCellType, isData, isNoData}
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

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAccessor}
import java.time.{Duration, ZonedDateTime}
import java.util
import scala.Double.NaN
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{immutable, mutable}
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

object Exp extends Serializable {
  /** Take the square root each value in a raster. */
  def apply(r: Tile) =
    r.dualMap { z: Int => if(isNoData(z) || z < 0) NODATA else math.exp(z).toInt }
    { z: Double =>math.exp(z) }
}

object OpenEOProcessScriptBuilder{

  private val logger = LoggerFactory.getLogger(classOf[OpenEOProcessScriptBuilder])

  //operators that return a boolean
  private val booleanOperators = Set("or", "and", "eq", "neq", "date_between")

  type OpenEOProcess =  Map[String,Any] => (Seq[Tile]  => Seq[Tile] )
  type AnyProcess =  Map[String,Any] => (Any  => Any )

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

  private def varianceWithNoData(rs: Seq[Tile]): Tile = {
    rs.assertEqualDimensions()

    val layerCount = rs.length
    if (layerCount == 0) sys.error(s"Can't compute variance of empty sequence.")
    else {
      val newCellType = rs.map(_.cellType).reduce(_.union(_))
      val Dimensions(cols, rows) = rs(0).dimensions
      val tile = ArrayTile.alloc(newCellType, cols, rows)

      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < cols, _ + 1) { col =>
          var count = 0
          var mean = 0.0
          var m2 = 0.0
          var noDataPixel = false

          breakable {
            cfor(0)(_ < layerCount, _ + 1) { i =>
              val v = rs(i).getDouble(col, row)
              if (isData(v)) {
                count += 1
                val delta = v - mean
                mean += delta / count
                m2 += delta * (v - mean)
              } else {
                noDataPixel = true
                break
              }
            }
          }

          if (newCellType.isFloatingPoint) {
            if (count > 1 && !noDataPixel) tile.setDouble(col, row, m2 / (count - 1))
            else tile.setDouble(col, row, Double.NaN)
          } else {
            if (count > 1 && !noDataPixel) tile.set(col, row, (m2 / (count - 1)).round.toInt)
            else tile.set(col, row, NODATA)
          }
        }
      }

      tile
    }
  }


  private def firstFunctionIgnoreNoData(tiles: Seq[Tile]): Seq[Tile] = {
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

  private def lastFunctionIgnoreNoData(tiles: Seq[Tile]): Seq[Tile] = {
    val tile = MultibandTile(tiles)
    val mutableResult: MutableArrayTile = tiles.head.mutable.prototype(tile.cols, tile.rows)

    def findLastBandValue(col: Int, row: Int): Any = {
      // Find the first band where the value is not NaN and use that as the value for this (col, row).
      cfor(tile.bandCount - 1)(_ >= 0, _ - 1) { band =>
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

  private def firstFunctionWithNodata(tiles: Seq[Tile]): Seq[Tile] = Seq(tiles.head)

  private def lastFunctionWithNoData(tiles: Seq[Tile]): Seq[Tile] = Seq(tiles.last)

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


  private def unifyCellType(combined: Seq[Tile]) = {
    if (combined.nonEmpty) {
      val unionCelltype = combined.map(_.cellType).reduce(_.union(_))
      combined.map(_.convert(unionCelltype))
    } else {
      combined
    }
  }

  def argumentToDate(argument: Any, context: Map[String, Any],process:String="unknown"): String = {
    argument match {
      case str: String =>
        str
      case aTimeClass: TemporalAccessor =>
        DateTimeFormatter.ISO_INSTANT.format(aTimeClass)
      case objectHash: util.Map[String, Any] if objectHash.containsKey("from_parameter") =>
        val paramName = objectHash.get("from_parameter").asInstanceOf[String]
        val theParamValue = context.getOrElse(paramName, throw new IllegalArgumentException(s"$process: Parameter $paramName not found in context: $context"))
        theParamValue match {
          case accessor: TemporalAccessor =>
            DateTimeFormatter.ISO_INSTANT.format(accessor)
          case _ =>
            theParamValue.asInstanceOf[String]
        }
      case _ =>
        throw new IllegalArgumentException(s"$process got unexpected argument: $argument")
    }
  }

  private def createConstantTileFunction(value: Number): Seq[Tile] => Seq[Tile] = {
    val constantTileFunction: Seq[Tile] => Seq[Tile] = (tiles: Seq[Tile]) => {
      if (tiles.isEmpty) {
        tiles
      } else {
        val rows = tiles.head.rows
        val cols = tiles.head.cols
        value match {
          case x: java.lang.Byte => Seq(UByteConstantTile(value.byteValue(), cols, rows))
          case x: java.lang.Short => Seq(ShortConstantTile(value.byteValue(), cols, rows))
          case x: Integer => Seq(IntConstantTile(value.intValue(), cols, rows))
          case x: java.lang.Float => Seq(FloatConstantTile(value.floatValue(), cols, rows))
          case _ => Seq(DoubleConstantTile(value.doubleValue(), cols, rows))
        }
      }

    }
    return constantTileFunction
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
  val contextStack: mutable.Stack[mutable.Map[String,Object]] = new mutable.Stack[mutable.Map[String, Object]]()
  val typeStack: mutable.Stack[mutable.Map[String,String]] = new mutable.Stack[mutable.Map[String, String]]()
  var arrayCounter : Int =  0
  var inputFunction:  Object = null

  var resultingDataType: CellType = FloatConstantNoDataCellType
  val defaultDataParameterName:String = "data"

  def generateFunction(context: Map[String,Any] = Map.empty): Seq[Tile] => Seq[Tile] = {
    if(inputFunction.isInstanceOf[OpenEOProcess]) {
      inputFunction.asInstanceOf[OpenEOProcess](context)
    }else{
      throw new IllegalArgumentException(s"The openEO callback resulted into an unsupported function: $inputFunction")
    }
  }

  def generateFunction(context: util.Map[String, Any]): Seq[Tile] => Seq[Tile] = {
    this.generateFunction(context.toMap)
  }

  def generateAnyFunction(context: util.Map[String, Any]): Any => Any = {
    this.inputFunction.asInstanceOf[AnyProcess](context.toMap)
  }

  def generateFunction(): Seq[Tile] => Seq[Tile] = {
    wrapProcessWithDefaultContext(inputFunction.asInstanceOf[OpenEOProcess])
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
    val inputFunction: Option[OpenEOProcess] = storedArgs.get(argName).asInstanceOf[Option[OpenEOProcess]]
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

  private def getProcessArg(name:String):OpenEOProcess = {
    contextStack.head.getOrElse(name,throw new IllegalArgumentException(s"Process [${processStack.head}] expects a $name argument. These arguments were found: " + contextStack.head.keys.mkString(", ") + s"function tree: ${processStack.reverse.mkString("->")}")).asInstanceOf[OpenEOProcess]
  }

  private def getAnyProcessArg(name: String,arguments: java.util.Map[String, Object],default:Any = null): AnyProcess = {
    if(arguments.get(name).isInstanceOf[String] || arguments.get(name).isInstanceOf[Boolean]) {
      val theArg:Any = arguments.get(name)
      (context: Map[String,Any]) => (inputArg: Any) => {
        theArg
      }
    }else if(default!=null && !contextStack.head.contains(name)) {
      (context: Map[String, Any]) =>
        (inputArg: Any) => {
          default
        }
    }
    else{

      contextStack.head.getOrElse(name, throw new IllegalArgumentException(s"Process [${processStack.head}] expects a $name argument. These arguments were found: " + contextStack.head.keys.mkString(", ") + s"function tree: ${processStack.reverse.mkString("->")}")).asInstanceOf[AnyProcess]
    }
  }

  private def optionalArg(name: String): OpenEOProcess = {
    contextStack.head.getOrElse(name, null).asInstanceOf[OpenEOProcess]
  }


  private def arrayFind(arguments:java.util.Map[String,Object]) : OpenEOProcess = {
    val storedArgs = contextStack.head
    val value = getProcessArg("value")
    val data = getProcessArg("data")

    val reverse = (arguments.getOrDefault("reverse",Boolean.box(false).asInstanceOf[Object]) == Boolean.box(true) || arguments.getOrDefault("reverse",None) == "true" )

    val arrayfindProcess = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val value_input: Seq[Tile] = evaluateToTiles(value, context, tiles)
      val data_input: Seq[Tile] = evaluateToTiles(data, context, tiles)
      if(value_input.size!=1)
        throw new IllegalArgumentException("The value argument of the array_find function should resolve to exactly one input.")
      val the_value = value_input.head
      val tile = MultibandTile(data_input)
      val mutableResult:MutableArrayTile = ArrayTile.empty(tile.cellType,tile.cols,tile.rows)
      var i = 0
      cfor(0)(_ < tile.cols, _ + 1) { col =>
        cfor(0)(_ < tile.rows, _ + 1) { row =>
          val bandValues = new ArrayBuffer[Double](tile.bandCount)
          cfor(0)(_ < tile.bandCount, _ + 1) { band =>
            val d = tile.bands(band).getDouble(col, row)
            bandValues.append(d)
          }
          if(!bandValues.isEmpty) {
            val resultValues = if(!reverse) bandValues.indexOf(the_value.getDouble(col,row)) else bandValues.lastIndexOf(the_value.getDouble(col,row))
            if(resultValues>=0)
              mutableResult.setDouble(col, row,resultValues)
          }
          i += 1
        }
      }
      Seq(mutableResult)

    }

    arrayfindProcess
  }

  private def mapListFunction(listArgName: String, mapArgName:String, operator: Seq[Tile] => Seq[Tile]): OpenEOProcess = {
    val storedArgs = contextStack.head
    val mapFunction: Option[OpenEOProcess] = storedArgs.get(mapArgName).asInstanceOf[Option[OpenEOProcess]]
    val listFunction: Option[OpenEOProcess] = storedArgs.get(listArgName).asInstanceOf[Option[OpenEOProcess]]

    (context: Map[String,Any]) => (tiles: Seq[Tile]) => {
      val mapTiles = if (mapFunction.isDefined) mapFunction.get(context)(tiles) else tiles
      composeFunctions((tiles: Seq[Tile]) => operator(tiles), listFunction)(context)(mapTiles)
    }
  }

  private def reduceListFunction(argName: String, operator: Seq[Tile] => Tile): OpenEOProcess = {
    unaryFunction(argName, (tiles: Seq[Tile]) => Seq(operator(tiles)))
  }

  private def ifProcess(arguments:java.util.Map[String,Object]): OpenEOProcess ={
    val value = getProcessArg("value")
    val accept = getProcessArg("accept")
    val reject: OpenEOProcess = optionalArg("reject")
    ifElseProcess(value, accept, reject)
  }

  private def dateShift(arguments: java.util.Map[String, Object]): AnyProcess = {
    val date = arguments.get("date")
    //TODO full evaluation of integer arguments
    val theValue = arguments.get("value")
    val unit = arguments.get("unit")

    if (!theValue.isInstanceOf[Integer]) {
      throw new IllegalArgumentException("date_shift: The 'value' argument should be an integer, but got: " + theValue)
    }
    val dateProcess = (context: Map[String, Any]) => {
      val parsedDate = ZonedDateTime.parse(argumentToDate(date, context, "date_shift"))
      val theFunction = (arg: Any) => {
        parsedDate.plus(theValue.asInstanceOf[Integer].longValue(),ChronoUnit.valueOf(unit.asInstanceOf[String].replace("millisecond","MILLI").toUpperCase + "S"))
      }
      theFunction
    }
    return dateProcess
  }

  private def dateReplaceComponent(arguments: java.util.Map[String, Object]): AnyProcess = {
    val date = arguments.get("date")
    val value = arguments.get("value")
    val component = arguments.get("component")
    if (!value.isInstanceOf[Integer]) {
      throw new IllegalArgumentException("date_replace_component: The 'value' argument should be an integer, but got: " + value)
    }

    val dateProcess = (context: Map[String, Any]) => {
      val parsedDate = ZonedDateTime.parse(argumentToDate(date,context,"date_replace_component"))
      val theFunction = (arg:Any) => {
        if ("second" == component) {
          parsedDate.withSecond(value.asInstanceOf[Integer]).toString
        }
        else if ("minute" == component) {
          parsedDate.withMinute(value.asInstanceOf[Integer]).toString
        }
        else if ("hour" == component) {
          parsedDate.withHour(value.asInstanceOf[Integer]).toString
        }
        else if("day" == component) {
          parsedDate.withDayOfMonth(value.asInstanceOf[Integer]).toString
        } else if ("month" == component) {
          parsedDate.withMonth(value.asInstanceOf[Integer]).toString
        } else if ("year" == component) {
          parsedDate.withYear(value.asInstanceOf[Integer]).toString
        }else{
          throw new IllegalArgumentException(s"date_replace_component: unsupported component $component")
        }
      }
      theFunction
    }
    return dateProcess
  }

  private def dateDifferenceProcess(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val date1: AnyProcess = getAnyProcessArg("date1",arguments)
    val date2 = getAnyProcessArg("date2",arguments)
    val unit = getAnyProcessArg("unit",arguments,"second")

    val dateDiffProcess = (context: Map[String, Any]) => {
      val date1Evaluated = date1(context)(null)
      val date2Evaluated = date2(context)(null)
      val parsedDate1 = ZonedDateTime.parse(argumentToDate(date1Evaluated, context, "date_difference"))
      val parsedDate2 = ZonedDateTime.parse(argumentToDate(date2Evaluated, context, "date_difference"))
      val duration = Duration.between(parsedDate1, parsedDate2)
      val unitEvaluated = unit(context)(null)
      var diff:Number =
        unitEvaluated match {
          case "year" => duration.toDays/365.0//the spec is not exact on how the fractional part is to be computed
          case "month" => ChronoUnit.MONTHS.between(parsedDate1,parsedDate2)//the spec is not exact on how the fractional part is to be computed
          case "day" => duration.toHours/24.0
          case "hour" => duration.toMinutes/60.0
          case "second" => duration.getSeconds
          case _ => throw new IllegalArgumentException(s"date_difference: unsupported unit $unit")
        }

      if(diff.floatValue().isValidInt){
        diff = diff.intValue()
      }

      createConstantTileFunction(diff)
    }
    dateDiffProcess
  }


  private def dateBetweenProcess(arguments: java.util.Map[String, Object]): AnyProcess = {
    val xDate: AnyProcess = getAnyProcessArg("x", arguments)
    val minDate: AnyProcess = getAnyProcessArg("min", arguments)
    val maxDate = getAnyProcessArg("max", arguments)
    val excludeMax = getAnyProcessArg("exclude_max", arguments, false)

    val dateBetweenProcess = (context: Map[String, Any]) => {

      val theFunction = (arg:Any) => {
        val date1Evaluated = minDate(context)(null)
        val date2Evaluated = maxDate(context)(null)
        val xEvaluated = xDate(context)(null)
        val excludeEvaluated = excludeMax(context)(null).asInstanceOf[Boolean]
        val parsedDate1 = ZonedDateTime.parse(argumentToDate(date1Evaluated, context, "date_between"))
        val parsedDate2 = ZonedDateTime.parse(argumentToDate(date2Evaluated, context, "date_between"))
        val parsedX = ZonedDateTime.parse(argumentToDate(xEvaluated, context, "date_between"))
        parsedX.isEqual(parsedDate1) || (parsedX.isAfter(parsedDate1)&&parsedX.isBefore(parsedDate2)) || (!excludeEvaluated && parsedX.isEqual(parsedDate2))
      }

      theFunction
    }
    dateBetweenProcess
  }

  private def xyConstantFunction(process:String, arguments: java.util.Map[String, Object]): AnyProcess = {
    val xVal: AnyProcess = getAnyProcessArg("x", arguments)
    val yVal: AnyProcess = getAnyProcessArg("y", arguments)


    val xyProcess = (context: Map[String, Any]) => {

      val theFunction = (arg: Any) => {
        val xEvaluated = xVal(context)(null)
        val yEvaluated = yVal(context)(null)
        process match {
          case "eq" => xEvaluated == yEvaluated
          case "neq" => xEvaluated != yEvaluated
          case "or" => xEvaluated.asInstanceOf[Boolean] || yEvaluated.asInstanceOf[Boolean]
          case "and" => xEvaluated.asInstanceOf[Boolean] && yEvaluated.asInstanceOf[Boolean]
          case _ => throw new IllegalArgumentException(s"Unsupported operation: $process (arguments: ${arguments.keySet()})")
        }
      }

      theFunction
    }
    xyProcess
  }


  private def xyFunction(operator:(Tile,Tile) => Tile, xArgName:String = "x", yArgName:String = "y" ,convertBitCells: Boolean = true): OpenEOProcess = {
    val x_function: OpenEOProcess = getProcessArg(xArgName)
    val y_function: OpenEOProcess = getProcessArg(yArgName)
    val processString = processStack.reverse.mkString("->")
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
        throw new IllegalArgumentException(s"Incompatible numbers of tiles in this XY operation '${processString}' $xArgName has: ${x_input.size} , $yArgName has: ${y_input.size}\n We expect either equal counts, are one of them should be 1.")
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
    val defaultName = defaultDataParameterName
    inputFunction = (context:Map[String,Any]) => (tiles: Seq[Tile]) => {
      if(context.contains(parameterName)) {
        if(context(parameterName).isInstanceOf[Seq[Tile]]) {
          context.getOrElse(parameterName,tiles).asInstanceOf[Seq[Tile]]
        }
        else{
          context(parameterName)
        }
      }else if(parameterName == defaultName) {
        tiles
      }
      else{
        logger.debug(s"Parameter with name: $parameterName not found. Available parameters: ${context.keys.mkString(",")} or $defaultName")
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
    contextStack.push(mutable.Map[String,Object]())
    typeStack.head.put(name,"array")
    typeStack.push(mutable.Map[String,String]())
    processStack.push("array")
    arrayCounter = 0
  }

  def arrayElementDone():Unit = {
    val scope = contextStack.head
    scope.put(arrayCounter.toString,inputFunction)
    typeStack.head.put(arrayCounter.toString,resultingDataType.toString())
    arrayCounter += 1
    inputFunction = null
  }



  def constantArrayElement(value: Number):Unit = {
    val constantTileFunction:OpenEOProcess = wrapSimpleProcess(createConstantTileFunction(value))
    val scope = contextStack.head
    scope.put(arrayCounter.toString,constantTileFunction)
    typeStack.head.put(arrayCounter.toString,value.getClass.toString)
    arrayCounter += 1
  }

  def arrayEnd():Unit = {
    val name = argNames.pop()
    typeStack.pop()
    val scope = contextStack.pop()
    processStack.pop()

    val nbElements = arrayCounter
    inputFunction = (context:Map[String,Any]) => (tiles:Seq[Tile]) => {
      var results = Seq[Tile]()
      for( i <- 0 until nbElements) {
        val tileFunction = scope.get(i.toString).get.asInstanceOf[OpenEOProcess]
        results = results ++ tileFunction(context)(tiles)
      }
      results
    }
    arrayCounter = arrayElementStack.pop()
    contextStack.head.put(name,inputFunction)

  }


  def expressionStart(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    processStack.push(operator)
    contextStack.push(mutable.Map[String,Object]())
    typeStack.push(mutable.Map[String,String]())
  }

  def expressionEnd(operator:String,arguments:java.util.Map[String,Object]): Unit = {
    // TODO: this is not only about expressions anymore. Rename it to e.g. "leaveProcess" to be more in line with graph visitor in Python?
    logger.debug(operator + " process with arguments: " + contextStack.head.mkString(",") + " direct args: " + arguments.mkString(",") + " of types: " + typeStack.head.mkString(","))
    // Bit of argument sniffing to support multiple versions/variants of processes
    val hasXY = arguments.containsKey("x") && arguments.containsKey("y")
    val hasX = arguments.containsKey("x")
    val hasExpression = arguments.containsKey("expression")
    val hasExpressions = arguments.containsKey("expressions")
    val hasData = arguments.containsKey("data")
    val ignoreNoData = !(arguments.getOrDefault("ignore_nodata",Boolean.box(true).asInstanceOf[Object]) == Boolean.box(false) || arguments.getOrDefault("ignore_nodata",None) == "false" )
    val hasTrueCondition = Try(arguments.get("condition").toString.toBoolean).getOrElse(false)
    val hasConditionExpression = arguments.get("condition") != null && !arguments.get("condition").isInstanceOf[Boolean]

    //TODO check below can be more generic, needs some work to make sure 'typeStack' holds the right info in a consistent manner
    val xyConstantComparison = hasXY && ((arguments("x").isInstanceOf[String] && arguments("x") != "dummy" )
      || typeStack.head.getOrElse("x","") == "boolean"
      || (arguments("y").isInstanceOf[String]  && arguments("y") != "dummy"  )
      || typeStack.head.getOrElse("y","") == "boolean")

    val operation = {
      if(xyConstantComparison && booleanOperators.contains(operator)) {
        xyConstantFunction(operator,arguments)
      }else{
        operator match {
          case "date_difference" => dateDifferenceProcess(arguments)
          case "date_between" => dateBetweenProcess(arguments)
          case "date_shift" => dateShift(arguments)
          case "date_replace_component" => dateReplaceComponent(arguments)
          case "if" => ifProcess(arguments)
          // Comparison operators
          case "gt" if hasXY => xyFunction(Greater.apply, convertBitCells = false)
          case "lt" if hasXY => xyFunction(Less.apply, convertBitCells = false)
          case "gte" if hasXY => xyFunction(GreaterOrEqual.apply, convertBitCells = false)
          case "lte" if hasXY => xyFunction(LessOrEqual.apply, convertBitCells = false)
          case "between" if hasX => betweenFunction(arguments)
          case "eq" if hasXY => xyFunction(Equal.apply, convertBitCells = false)
          case "neq" if hasXY => xyFunction(Unequal.apply, convertBitCells = false)
          // Boolean operators
          case "not" if hasX => mapFunction("x", Not.apply)
          case "not" if hasExpression => mapFunction("expression", Not.apply) // legacy 0.4 style
          case "and" if hasXY => xyFunction(And.apply, convertBitCells = false)
          case "and" if hasExpressions => reduceFunction("expressions", And.apply) // legacy 0.4 style
          case "all" => reduceFunction("data", And.apply)
          case "or" if hasXY => xyFunction(Or.apply, convertBitCells = false)
          case "or" if hasExpressions => reduceFunction("expressions", Or.apply) // legacy 0.4 style
          case "any" => reduceFunction("data", Or.apply)
          case "xor" if hasXY => xyFunction(Xor.apply, convertBitCells = false)
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
          case "power" => xyFunction(Pow.apply, xArgName = "base", yArgName = "p")
          case "exp" => mapFunction("p", Exp.apply)
          case "normalized_difference" if hasXY => xyFunction((x, y) => Divide(Subtract(x, y), Add(x, y)))
          case "clip" => clipFunction(arguments)
          case "int" => intFunction(arguments)
          // Statistics
          case "max" if hasData && !ignoreNoData => reduceFunction("data", Max.apply)
          case "max" if hasData && ignoreNoData => reduceFunction("data", MaxIgnoreNoData.apply)
          case "min" if hasData && !ignoreNoData => reduceFunction("data", Min.apply)
          case "min" if hasData && ignoreNoData => reduceFunction("data", MinIgnoreNoData.apply)
          //TODO take ignorenodata into account!
          case "mean" if hasData => reduceListFunction("data", Mean.apply)
          case "variance" if hasData && !ignoreNoData => reduceListFunction("data", varianceWithNoData)
          case "variance" if hasData && ignoreNoData => reduceListFunction("data", Variance.apply)
          case "sd" if hasData && !ignoreNoData => reduceListFunction("data", Sqrt.apply _ compose varianceWithNoData)
          case "sd" if hasData && ignoreNoData => reduceListFunction("data", Sqrt.apply _ compose Variance.apply)
          case "median" if ignoreNoData => applyListFunction("data", median)
          case "median" => applyListFunction("data", medianWithNodata)
          case "count" if hasTrueCondition => applyListFunction("data", countAll)
          case "count" if hasConditionExpression => mapListFunction("data", "condition", countCondition)
          case "count" => applyListFunction("data", countValid)
          // Unary math
          case "abs" if hasX => mapFunction("x", Abs.apply)
          case "absolute" if hasX => mapFunction("x", Abs.apply)
          //TODO: "int" integer part of a number
          //TODO "arccos" -> Acosh.apply,
          //TODO: arctan 2 is not unary! "arctan2" -> Atan2.apply,
          case "log" => xyFunction(LogBase.apply, xArgName = "x", yArgName = "base")
          case "ln" if hasX => mapFunction("x", Log.apply)
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
          case "inspect" => inspectFunction(arguments)
          case "first" if ignoreNoData => applyListFunction("data", firstFunctionIgnoreNoData)
          case "first" => applyListFunction("data", firstFunctionWithNodata)
          case "last" if ignoreNoData => applyListFunction("data", lastFunctionIgnoreNoData)
          case "last" => applyListFunction("data", lastFunctionWithNoData)
          case "is_nodata" if hasX => mapFunction("x", Undefined.apply)
          case "is_nan" if hasX => mapFunction("x", Undefined.apply)
          case "array_element" => arrayElementFunction(arguments)
          case "array_modify" => arrayModifyFunction(arguments)
          case "array_interpolate_linear" => applyListFunction("data", linearInterpolation)
          case "array_find" => arrayFind(arguments)
          case "linear_scale_range" => linearScaleRangeFunction(arguments)
          case "quantiles" => quantilesFunction(arguments, ignoreNoData)
          case "array_concat" => arrayConcatFunction(arguments)
          case "array_append" => arrayAppendFunction(arguments)
          case "array_create" => arrayCreateFunction(arguments)
          case "array_apply" => arrayApplyFunction(arguments)
          case "predict_random_forest" if hasData => predictRandomForestFunction(arguments)
          case "predict_catboost" if hasData => predictCatBoostFunction(arguments)
          case "predict_probabilities" if hasData => predictCatBoostProbabilitiesFunction(arguments)
          case _ => throw new IllegalArgumentException(s"Unsupported operation: $operator (arguments: ${arguments.keySet()})")
        }
      }

    }

    if(operator != "linear_scale_range") {
      //TODO: generalize to other operations that result in a specific datatype?
      if(Array("gt","lt","lte","gte","eq","neq","between").contains(operator)) {
        resultingDataType = BitCellType
      }else{
        resultingDataType = FloatConstantNoDataCellType
      }
    }
    inputFunction = operation


    val expectedOperator = processStack.pop()
    assert(expectedOperator.equals(operator))
    contextStack.pop()
    typeStack.pop()
    if(typeStack.nonEmpty) {

      if((xyConstantComparison && booleanOperators.contains(operator)) || operator == "date_between"){
        typeStack.head(argNames.head) = "boolean"
      }else{
        typeStack.head(argNames.head) = resultingDataType.toString()
      }
    }

  }

  private def inspectFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val message = arguments.get("message").asInstanceOf[String]
    val level = arguments.get("level").asInstanceOf[String]

    val inspectFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      def log(message:String)={
        level.toLowerCase match{
          case "debug" => logger.debug(message)
          case "warning" => logger.warn(message)
          case "error" => logger.error(message)
          case _ => logger.info(message)
        }
      }
      if(message!="")
        log(message)
      log(tiles.map(_.asciiDraw()).mkString(""))
      tiles
    }
    inspectFunction
  }

  private def linearScaleRangeFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {

    val inMin = arguments.get("inputMin").asInstanceOf[Number].doubleValue()
    val inMax = arguments.get("inputMax").asInstanceOf[Number].doubleValue()
    val outMinRaw = arguments.getOrDefault("outputMin", 0.0.asInstanceOf[Object])
    val outMin: Double = outMinRaw.asInstanceOf[Number].doubleValue()
    val outMaxRaw = arguments.getOrDefault("outputMax", 1.0.asInstanceOf[Object])
    val outMax:Double = outMaxRaw.asInstanceOf[Number].doubleValue()
    val inputFunction = getProcessArg("x")
    val output_range = outMax - outMin
    val doTypeCast = output_range > 1 && (!outMinRaw.isInstanceOf[Double] && !outMinRaw.isInstanceOf[Float]) && (!outMaxRaw.isInstanceOf[Double] && !outMaxRaw.isInstanceOf[Float])
    val targetType: Option[CellType] =
    if(doTypeCast){
      if(output_range < 254 && outMin >= 0) {
        Some(UByteUserDefinedNoDataCellType(255.byteValue()))
      }else if(output_range < 65535 && outMin >= 0){
        Some(UShortUserDefinedNoDataCellType(UShort.MaxValue.toShort))
      }else if (outMax <= Short.MaxValue && outMin >= Short.MinValue + 1) {
        Some(ShortConstantNoDataCellType)
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
    val inputFunction = getProcessArg("data")
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
    val inputFunction = getProcessArg("data")
    val valuesFunction = getProcessArg("values")
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
        unifyCellType(data.take(index.asInstanceOf[Integer]) ++ values ++ data.drop(index.asInstanceOf[Integer]))
      }else{
        throw new UnsupportedOperationException("Geotrellis backend only supports inserting in array-modify")
      }

    }
    bandFunction
  }

  private def arrayAppendFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
        val inputFunction = getProcessArg("data")
    val valueFunction = getProcessArg("value")

    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) =>{
      val data: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val values: Seq[Tile] = evaluateToTiles(valueFunction, context, tiles)
      unifyCellType(data ++ values)
    }
    bandFunction
  }

  private def arrayApplyFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val inputFunction = getProcessArg("data")
    val processFunction = getProcessArg("process")

    val bandFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val labels = context.get("array_labels").asInstanceOf[Option[Seq[Any]]]
      val data: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val mappedValues = data.zipWithIndex.map{
        case (e, i) => evaluateToTiles(processFunction, context + ("x" -> Seq(e))  + ("index" -> i) + ("data"-> data) + ("parent.data"-> tiles) + ("label" -> labels.map(_(i)).orNull), Seq(e)).head
      }

      mappedValues
    }
    bandFunction
  }

  private def arrayConcatFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val array1Function = getProcessArg("array1")
    val array2Function = getProcessArg("array2")

    val bandFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val array1 = evaluateToTiles(array1Function, context, tiles)
      val array2 = evaluateToTiles(array2Function, context, tiles)

      val combined = array1 ++ array2
      unifyCellType(combined)

    }
    bandFunction
  }


  private def arrayCreateFunction(arguments: java.util.Map[String, Object]): OpenEOProcess = {
    val dataFunction:OpenEOProcess = getProcessArg("data")
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

    val inputFunction: OpenEOProcess = getProcessArg("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction(context)(tiles), context)
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
    val inputFunction: OpenEOProcess = getProcessArg("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction(context)(tiles), context)
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
    val inputFunction: OpenEOProcess = getProcessArg("data")
    def composed(context: Map[String, Any])(tiles: Seq[Tile]): Seq[Tile] = {
      operator(inputFunction(context)(tiles), context)
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




  private def arrayElementFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {

    val inputFunction = getProcessArg("data")
    val index = arguments.getOrDefault("index",null)
    val label = arguments.getOrDefault("label",null)
    if(index == null && label == null ) {
      throw new IllegalArgumentException("Missing 'index' or 'label' argument in array_element.")
    }
    if(label == null && !index.isInstanceOf[Integer]){
      throw new IllegalArgumentException("The 'index' argument should be an integer, but got: " + index)
    }
    val bandFunction = (context: Map[String,Any]) => (tiles:Seq[Tile]) => {
      val input: Seq[Tile] = evaluateToTiles(inputFunction, context, tiles)
      val theActualIndex: Int =
      if (label != null) {
        if(context.contains("array_labels")) {

          context("array_labels").asInstanceOf[util.List[String]].indexOf(label)
        }else{
          throw new IllegalArgumentException("array_element: ArrayNotLabeled")
        }
      }
      else{
        index.asInstanceOf[Integer]
      }

      if(theActualIndex<0 && label !=null){
        throw new IllegalArgumentException(s"array_element: Could not find label ${label}, these labels are available:${context("array_labels")}")
      }

      if (input.size <= theActualIndex) {
        throw new IllegalArgumentException("Invalid band index " + index + ", only " + input.size + " bands available.")
      }
      Seq(input(theActualIndex))
    }
    bandFunction
  }

  private def betweenFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {
    val inputFunction = getProcessArg("x")
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
    val inputFunction = getProcessArg("x")
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

  private def intFunction(arguments:java.util.Map[String,Object]): OpenEOProcess = {

    val inputFunction = getProcessArg("x")

    val clipFunction = (context: Map[String, Any]) => (tiles: Seq[Tile]) => {
      val input = evaluateToTiles(inputFunction, context, tiles).map(_.convert(FloatConstantNoDataCellType))
      input.map(_.mapIfSetDouble(_.toInt))
    }
    clipFunction
  }
}
