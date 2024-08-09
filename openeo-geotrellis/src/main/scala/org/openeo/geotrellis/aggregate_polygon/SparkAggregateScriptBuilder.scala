package org.openeo.geotrellis.aggregate_polygon

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{avg, count, countDistinct, first, kurtosis, last, lit, max, min, not, percentile_approx, product, skewness, stddev, sum, variance, when}
import org.apache.spark.sql.types.DataType
import org.openeo.geotrellis.OpenEOProcessScriptBuilder
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ListBuffer

object SparkAggregateScriptBuilder {
  private val logger = LoggerFactory.getLogger(classOf[AggregatePolygonProcess])
}

class SparkAggregateScriptBuilder {

  import SparkAggregateScriptBuilder._

  type ExpressionBuilder = (Column, String) => Column
  type MultiExpressionBuilder = (Column, String) => Seq[Column]

  val reducers = ListBuffer[ExpressionBuilder]()

  def generateFunction(context: Map[String,Any] = Map.empty): MultiExpressionBuilder = {
    return (col: Column, columnName: String) => {
      reducers.map(_(col, columnName))
    }
  }

  def generateFunction():MultiExpressionBuilder = {
    generateFunction(Map.empty)
  }

  def constantArgument(name:String,value:Number): Unit = {

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

  }

  def fromParameter(parameterName:String): Unit = {

  }

  def argumentEnd(): Unit = {
  }

  /**
   * Called for each element in the array.
   * @param name
   */
  def arrayStart(name:String): Unit = {
  }

  def arrayElementDone():Unit = {

  }



  def constantArrayElement(value: Number):Unit = {
    //quantiles can have this as part of its 'probabilities' argument, just ignore it here
  }

  def arrayEnd():Unit = {

  }


  def expressionStart(operator:String,arguments:java.util.Map[String,Object]): Unit = {

  }

  def badReducer(operator:String):ExpressionBuilder = {
    throw new IllegalArgumentException(s"The process $operator is not supported to be used in a reducer callback.")
  }

  def expressionEnd(operator:String,arguments:java.util.Map[String,Object]): Unit = {

    logger.debug(operator + " process with arguments: " + arguments.mkString(","))

    if( operator == "create_array" ||  operator == "array_create") {
      return
    }

    val hasData = arguments.containsKey("data")
    val ignoreNoData = !(arguments.getOrDefault("ignore_nodata",Boolean.box(true).asInstanceOf[Object]) == Boolean.box(false) || arguments.getOrDefault("ignore_nodata",None) == "false" )
    val condition = arguments.getOrDefault("condition",null)

    if(operator == "quantiles") {
      val probs = arguments.get("probabilities")
      val qRaw = arguments.get("q")
      if(probs==null && qRaw == null) {
        throw new IllegalArgumentException("QuantilesParameterMissing: either 'q' or 'probabilities' argument needs to be set")
      }else if(probs!=null && qRaw != null) {
        throw new IllegalArgumentException(s"QuantilesParameterConflict: both 'q' and 'probabilities' argument is set. ${qRaw} - ${probs}")
      }else {
        val probabilities: Seq[Double] = OpenEOProcessScriptBuilder.getQuantilesProbabilities(arguments)
        probabilities.foreach(p=>{
          reducers.append( (col: Column, columnName: String) => {percentile_approx(col,lit(p),lit(100000))})
        })
      }

    }else{

      val expressionBuilder: (Column, String) => Column = (col: Column, columnName: String) => {
        val theExpression:Column =
          operator match {
            case "mean" => avg(col)
            case "count" => {
              if(condition == Boolean.box(true) || condition == "true") {
                count(col.isNull.or(col.isNotNull))
              }else{
                count(col)
                //sum(not(col.isNull).cast("long"))
              }
            }
            case "max" => max(col)
            case "min" => min(col)
            case "first" => first(col)
            case "last" => last(col)
            case "median" => percentile_approx(col,lit(0.5),lit(100000))
            case "product" => product(col)
            case "sd" => stddev(col)
            case "sum" => sum(col)
            case "variance" => variance(col)
            case "kurtosis" => kurtosis(col)
            case "skewness" => skewness(col)
            case _ => throw new IllegalArgumentException(s"Unsupported reducer for aggregate_spatial: ${operator}")
          }
        theExpression
      }

      reducers.append(expressionBuilder)
    }




  }
}
