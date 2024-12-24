package org.openeo.geotrellis

import scala.collection.mutable

class ProcessGraphVisitException(message: String) extends Exception(message)

class ProcessGraphVisitor() {

  private var processStack = mutable.Stack[String]()

  def dereferenceFromNodeArguments(processGraph: java.util.Map[String, Any]): String = {

    def resolveFromNode(node: Any, fromNode: Any): String = {
      processGraph.get(fromNode) match {
        case Some(value:String) => value
        case null => throw new ProcessGraphVisitException(s"from_node $fromNode (referenced by $node) not in process graph.")
      }
    }

    var resultNode: Option[String] = None
    processGraph.forEach {
      case (node, nodeDict: java.util.Map[String,Any]) =>
        if (nodeDict.containsKey("result")) {
          if (resultNode.isDefined)
            throw new ProcessGraphVisitException(s"Multiple result nodes found: $resultNode, $node")
          resultNode = Some(node)
        }
        var arguments: java.util.Map[String, Any] = nodeDict.get("arguments") match {
          case Some(v:java.util.Map[String,Any]) => v
          case _ => new java.util.HashMap[String, Any]
        }

        arguments.forEach {
          case (_, value: java.util.Map[String, Any]) =>
            value.get("from_node") match {
              case Some(fromNode: String) =>
                value.put("node", resolveFromNode(node, fromNode))
              case null =>
                value.forEach {
                  case (k, v: Map[String, Any]) =>
                    if (v.contains("from_node"))
                      value.put(k, resolveFromNode(node, v("from_node")))
                  case _ =>
                }
            }
          case (argId, value: List[Any]) =>
            value.foreach {
              case elem: Map[String, Any] =>
                if (elem.contains("from_node"))
                  arguments.put(argId, resolveFromNode(node, elem("from_node")))
              case _ =>
            }
          case _ =>
        }
      case _ =>
    }

    resultNode.getOrElse(throw new ProcessGraphVisitException("No result node found"))
  }

  def acceptProcessGraph(graph: java.util.Map[String, Any]): ProcessGraphVisitor = {
    val topLevelNode = dereferenceFromNodeArguments(graph)
    graph.get(topLevelNode) match {
      case Some(map: java.util.Map[String,Any]) =>
        acceptNode(map)
      case _ =>
    }
    this
  }

  def acceptNode(node: java.util.Map[String, Any]): Unit = {
    val pid = node.get("process_id") match{
      case Some(v:String) => v
    }
    val arguments = node.get("arguments") match {
      case Some(v:java.util.Map[String,Any]) => v
      case _ => new java.util.HashMap[String,Any]
    }
    val namespace = node.get("namespace") match {
      case v : Option[String] => v
      case _ => null
    }
    _acceptProcess(pid, arguments, namespace)
  }

  private def _acceptProcess(processId: String, arguments: java.util.Map[String, Any], namespace: Option[String]): Unit = {
    processStack.push(processId)
    enterProcess(processId, arguments, namespace)
    arguments.forEach {
      case (argId, value: List[Any]) =>
        enterArray(argId)
        _acceptArgumentList(value)
        leaveArray(argId)
      case (argId, value: java.util.Map[String, Any]) =>
        enterArgument(argId, value)
        _acceptArgumentDict(value)
        leaveArgument(argId, value)
      case (argId, value) =>
        constantArgument(argId, value)
    }
    leaveProcess(processId, arguments, namespace)
    assert(processStack.pop() == processId)
  }

  // Accept a list of arguments
  private def _acceptArgumentList(elements: List[Any]): Unit = {
    elements.foreach {
      case elem: java.util.Map[String, Any] =>
        _acceptArgumentDict(elem)
        arrayElementDone(elem)
      case elem =>
        constantArrayElement(elem)
    }
  }

  // Accept a dictionary argument
  private def _acceptArgumentDict(value: java.util.Map[String, Any]): Unit = {
    value.get("node") match {
      case Some(node: java.util.Map[String, Any]) => acceptNode(node)
      case None =>
        value.get("from_node") match {
          case Some(node: java.util.Map[String,Any]) => acceptNode(node)
          case None =>
            value.get("process_id") match {
              case Some(_) => acceptNode(value)
              case None =>
                value.get("from_parameter") match {
                  case Some(parameter:java.util.Map[String,Any]) => fromParameter(parameter.asInstanceOf[String])
                  case None => _acceptDict(value)
                }
            }
        }
    }
  }

  private def _acceptDict(value: java.util.Map[String, Any]): Unit = {

  }

  private def fromParameter(parameterId: String): Unit = {

  }

  private def enterProcess(processId: String, arguments: java.util.Map[String, Any], namespace: Option[String]): Unit = {

  }

  private def leaveProcess(processId: String, arguments: java.util.Map[String, Any], namespace: Option[String]): Unit = {

  }


  private def enterArgument(argumentId: String, value: java.util.Map[String, Any]): Unit = {

  }

  private def leaveArgument(argumentId: String, value: java.util.Map[String, Any]): Unit = {

  }

  private def constantArgument(argumentId: String, value: Any): Unit = {

  }

  private def enterArray(argumentId: String): Unit = {

  }

  private def leaveArray(argumentId: String): Unit = {

  }

  private def constantArrayElement(value: Any): Unit = {

  }

  private def arrayElementDone(value: java.util.Map[String, Any]): Unit = {

  }
}


class GeotrellisTileProcessGraphVisitor (_builder: Option[OpenEOProcessScriptBuilder] = None) extends ProcessGraphVisitor {
  private val builder = _builder.getOrElse(new OpenEOProcessScriptBuilder())
  // Process list to keep track of processes
  private val processes = mutable.LinkedHashMap[String, java.util.Map[String, Object]]()
  // Companion object for the 'create' method

  def create(defaultInputParameter: Option[String] = None, defaultInputDataType: Option[String] = None) = {
    val builder = new OpenEOProcessScriptBuilder()
    defaultInputDataType match {
      case Some(v) => builder.setInputDataType(v)
      case _ =>
    }
    new GeotrellisTileProcessGraphVisitor(Some(builder))
  }

  def enterProcess(processId: String, arguments: java.util.Map[String, Object], namespace: Option[String]): GeotrellisTileProcessGraphVisitor = {
    builder.expressionStart(processId, arguments)
    processes += (processId -> arguments)
    this
  }

  // Method to handle leaving a process
  def leaveProcess(processId: String, arguments: java.util.Map[String, Object], namespace: Option[String]): GeotrellisTileProcessGraphVisitor = {
    builder.expressionEnd(processId, arguments)
    this
  }

  def enterArgument(argumentId: String, value: Any): GeotrellisTileProcessGraphVisitor = {
    builder.argumentStart(argumentId)
    this
  }

  // Method to handle leaving an argument
  def leaveArgument(argumentId: String, value: Any): GeotrellisTileProcessGraphVisitor = {
    builder.argumentEnd()
    this
  }

  // Method to handle 'from parameter' calls
  def fromParameter(parameterId: String): GeotrellisTileProcessGraphVisitor = {
    builder.fromParameter(parameterId)
    this
  }

  // Method to handle constant arguments
  def constantArgument(argumentId: String, value: Any): GeotrellisTileProcessGraphVisitor = {
    value match {
      case _: String =>
      case v: Double => builder.constantArgument(argumentId, v)
      case v: Boolean => builder.constantArgument(argumentId, v)
      case _ => throw new IllegalArgumentException(s"Unexpected value for $argumentId: got $value")
    }
    this
  }

  // Method to handle entering an array
  def enterArray(argumentId: String): Unit = {
    builder.arrayStart(argumentId)
  }

  // Method to handle constant array elements
  def constantArrayElement(value: Number): Unit = {
    builder.constantArrayElement(value)
  }

  // Method to handle array element completion
  def arrayElementDone(value: Map[String, Any]): Unit = {
    builder.arrayElementDone()
  }

  // Method to handle leaving an array
  def leaveArray(argumentId: String): Unit = {
    builder.arrayEnd()
  }

  // Method to handle a dictionary (process graph)
  def acceptDict(value: java.util.Map[String, Any]): Unit = {
    value.get("process_graph") match {
      case Some(v: java.util.Map[String,Any]) => acceptProcessGraph(v)
    }
  }

}

