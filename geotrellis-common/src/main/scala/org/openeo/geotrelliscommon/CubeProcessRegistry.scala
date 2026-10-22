package org.openeo.geotrelliscommon

import geotrellis.spark.MultibandTileLayerRDD
import org.slf4j.LoggerFactory

import java.lang.reflect.Method
import java.util.ServiceLoader
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Registry for openEO processes implemented as static functions on Scala objects
 * (companion objects / singletons) that operate on [[MultibandTileLayerRDD]].
 *
 * == Method contract ==
 *
 * Every method registered here must:
 *  - Be annotated with [[OpenEOProcess]]
 *  - Declare one or two parameters in this order:
 *    1. `datacube: MultibandTileLayerRDD[SpaceTimeKey]` — the input cube  (required)
 *    2. `context: Map[String, Any]`                    — process arguments (optional)
 *  - Return any value (the return is handed back to the caller as `AnyRef`)
 *
 *  Methods with only the datacube parameter are invoked without passing the context map.
 *
 * == Registration ==
 *
 * Pass the Scala object singleton instance to [[register]]:
 * {{{
 *   CubeProcessRegistry.register(CroptypeInference)
 * }}}
 * All `@OpenEOProcess`-annotated methods on the object are then discoverable
 * via [[listProcesses]] and invocable via [[invoke]].
 *
 * == Invocation ==
 *
 * {{{
 *   val args = java.util.Map.of("onnx_model_path", "/tmp/presto.onnx")
 *   val result = CubeProcessRegistry.invoke(datacube, "croptype_inference", args)
 * }}}
 *
 * The `java.util.Map` supplied to [[invoke]] is converted to a
 * `scala.collection.immutable.Map[String, Any]` before being passed to the
 * method, so callers do not need to handle Scala map creation on the Python side.
 */
object CubeProcessRegistry {

  private val logger = LoggerFactory.getLogger(getClass)

  private case class FunctionBinding(
    instance:   AnyRef,
    method:     Method,
    annotation: OpenEOProcess
  )

  private val bindings = mutable.LinkedHashMap.empty[String, FunctionBinding]

  /** Load all [[CubeProcessProvider]] implementations on the classpath via SPI. */
  private lazy val autoLoaded: Unit = {
    val loader = ServiceLoader.load(classOf[CubeProcessProvider])
    loader.iterator().asScala.foreach { provider =>
      logger.debug(s"SPI: loading CubeProcessProvider ${provider.getClass.getName}")
      register(provider.getInstance())
    }
  }

  /** Ensure SPI providers are loaded before any lookup. */
  private def ensureLoaded(): Unit = autoLoaded

  /**
   * Scan a Scala object for [[OpenEOProcess]]-annotated methods and add them
   * to the registry.
   *
   * Duplicate process IDs (same `id()` already registered from a previous
   * [[register]] call) are silently ignored — first registration wins.
   *
   * @param obj  The Scala object singleton, e.g. `CroptypeInference`.
   */
  def register(obj: AnyRef): Unit = {
    val cls = obj.getClass
    var found = 0
    for {
      method <- cls.getMethods
      ann    <- Option(method.getAnnotation(classOf[OpenEOProcess]))
      if !bindings.contains(ann.id())
    } {
      bindings(ann.id()) = FunctionBinding(obj, method, ann)
      found += 1
      logger.debug(s"Registered cube process '${ann.id()}' → ${cls.getName}#${method.getName}")
    }
    if (found == 0)
      logger.warn(s"No @OpenEOProcess methods found on ${cls.getName}. " +
        "Did you forget the annotation?")
  }

  /**
   * List all registered processes as Java maps (compatible with py4j → Python dict).
   *
   * Each map contains: `id`, `description`, `returns`, `scala_method`, `params`.
   */
  def listProcesses(): java.util.List[java.util.Map[String, AnyRef]] = {
    ensureLoaded()
    bindings.values.map { fb =>
      val params: java.util.List[java.util.Map[String, AnyRef]] =
        fb.method.getParameters.map { p =>
          Map[String, AnyRef](
            "name"     -> p.getName,
            "type"     -> p.getType.getSimpleName,
            "required" -> java.lang.Boolean.TRUE
          ).asJava
        }.toList.asJava
      Map[String, AnyRef](
        "id"           -> fb.annotation.id(),
        "description"  -> fb.annotation.description(),
        "returns"      -> fb.annotation.returns(),
        "scala_method" -> fb.method.getName,
        "params"       -> params
      ).asJava
    }.toList.asJava
  }

  /**
   * Invoke a registered process.
   *
   * The `args` map is converted to `scala.collection.immutable.Map[String, Any]`
   * and passed as the second argument to the method; the cube is passed as the first.
   *
   * @param cube       Input datacube.
   * @param processId  The `id()` value from the [[OpenEOProcess]] annotation.
   * @param args       Process arguments as a Java map (e.g. from py4j).
   * @return           Whatever the registered method returns.
   * @throws IllegalArgumentException if `processId` is not registered.
   */
  def invoke(
    cube:      Object,
    processId: String,
    args:      java.util.Map[String, AnyRef]
  ): AnyRef = {
    ensureLoaded()
    val fb = bindings.getOrElse(processId,
      throw new IllegalArgumentException(
        s"Unknown process: '$processId'. " +
          s"Available: ${bindings.keys.mkString(", ")}"))

    if (fb.method.getParameterCount == 1)
      fb.method.invoke(fb.instance, cube)
    else {
      fb.method.invoke(fb.instance, cube, args)
    }
  }

  /** Returns true if `processId` has been registered. */
  def hasProcess(processId: String): Boolean = { ensureLoaded(); bindings.contains(processId) }

  /** Returns all registered process IDs. */
  def processIds(): java.util.List[String] = { ensureLoaded(); bindings.keys.toList.asJava }

  /**
   * Remove all registrations.  Useful in tests to reset state between runs.
   */
  def clear(): Unit = bindings.clear()
}
