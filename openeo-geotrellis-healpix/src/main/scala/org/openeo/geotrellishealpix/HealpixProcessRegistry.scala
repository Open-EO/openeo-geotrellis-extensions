package org.openeo.geotrellishealpix
import org.openeo.geotrelliscommon.OpenEOProcess

import java.lang.reflect.Method
import scala.collection.mutable
import scala.jdk.CollectionConverters._
/**
 * Discovers methods annotated with [[OpenEOProcess]] on HealpixDatacube
 * implementations. The Python wrapper calls [[listProcesses]] at init time,
 * then dispatches generically via [[invoke]].
 *
 * Process descriptors are returned as java.util.Map so py4j converts them
 * directly to Python dicts — no JSON serialization needed.
 */
object HealpixProcessRegistry {
  private case class MethodBinding(method: Method, annotation: OpenEOProcess)
  private lazy val bindings: Map[String, MethodBinding] = {
    // Scan concrete classes AND their interfaces (Scala traits compile to
    // Java interfaces, and annotations on trait methods live on the interface,
    // not on the implementing class).
    val seedClasses: Seq[Class[_]] = Seq(
      classOf[ScalarHealpixDatacube],
      classOf[PackedHealpixDatacube]
    )
    val allClasses: Seq[Class[_]] = seedClasses.flatMap { cls =>
      cls +: cls.getInterfaces.toSeq :+ cls.getSuperclass
    }.distinct.filter(_ != null)

    val seen = mutable.LinkedHashMap.empty[String, MethodBinding]
    for {
      cls    <- allClasses
      method <- cls.getMethods
      ann    <- Option(method.getAnnotation(classOf[OpenEOProcess]))
      if !seen.contains(ann.id())
    } {
      seen(ann.id()) = MethodBinding(method, ann)
    }
    seen.toMap
  }
  def listProcesses(): java.util.List[java.util.Map[String, AnyRef]] = {
    bindings.values.map { mb =>
      val params: java.util.List[java.util.Map[String, AnyRef]] =
        mb.method.getParameters.map { p =>
          Map[String, AnyRef](
            "name"     -> p.getName,
            "type"     -> p.getType.getSimpleName,
            "required" -> java.lang.Boolean.TRUE
          ).asJava
        }.toList.asJava
      Map[String, AnyRef](
        "id"           -> mb.annotation.id(),
        "description"  -> mb.annotation.description(),
        "returns"      -> mb.annotation.returns(),
        "scala_method" -> mb.method.getName,
        "params"       -> params
      ).asJava
    }.toList.asJava
  }
  def invoke(cube: HealpixDatacube,
             processId: String,
             args: java.util.Map[String, AnyRef]): AnyRef = {
    val mb = bindings.getOrElse(processId,
      throw new IllegalArgumentException(
        s"Unknown HealpixDatacube process: '$processId'. " +
          s"Available: ${bindings.keys.mkString(", ")}"))
    val method = cube.getClass.getMethod(
      mb.method.getName, mb.method.getParameterTypes: _*)
    val paramNames = method.getParameters.map(_.getName)
    val argValues = paramNames.map { name =>
      if (args.containsKey(name)) args.get(name)
      else null
    }
    method.invoke(cube, argValues: _*).asInstanceOf[AnyRef]
  }
  def hasProcess(processId: String): Boolean = bindings.contains(processId)
  def processIds(): java.util.List[String] = bindings.keys.toList.asJava
}
