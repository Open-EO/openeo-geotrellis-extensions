package org.openeo.geotrellis.processgraph

import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.lang.management.ManagementFactory
import java.net.ServerSocket
import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.sys.process._
import scala.util.Using

object ProcessGraphRunner {

  def run(processGraphS: String): Unit = {
    run(new File(getClass.getResource(processGraphS).getFile))
  }

  def run(processGraph: File): Unit = {

    val logger: Logger = LoggerFactory.getLogger("docker-issues")

    val hostGraphFolder = processGraph.getParent
    val processGraphName = processGraph.getName

    val currentDir = System.getProperty("user.dir")
    val outputDir = currentDir + "/target/processgraph/results/" + processGraphName.replaceFirst(".json", "")

    logger.error(f"Output dir: ${outputDir}")

    new File(outputDir).mkdirs()

    logger.error(f"Output dir: ${outputDir} (OK)")

    val classPath = System.getProperty("java.class.path")
    logger.error(f"full Classpath: $classPath")


    classPath.split(":").foreach(
      cpe => println(f"Check ${cpe}: ${new File(cpe).exists}")
    )

    def findCommonPrefix(strings: Array[String]): String = {
      if (strings.length < 2) {
        ""
      } else {
        val first = strings.head
        val last = strings.last
        val maxSize = Math.min(first.length, last.length)
        var i = 0
        while (i < maxSize && (first.charAt(i) == last.charAt(i))) {
          i += 1
        }
        val commonPart = first.substring(0, i)
        commonPart.substring(0, commonPart.lastIndexOf("/"))
      }
    }


    val jarParts = classPath.split(":").filter(_.endsWith(".jar")).groupBy(s => s.substring(0, s.indexOf("/", 1))).map(e => findCommonPrefix(e._2))

    val folderParts = classPath.split(":").filter(!_.endsWith(".jar")).groupBy(s => s.substring(0, s.indexOf("/", 1))).map(e => findCommonPrefix(e._2))

    var modifiedClassPath = classPath.split(":")

    val jarMapping = jarParts.zipWithIndex.map { case (jarPart, i) => (jarPart, f"/jars${i}/") }
    val folderMapping = folderParts.zipWithIndex.map { case (folderPart, i) => (folderPart, f"/code${i}/") }

    jarMapping.foreach {
      case (jarPart, replacement) =>
        modifiedClassPath = modifiedClassPath.map(mcpe => if (mcpe.endsWith(".jar") && mcpe.startsWith(jarPart)) {
          mcpe.replaceFirst(jarPart, replacement)
        } else {
          mcpe
        })
    }

    folderMapping.foreach {
      case (folderPart, replacement) =>
        modifiedClassPath = modifiedClassPath.map(mcpe =>
          if (!mcpe.endsWith(".jar") && mcpe.startsWith(folderPart)) {
            mcpe.replaceFirst(folderPart, replacement)
          } else {
            mcpe
          })
    }
    modifiedClassPath = modifiedClassPath.filter(f => !f.startsWith("/opt"))

    Stream(jarMapping, folderMapping).flatten
      .foreach(f => {
        val file = new File(f._1)
        val bool = file.canExecute()
        println(bool)
      })

    val classPathMappings = Stream(jarMapping, folderMapping).flatten
      .filter(f => !f._1.startsWith("/opt"))
      .map { case (a, b) => f"-v ${a}:${b}" }.mkString(" ")

    val dockerClassPath = modifiedClassPath.mkString(":")
    logger.error(f"Docker classpath: $dockerClassPath")

    logger.error("Checking if running in debug mode")
    val debug = ManagementFactory.getRuntimeMXBean().getInputArguments().stream().anyMatch(_.contains("-agentlib:jdwp"))
    if (debug) {
      logger.error("Running in debug mode")
    } else {
      logger.error("Not running in debug mode")
    }

    val dockerImage = "vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper"

    val cmd =
      if (debug) {
        val debugPort = findFirstOpenPort(5005)
        val sparkUIPort = findFirstOpenPort(4040)
        println(f"Waiting for remote debugger on port ${debugPort}")
        println(f"SparkUI will be available at http://localhost:${sparkUIPort}")
        f"docker run -p ${debugPort}:5005 -p ${sparkUIPort}:4040 -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs ${classPathMappings} ${dockerImage} /graphs/${processGraphName} /out ${dockerClassPath} DEBUG"
      } else {
        f"docker run -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs ${classPathMappings} ${dockerImage} /graphs/${processGraphName} /out ${dockerClassPath}"
      }
    logger.error(f"Prepared command: $cmd")
    val output = cmd.!!
    println(output)
  }

  @tailrec
  def findFirstOpenPort(fromPort: Int): Int = {
    val triedInt = Using(new ServerSocket(fromPort))(
      _.getLocalPort
    )
    if (triedInt.isSuccess) {
      triedInt.get
    } else {
      findFirstOpenPort(fromPort + 1)
    }
  }
}
