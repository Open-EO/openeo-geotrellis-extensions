package org.openeo.geotrellis.processgraph

import org.openeo.geotrellis.layers.FileLayerProvider
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.lang.management.ManagementFactory
import java.net.ServerSocket
import scala.annotation.tailrec
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
    logger.error(f"Classpath: $classPath")

    val m2Dev = ".m2/repository"
    val m2Jenkins = "/localdata/M2"

    val aM2RepositoryJar = classPath.split(":").filter(cpe => {
      cpe.contains(m2Dev) || cpe.contains(m2Jenkins)
    }).head

    val hostM2RepositoryFolder = {
      if (aM2RepositoryJar.contains(m2Dev))
        aM2RepositoryJar.substring(0, aM2RepositoryJar.indexOf(m2Dev) + m2Dev.length)
      else
        aM2RepositoryJar.substring(0, aM2RepositoryJar.indexOf(m2Jenkins) + m2Jenkins.length)
    }
    logger.error(f"M2 folder: $hostM2RepositoryFolder")

    val dockerM2RepositoryFolder = "/repository"

    val p1 = classPath.split(":").filter(!_.endsWith(".jar")).minBy(_.length)
    val p2 = classPath.split(":").filter(!_.endsWith(".jar")).maxBy(_.length)

    val hostCodeFolder = Range(0, p1.length).filter(i => p1.substring(0, i) == p2.substring(0, i)).map(i => p1.substring(0, i)).filter(_.endsWith("/")).maxBy(_.length)
    logger.error(f"Code folder: $hostCodeFolder")
    val dockerCodeFolder = "/code/"
    val jars = classPath.split(":").filter(_.endsWith(".jar")).filter(_.contains(hostM2RepositoryFolder)).map(f => f.replaceFirst(hostM2RepositoryFolder, dockerM2RepositoryFolder)).reduce((acc, e) => acc + ":" + e)
    val folders = classPath.split(":").filter(!_.endsWith(".jar")).map(f => f.replaceFirst(hostCodeFolder, dockerCodeFolder)).reduce((acc, e) => acc + ":" + e)

    val dockerClassPath = folders + ":" + jars
    logger.error(f"Docker classpath: $dockerClassPath")

    logger.error("Checking if running in debug mode")
    val debug = ManagementFactory.getRuntimeMXBean().getInputArguments().stream().anyMatch(_.contains("-agentlib:jdwp"))
    if (debug) {
      logger.error("Running in debug mode")
    } else {
      logger.error("Not running in debug mode")
    }

    val dockerImage = "vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper"
//    val dockerImage = "run_process_graph_locally2"

    val cmd =
      if (debug) {
        val debugPort = findFirstOpenPort(5005)
        val sparkUIPort = findFirstOpenPort(4040)
        println(f"Waiting for remote debugger on port ${debugPort}")
        println(f"SparkUI will be available at http://localhost:${sparkUIPort}")
        f"docker run -p ${debugPort}:5005 -p ${sparkUIPort}:4040 -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs -v ${hostM2RepositoryFolder}:${dockerM2RepositoryFolder} -v ${hostCodeFolder}:${dockerCodeFolder} ${dockerImage} /graphs/${processGraphName} /out ${dockerClassPath} DEBUG"
      } else {
        f"docker run -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs -v ${hostM2RepositoryFolder}:${dockerM2RepositoryFolder} -v ${hostCodeFolder}:${dockerCodeFolder} ${dockerImage} /graphs/${processGraphName} /out ${dockerClassPath}"
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
