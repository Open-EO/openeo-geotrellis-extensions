package geopyspark.geotrellis

import org.junit.Test

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
    val hostGraphFolder = processGraph.getParent
    val processGraphName = processGraph.getName

    val currentDir = System.getProperty("user.dir")
    val outputDir = currentDir + "/target/processgraphs/results/" + processGraphName.replaceFirst(".json", "")
    new File(outputDir).mkdirs()

    val classPath = System.getProperty("java.class.path")
    val aM2RepositoryJar = classPath.split(":").filter(_.contains(".m2/repository")).head
    val hostM2RepositoryFolder = aM2RepositoryJar.substring(0, aM2RepositoryJar.indexOf(".m2/repository") + ".m2/repository".length)
    val dockerM2RepositoryFolder = "/repository"

    val p1 = classPath.split(":").filter(!_.endsWith(".jar")).minBy(_.length)
    val p2 = classPath.split(":").filter(!_.endsWith(".jar")).maxBy(_.length)

    val hostCodeFolder = Range(0, p1.length).filter(i => p1.substring(0, i) == p2.substring(0, i)).map(i => p1.substring(0, i)).filter(_.endsWith("/")).maxBy(_.length)
    val dockerCodeFolder = "/code/"
    val jars = classPath.split(":").filter(_.endsWith(".jar")).filter(_.contains(hostM2RepositoryFolder)).map(f => f.replaceFirst(hostM2RepositoryFolder, dockerM2RepositoryFolder)).reduce((acc, e) => acc + ":" + e)
    val folders = classPath.split(":").filter(!_.endsWith(".jar")).map(f => f.replaceFirst(hostCodeFolder, dockerCodeFolder)).reduce((acc, e) => acc + ":" + e)

    val dockerClassPath = jars + ":" + folders

    val debug = ManagementFactory.getRuntimeMXBean().getInputArguments().stream().anyMatch(_.contains("-agentlib:jdwp"))

    val cmd =
      if (debug) {
        val debugPort = findFirstOpenPort(5005)
        val sparkUIPort = findFirstOpenPort(4040)
        println(f"Waiting for remote debugger on port ${debugPort}")
        println(f"SparkUI will be available at http://localhost:${sparkUIPort}")
        f"docker run -p ${debugPort}:5005 -p ${sparkUIPort}:4040 -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs -v ${hostM2RepositoryFolder}:${dockerM2RepositoryFolder} -v ${hostCodeFolder}:${dockerCodeFolder} vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper /graphs/${processGraphName} /out ${dockerClassPath} DEBUG"
      } else {
        f"docker run -v ${outputDir}:/out -v ${hostGraphFolder}:/graphs -v ${hostM2RepositoryFolder}:${dockerM2RepositoryFolder} -v ${hostCodeFolder}:${dockerCodeFolder} vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper /graphs/${processGraphName} /out ${dockerClassPath}"
      }

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
