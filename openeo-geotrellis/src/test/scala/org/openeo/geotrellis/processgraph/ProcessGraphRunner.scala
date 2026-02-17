package org.openeo.geotrellis.processgraph

import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.lang.management.ManagementFactory
import java.net.ServerSocket
import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Using

object ProcessGraphRunner {

  val logger: Logger = LoggerFactory.getLogger(ProcessGraphRunner.getClass)

  def run(processGraphS: String): Unit = {
    run(new File(getClass.getResource(processGraphS).getFile))
  }

  def run(processGraph: File): Unit = {

    val hostGraphFolder = processGraph.getParent
    val processGraphName = processGraph.getName

    val currentDir = System.getProperty("user.dir")
    val outputDir = currentDir + "/target/processgraph/results/" + processGraphName.replaceFirst(".json", "")

    new File(outputDir).mkdirs()

    logger.info(f"Output dir: $outputDir")

    val classPath = System.getProperty("java.class.path")

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

    val jarParts = classPath.split(":").filter(_.endsWith(".jar")).groupBy(s => s.substring(0, s.indexOf("/", 1))).map(e => findCommonPrefix(e._2.sorted))
    val folderParts = classPath.split(":").filter(!_.endsWith(".jar")).groupBy(s => s.substring(0, s.indexOf("/", 1))).map(e => findCommonPrefix(e._2.sorted))

    val jarMapping = jarParts.zipWithIndex.map { case (jarPart, i) => (jarPart, f"/jars$i") }
    val folderMapping = folderParts.zipWithIndex.map { case (folderPart, i) => (folderPart, f"/code$i") }

    var modifiedClassPath = classPath.split(":")
    jarMapping.foreach {
      case (jarPart, replacement) =>
        modifiedClassPath = modifiedClassPath.map(classPathElement => if (classPathElement.endsWith(".jar") && classPathElement.startsWith(jarPart)) {
          classPathElement.replaceFirst(jarPart, replacement)
        } else {
          classPathElement
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

    val classPathMappings = Stream(jarMapping, folderMapping).flatten
      .filter(f => !f._1.startsWith("/opt"))
      .map { case (a, b) => f"-v $a:$b" }.mkString(" ")

    val dockerClassPath = modifiedClassPath.mkString(":")

    val debug = ManagementFactory.getRuntimeMXBean.getInputArguments.stream().anyMatch(_.contains("-agentlib:jdwp"))

    val dockerImage = "vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper"

    val cmd =
      if (debug) {
        val debugPort = findFirstOpenPort(5005)
        val sparkUIPort = findFirstOpenPort(4040)
        logger.info(f"Waiting for remote debugger on port $debugPort")
        logger.info(f"SparkUI will be available at http://localhost:$sparkUIPort")
        f"docker run -e LD_LIBRARY_PATH=/opt/venv/lib/python3.11/site-packages/jep -p $debugPort:5005 -p $sparkUIPort:4040 $credentialsFileMapping $optionalDataMapping $optionalEODataMapping -v $outputDir:/out -v $hostGraphFolder:/graphs $classPathMappings $dockerImage /graphs/$processGraphName /out $dockerClassPath DEBUG"
      } else {
        f"docker run -e LD_LIBRARY_PATH=/opt/venv/lib/python3.11/site-packages/jep -v $outputDir:/out $credentialsFileMapping $optionalDataMapping $optionalEODataMapping -v $hostGraphFolder:/graphs $classPathMappings $dockerImage /graphs/$processGraphName /out $dockerClassPath"
      }
    logger.debug(f"Prepared command: $cmd")
    val output = cmd.!!
    logger.info(output)
  }

  lazy val awsCredentialsMapping: String = {
    Option(System.getProperty("http.credentials.file")).getOrElse(Option(System.getenv("HTTP_CREDENTIALS_FILE")).getOrElse("./http_credentials.json"))
  }

  lazy val credentialsFileMapping: String = {
    val credentialsFile = {
      val path = Option(System.getProperty("http.credentials.file")).getOrElse(Option(System.getenv("HTTP_CREDENTIALS_FILE")).getOrElse("./http_credentials.json"))
      val file = new File(path)
      if (file.exists()) {
        Some(file)
      } else {
        None
      }
    }
    if (credentialsFile.isEmpty) {
      logger.warn("No credentials file found")
    }
    credentialsFile.map(f => f"-v ${f.getAbsolutePath}:/opt/openeo/http_credentials.json").getOrElse("")
  }

  lazy val optionalDataMapping: String = {
    val dataFolder = {
      val file = new File("/data")
      if (file.exists && file.isDirectory) {
        Some(file)
      } else {
        None
      }
    }
    dataFolder.map(f => f"-v ${f.getAbsolutePath}:/data").getOrElse("")
  }

  lazy val optionalEODataMapping: String = {
    val dataFolder = {
      val file = new File("/eodata")
      if (file.exists && file.isDirectory) {
        Some(file)
      } else {
        None
      }
    }
    dataFolder.map(f => f"-v ${f.getAbsolutePath}:/eodata").getOrElse("")
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
