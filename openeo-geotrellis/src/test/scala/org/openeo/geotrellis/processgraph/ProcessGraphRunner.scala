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
  /**
   * Spark master used by run_process_graph_locally.py in the Docker image. Use e.g. "local-cluster[2,1,4096]" to run
   * with separate executor JVMs, so that executor loss (fail_once) actually kills an executor.
   */
  val defaultSparkMaster = "local[2,2]"
  val localClusterSparkMaster = "local-cluster[2,1,4096]"

  private val dockerImage = "vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper:latest"

  /**
   * Checked once per JVM: compares the digest of the image in the registry with the digests of the local copy and
   * warns if the local copy is missing or outdated. Does not pull.
   */
  private lazy val checkDockerImageUpToDate: Unit = {
    def dockerOutput(cmd: String*): Option[String] =
      try {
        val stdout = new StringBuilder
        val exitCode = cmd.!(ProcessLogger(line => stdout.append(line).append('\n'), _ => ()))
        if (exitCode == 0) Some(stdout.toString.trim) else None
      } catch {
        case _: Throwable => None
      }

    val remoteDigest = dockerOutput("docker", "buildx", "imagetools", "inspect", dockerImage, "--format", "{{.Manifest.Digest}}")
      .filter(_.startsWith("sha256:"))
    val localDigests = dockerOutput("docker", "image", "inspect", "--format", "{{range .RepoDigests}}{{println .}}{{end}}", dockerImage)
      .map(_.linesIterator.map(_.trim).filter(_.nonEmpty).map(_.split('@').last).toSet)

    (remoteDigest, localDigests) match {
      case (None, _) =>
        logger.warn(f"Could not determine the registry digest of $dockerImage, unable to check whether the local image is up to date")
      case (Some(_), None) =>
        logger.warn(f"$dockerImage is not available locally; run 'docker pull $dockerImage'")
      case (Some(remote), Some(local)) if !local.contains(remote) =>
        logger.warn(f"A newer $dockerImage is available ($remote, local: ${local.mkString(", ")}); run 'docker pull $dockerImage'")
      case _ =>
        logger.info(f"$dockerImage is up to date")
    }
  }

  def run(processGraphS: String): Unit = {
    run(processGraphS, defaultSparkMaster)
  }

  def run(processGraphS: String, sparkMaster: String): Unit = {
    run(new File(getClass.getResource(processGraphS).getFile), sparkMaster)
  }

  def run(processGraph: File, sparkMaster: String = defaultSparkMaster): Unit = {
    require(sparkMaster.nonEmpty && !sparkMaster.exists(_.isWhitespace), f"invalid Spark master: '$sparkMaster'")
    checkDockerImageUpToDate

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

    val cmd =
      if (debug) {
        val debugPort = findFirstOpenPort(5005)
        val sparkUIPort = findFirstOpenPort(4040)
        logger.info(f"Waiting for remote debugger on port $debugPort")
        logger.info(f"SparkUI will be available at http://localhost:$sparkUIPort")
        f"docker run $openeoPythonSrcMappings -e SPARK_MASTER_OVERRIDE=$sparkMaster -e PYTHON_SRC=/pyproj -e LD_LIBRARY_PATH=/opt/venv/lib/python3.11/site-packages/jep -p $debugPort:5005 -p $sparkUIPort:4040 $credentialsFileMapping $layerCatalogMapping $optionalDataMapping $optionalEODataMapping -v $outputDir:/out -v $hostGraphFolder:/graphs $classPathMappings $dockerImage /graphs/$processGraphName /out $dockerClassPath DEBUG"
      } else {
        f"docker run $openeoPythonSrcMappings -e SPARK_MASTER_OVERRIDE=$sparkMaster -e LD_LIBRARY_PATH=/opt/venv/lib/python3.11/site-packages/jep -v $outputDir:/out $credentialsFileMapping $layerCatalogMapping $optionalDataMapping $optionalEODataMapping -v $hostGraphFolder:/graphs $classPathMappings $dockerImage /graphs/$processGraphName /out $dockerClassPath"
      }
    logger.debug(f"Prepared command: $cmd")
    val output = cmd.!!
    logger.info(output)
  }

  /**
   * Mount the layer catalog from this repository over the one baked into the image, so catalog changes are picked up
   * without rebuilding the image.
   */
  lazy val layerCatalogMapping: String = {
    val relativePath = "src/test/python/testing/layercatalog.json"
    val currentDir = System.getProperty("user.dir")
    Seq(new File(currentDir, relativePath), new File(currentDir, "openeo-geotrellis/" + relativePath))
      .find(_.isFile)
      .map(f => f"-v ${f.getAbsolutePath}:/opt/openeo/testing/layercatalog.json:ro")
      .getOrElse {
        logger.warn(f"No $relativePath found, using the layer catalog of the Docker image")
        ""
      }
  }

  lazy val openeoPythonSrcMappings: String = {
    val openeoPythonSrc = System.getProperty("openeo.python.src")
    if (openeoPythonSrc != null) {
      s"-v $openeoPythonSrc:/openeo_python_src -e OPENEO_PYTHON_SRC=/openeo_python_src"
    } else {
      ""
    }
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

  lazy val optionalDataMapping: String =
    optionalMapping("/data", Seq("-v", "/data:/data"))

  /**
   * Host folder mounted read-only at /eodata in the container; defaults to /eodata, override with the EODATA_SOURCE
   * environment variable (e.g. when the eodata bucket is mounted elsewhere on this host).
   */
  lazy val eodataSource: String = Option(System.getenv("EODATA_SOURCE")).map(_.trim).filter(_.nonEmpty).getOrElse("/eodata")

  lazy val optionalEODataMapping: String =
    optionalMapping(eodataSource, Seq("--mount", f"type=bind,src=$eodataSource,dst=/eodata,readonly,bind-propagation=rslave"))

  /**
   * A folder that exists on this host is not necessarily mountable by the Docker daemon (e.g. FUSE mounts, mount
   * propagation that is neither shared nor slave, or a confined/remote daemon), in which case "docker run" fails.
   * Probing with the actual mount arguments keeps such an optional folder from breaking the whole test.
   */
  private def optionalMapping(path: String, dockerArgs: Seq[String]): String = {
    val folder = new File(path)
    if (!(folder.exists && folder.isDirectory)) {
      ""
    } else if (dockerDaemonCanMount(dockerArgs)) {
      dockerArgs.mkString(" ")
    } else {
      logger.warn(f"Skipping mount of $path: the Docker daemon cannot mount it")
      ""
    }
  }

  private def dockerDaemonCanMount(dockerArgs: Seq[String]): Boolean = {
    val cmd = Seq("docker", "run", "--rm", "--entrypoint", "true") ++ dockerArgs :+ dockerImage
    try {
      cmd.!(ProcessLogger(_ => ())) == 0
    } catch {
      case _: Throwable => false
    }
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
