package org.openeo.geotrellis.udf

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{Assumptions, Test}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.io.Source
import jep.SharedInterpreter

import scala.util.{Failure, Success, Try}

class JepMemoryGuardTest {

  private val guardSource: String = {
    val source = Source.fromURL(getClass.getResource("/org/openeo/geotrellis/udf/jep_memory_guard.py"))
    try source.mkString finally source.close()
  }

  // Allocates `mib` MiB in steps of 1 MiB, giving the watchdog time to intervene.
  private def allocatingUdf(mib: Int): String =
    s"""import time
       |_data = []
       |for _ in range($mib):
       |    _data.append(bytearray(1024 * 1024))
       |    time.sleep(0.005)
       |del _data
       |""".stripMargin

  @Test
  def testMemoryLimitFromSparkConf(): Unit = {
    assertEquals(-1L, Udf.udfMemoryLimitBytes(None))
    assertEquals(-1L, Udf.udfMemoryLimitBytes(Some(new SparkConf(false))))
    assertEquals(2048L * 1024 * 1024, Udf.udfMemoryLimitBytes(Some(new SparkConf(false).set("spark.executor.pyspark.memory", "2g"))))
    assertEquals(512L * 1024 * 1024, Udf.udfMemoryLimitBytes(Some(new SparkConf(false).set("spark.executor.pyspark.memory", "512"))))
  }

  @Test
  def testGuardInPlainPython(@TempDir tempDir: Path): Unit = {
    val python = Seq("python3", "python").find(p => Try(new ProcessBuilder(p, "--version").start().waitFor() == 0).getOrElse(false))
    Assumptions.assumeTrue(python.isDefined, "python is not installed here")

    Files.write(tempDir.resolve("jep_memory_guard.py"), guardSource.getBytes(StandardCharsets.UTF_8))
    val script =
      """import threading
         |import time
         |import jep_memory_guard as guard
         |
         |results = {}
         |
         |def run_udf(name, mib):
         |    guard.enter(50 * 1024 * 1024)
         |    try:
         |        data = []
         |        for _ in range(mib):
         |            data.append(bytearray(1024 * 1024))
         |            time.sleep(0.005)
         |        results[name] = "ok"
         |    except guard.UdfMemoryLimitExceeded:
         |        results[name] = "limited"
         |    finally:
         |        guard.leave()
         |
         |for name, mib in [("big", 200), ("small", 20)]:
         |    t = threading.Thread(target=run_udf, args=(name, mib))
         |    t.start()
         |    t.join()
         |run_udf("small_after_big", 20)
         |print(sorted(results.items()))
         |""".stripMargin
    Files.write(tempDir.resolve("test_guard.py"), script.getBytes(StandardCharsets.UTF_8))

    val process = new ProcessBuilder(python.get, "test_guard.py").directory(tempDir.toFile).redirectErrorStream(true).start()
    assertTrue(process.waitFor(60, TimeUnit.SECONDS), "python did not finish in time")
    val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    assertEquals(0, process.exitValue(), output)
    assertTrue(output.contains("exceeds the limit"), output)
    assertTrue(output.contains("[('big', 'limited'), ('small', 'ok'), ('small_after_big', 'ok')]"), output)
  }

  @Test
  def testGuardInJep(): Unit = {
    val interp: Try[SharedInterpreter] =
      try Success(SharedInterpreterFactory.create())
      catch { case e: LinkageError => Failure(e) } // e.g. the jep native library is not on java.library.path
    Assumptions.assumeTrue(interp.isSuccess, s"JEP is not available here: ${interp.failed.map(_.toString).getOrElse("")}")

    try {
      Udf.enterMemoryGuard(interp.get, 50L * 1024 * 1024)
      val exception = Try(interp.get.exec(allocatingUdf(200)))
      Udf.leaveMemoryGuard(interp.get)
      assertTrue(exception.isFailure, "expected the UDF to exceed the memory limit")
      assertTrue(exception.failed.get.getMessage.contains("UdfMemoryLimitExceeded"), exception.failed.get.getMessage)

      Udf.enterMemoryGuard(interp.get, 50L * 1024 * 1024)
      interp.get.exec(allocatingUdf(20))
      Udf.leaveMemoryGuard(interp.get)
    } finally interp.get.close()
  }
}
