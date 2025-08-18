package org.openeo.geotrellis

import geotrellis.spark.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.ClassRule
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.rules.ExternalResource
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Paths}

protected trait LocalSparkContextBase {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[LocalSparkContextBase])
  protected var _sc: Option[SparkContext] = None

  implicit def sc: SparkContext = {
    if (_sc.isEmpty) {
      var conf = new SparkConf()
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
        .set("spark.ui.enabled", "true")
      val eventsDir = Paths.get("/tmp/spark-events") // Can be configured with "spark.eventLog.dir"
      if (Files.exists(eventsDir)) {
        Files.list(eventsDir).forEach { path =>
          val weekInMs = 7L * 24 * 60 * 60 * 1000
          if (Files.isRegularFile(path) && Files.getLastModifiedTime(path).toMillis < System.currentTimeMillis() - weekInMs) {
            logger.info(s"Deleting old log file: $path")
            Files.delete(path)
          }
        }
        /*
        How to use history server:
        $SPARK_HOME/sbin/start-history-server.sh
        open http://localhost:18080/
         */
        conf = conf.set("spark.eventLog.enabled", "true")
      }
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf))
      if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
    }
    _sc.get
  }

  def tearDownSpark(): Unit = {
    if (_sc.isDefined) {
      _sc.get.stop()
      _sc = None
    }
  }
}

//noinspection JUnitMalformedDeclaration
trait LocalSparkContext extends LocalSparkContextBase {
  @ClassRule
  def sparkContext: ExternalResource = new ExternalResource {
    override def before(): Unit = sc

    override def after(): Unit = tearDownSpark()
  }
}

//noinspection JUnitMalformedDeclaration
trait LocalSparkContextJupyter extends LocalSparkContextBase {

  @BeforeAll
  def setupSpark(): Unit = sc

  @AfterAll
  def after(): Unit = tearDownSpark()
}
