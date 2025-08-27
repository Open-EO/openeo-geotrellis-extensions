import org.apache.spark.scheduler.SparkListener
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SparkRetryExample {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SparkRetryExample")
      .setMaster("local[*, 5]") //  allow retries
      .set("spark.task.maxFailures", "5")
      //      .set("spark.executor.extraJavaOptions", "-Dorg.slf4j.simpleLogger.log.org.apache.spark=DEBUG")
      .set("spark.ui.enabled", "true")


    val sc = new SparkContext(conf)

    // Create an RDD with more data and partitions
    val data = sc.parallelize(1 to 20, 1) //  partitions

    // Define a function that will fail randomly
    def failRandomly(x: Int): Int = {
      val random = new Random()
      if (random.nextDouble() < 0.3) { // 30% chance of failure
        val attempt = org.apache.spark.TaskContext.get().attemptNumber()
        println(s"Failing task for value $x (attempt $attempt)")
        throw new RuntimeException(s"Intentional failure for value $x (attempt $attempt)")
      } else {
        x
      }
    }

    // Apply the function to the RDD
    val result = data.map(failRandomly)

    // Trigger an action to execute the transformation

    try {
      val ret = result.collect()
      println(ret.mkString(", "))
    } catch {
      case _: Throwable =>
    }
    println("Sleeping now...")
    while (true) {
      Thread.sleep(1000)
    }

    // Stop the SparkContext
    sc.stop()
  }
}
