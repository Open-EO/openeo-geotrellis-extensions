package org.openeo.geotrellis

import org.junit.Assert._
import org.junit.Test
import org.openeo.sparklisteners.LogErrorSparkListener


class ErrorTest {
  private val inputMessage =
    """LogErrorSparkListener.onTaskEnd(...) error: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
      |  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 619, in main
      |    process()
      |  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 611, in process
      |    serializer.dump_stream(out_iter, outfile)
      |  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 132, in dump_stream
      |    for obj in iterator:
      |  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/util.py", line 74, in wrapper
      |    return f(*args, **kwargs)
      |  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 44, in wrapper
      |    return _FUNCTION_POINTERS[key](*args, **kwargs)
      |  File "/opt/openeo/lib/python3.8/site-packages/epsel.py", line 37, in first_time
      |    return f(*args, **kwargs)
      |  File "/opt/openeo/lib/python3.8/site-packages/openeo/util.py", line 362, in wrapper
      |    return f(*args, **kwargs)
      |  File "/opt/openeo/lib/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 794, in process_product
      |    dem_dir_context = S1BackscatterOrfeo._get_dem_dir_context(
      |  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 258, in _get_dem_dir_context
      |    dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
      |  File "/opt/openeo/lib64/python3.8/site-packages/openeogeotrellis/collections/s1backscatter_orfeo.py", line 664, in _creodias_dem_subset_srtm_hgt_unzip
      |    with zipfile.ZipFile(zip_filename, 'r') as z:
      |  File "/usr/lib64/python3.8/zipfile.py", line 1251, in __init__
      |    self.fp = io.open(file, filemode)
      |FileNotFoundError: [Errno 2] No such file or directory: '/eodata/auxdata/SRTMGL1/dem/N64E024.SRTMGL1.hgt.zip'
      |
      |
      |   at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:545)
      |   at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:703)
      |   at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:685)
      |   at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.hasNext(PythonRunner.scala:498)
      |   at org.apache.spark.InterruptibleIterator.hasNext(InterruptibleIterator.scala:37)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:511)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:511)
      |   at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:458)
      |   at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:489)
      |   at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:140)
      |   at org.apache.spark.shuffle.ShuffleWriteProcessor.write(ShuffleWriteProcessor.scala:59)
      |   at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:99)
      |   at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:52)
      |   at org.apache.spark.scheduler.Task.run(Task.scala:131)
      |   at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:506)
      |   at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1462)
      |   at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:509)
      |   at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
      |   at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
      |   at java.base/java.lang.Thread.run(Thread.java:829)""".stripMargin

  private val inputMessage2 =
    """LogErrorSparkListener.onTaskEnd(...) error: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
      |  File "/usr/lib64/python3.8/zipfile.py", line 1251, in __init__
      |    self.fp = io.open(file, filemode)
      |FileNotFoundError: [Errno 2] No such file or directory: '/eodata/auxdata/SRTMGL1/dem/N64E024.SRTMGL1.hgt.zip'
      |Second line of error
      |
      |   at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:545)
      |   at org.apache.spark.api.python.PythonRunner$$anon$3.read(PythonRunner.scala:703)
      |   at java.base/java.lang.Thread.run(Thread.java:829)""".stripMargin

  @Test
  def testPythonError(): Unit = {
    val extracted = LogErrorSparkListener.extractPythonError(inputMessage).get
    assertEquals("FileNotFoundError: [Errno 2] No such file or directory: '/eodata/auxdata/SRTMGL1/dem/N64E024.SRTMGL1.hgt.zip'", extracted)
  }

  @Test
  def testMultilineError(): Unit = {
    val extracted = LogErrorSparkListener.extractPythonError(inputMessage2)
    assertTrue(extracted.get.startsWith("FileNotFoundError:"))
    assertTrue(extracted.get.endsWith("Second line of error"))
  }

  @Test
  def testEmptyError(): Unit = {
    val extracted = LogErrorSparkListener.extractPythonError("")
    assertTrue(extracted.isEmpty)
  }

  @Test
  def testInvaliedError(): Unit = {
    // Should not crash
    val extracted = LogErrorSparkListener.extractPythonError("Traceback (most recent call last):")
    println(extracted)
  }
}
