package org.openeo.geotrelliscommon

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.{assertEquals, assertNotSame, assertSame}
import org.junit.{AfterClass, BeforeClass, Test}

object ScopedMetadataTrackerTest {
  private implicit var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = sc = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(getClass.getSimpleName)

    SparkContext.getOrCreate(conf)
  }

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()

}

class ScopedMetadataTrackerTest {
  import ScopedMetadataTrackerTest._

  @Test
  def testTrackerIsMemoized(): Unit = {
    assertSame(ScopedMetadataTracker("r-abc123"), ScopedMetadataTracker("r-abc123"))
    assertNotSame(ScopedMetadataTracker("r-abc123"), ScopedMetadataTracker("r-def456"))
  }

  @Test
  def testTrackSentinelHubProcessingUnits(): Unit = {
    try {
      val tracker1 = ScopedMetadataTracker("r-abc123")
      val tracker2 = ScopedMetadataTracker("r-def456")

      val ints = sc.parallelize(1 to 100)
        .map { i =>
          tracker1.addSentinelHubProcessingUnits(amount = 1.0)
          tracker2.addSentinelHubProcessingUnits(amount = 2.0)
          i
        }

      ints.sum()

      val tracker1_ = ScopedMetadataTracker("r-abc123")
      val tracker2_ = ScopedMetadataTracker("r-def456")

      assertEquals(100, tracker1_.sentinelHubProcessingUnits, 0.0)
      assertEquals(200, tracker2_.sentinelHubProcessingUnits, 0.0)
    } finally sc.stop()
  }
}
