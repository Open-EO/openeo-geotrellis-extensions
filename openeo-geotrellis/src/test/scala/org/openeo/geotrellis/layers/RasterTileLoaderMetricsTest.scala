package org.openeo.geotrellis.layers

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.{DoubleCounter, DoubleGauge}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{inOrder, mock, verify, verifyNoInteractions, verifyNoMoreInteractions}

class RasterTileLoaderMetricsTest {
  private val attributes = Attributes.builder().put("collection_ref", "test-collection").build()

  @Test
  def recordsFractionalDecimalMegapixelsAndLatestPartitionSpeed(): Unit = {
    val counter = mock(classOf[DoubleCounter])
    val gauge = mock(classOf[DoubleGauge])

    RasterTileLoader.recordLoadingMetrics(65536L, 500000000L, attributes, counter, gauge)
    RasterTileLoader.recordLoadingMetrics(1000000L, 2000000000L, attributes, counter, gauge)

    val ordered = inOrder(counter, gauge)
    ordered.verify(counter).add(0.065536, attributes)
    ordered.verify(gauge).set(0.131072, attributes)
    ordered.verify(counter).add(1.0, attributes)
    ordered.verify(gauge).set(0.5, attributes)
    verifyNoMoreInteractions(counter, gauge)
  }

  @Test
  def supportsPixelCountsAboveIntMaxValue(): Unit = {
    val counter = mock(classOf[DoubleCounter])
    val gauge = mock(classOf[DoubleGauge])

    RasterTileLoader.recordLoadingMetrics(3000000000L, 2000000000L, attributes, counter, gauge)

    verify(counter).add(3000.0, attributes)
    verify(gauge).set(1500.0, attributes)
  }

  @Test
  def retainsSubMillisecondTimingPrecision(): Unit = {
    val counter = mock(classOf[DoubleCounter])
    val gauge = mock(classOf[DoubleGauge])

    RasterTileLoader.recordLoadingMetrics(100L, 500000L, attributes, counter, gauge)

    verify(counter).add(0.0001, attributes)
    verify(gauge).set(0.2, attributes)
  }

  @Test
  def nonPositiveElapsedTimeDoesNotOverwriteGauge(): Unit = {
    Seq(0L, -1L).foreach { durationNanos =>
      val counter = mock(classOf[DoubleCounter])
      val gauge = mock(classOf[DoubleGauge])

      RasterTileLoader.recordLoadingMetrics(1000000L, durationNanos, attributes, counter, gauge)

      verify(counter).add(1.0, attributes)
      verifyNoInteractions(gauge)
    }
  }
}
