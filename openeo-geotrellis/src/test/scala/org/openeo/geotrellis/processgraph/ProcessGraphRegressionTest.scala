package org.openeo.geotrellis.processgraph

import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.{EnabledIf, EnabledIfSystemProperty}

@EnabledIf("org.openeo.geotrelliscommon.TestConditions#runProcessGraphRegressionTests")
class ProcessGraphRegressionTest {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data.json")
  }
}
