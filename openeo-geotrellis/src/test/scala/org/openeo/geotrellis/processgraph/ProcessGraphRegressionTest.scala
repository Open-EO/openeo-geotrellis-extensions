package org.openeo.geotrellis.processgraph

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIf

@EnabledIf("org.openeo.geotrelliscommon.TestConditions#runProcessGraphRegressionTests")
class ProcessGraphRegressionTest {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data.json")
  }

  @Test
  def loadSyntheticDataFailOnce(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data_fail_once.json")
  }

  /**
   * https://github.com/Open-EO/openeo-geotrellis-extensions/issues/286
   */
  @Test
  def filterLabelsAllNotDateBetween(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/filter_labels_all_not_date_between.json")
  }


}
