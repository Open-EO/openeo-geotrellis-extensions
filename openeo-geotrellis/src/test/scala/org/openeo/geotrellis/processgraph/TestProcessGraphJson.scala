package org.openeo.geotrellis.processgraph

import org.junit.Test

class TestProcessGraphJson {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data.json")
  }

  @Test
  def reduceT(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t.json")
  }

  @Test
  def reduceTApplyNeighbourhoodUdf(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t_apply_neighbourhood_udf.json")
  }
}
