package org.openeo.geotrellis.processgraph

import org.junit.Test
import org.junit.jupiter.api.Disabled

class TestProcessGraphJson {

  @Test
  @Disabled
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data.json")
  }

  @Test
  @Disabled
  def reduceT(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t.json")
  }

  @Test
  @Disabled
  def reduceTApplyNeighbourhoodUdf(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t_apply_neighbourhood_udf.json")
  }

  @Test
  @Disabled
  def applyNeighbourhoodUdfCheckDims(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/apply_neighbourhood_udf_check_dims.json")
  }
}
