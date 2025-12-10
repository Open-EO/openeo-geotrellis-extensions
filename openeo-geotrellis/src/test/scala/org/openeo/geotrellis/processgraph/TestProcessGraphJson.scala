package org.openeo.geotrellis.processgraph

import org.junit.jupiter.api.{Disabled, Test}

@Disabled("use locally for development")
class TestProcessGraphJson {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_synthetic_data.json")
  }

  @Test
  def loadSentinel5PData(): Unit = {
    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/load_sentinel_5p_data.json")
  }


  @Test
  @Disabled
  def reduceT(): Unit = {
    // TODO fix tests
//    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t.json")
  }

  @Test
  @Disabled
  def reduceTApplyNeighbourhoodUdf(): Unit = {
    // TODO fix tests
//    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/reduce_t_apply_neighbourhood_udf.json")
  }

  @Test
  @Disabled
  def applyNeighbourhoodUdfCheckDims(): Unit = {
    // TODO fix tests
//    ProcessGraphRunner.run("/org/openeo/geotrellis/processgraph/apply_neighbourhood_udf_check_dims.json")
  }
}
