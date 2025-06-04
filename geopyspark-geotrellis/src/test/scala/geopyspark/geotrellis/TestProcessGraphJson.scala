package geopyspark.geotrellis

import org.junit.Test

class TestProcessGraphJson {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/processgraphs/load_synthetic_data.json")
  }
}
