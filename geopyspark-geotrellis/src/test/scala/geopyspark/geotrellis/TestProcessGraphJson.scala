package geopyspark.geotrellis

import org.junit.Test
import org.junit.jupiter.api.Disabled

class TestProcessGraphJson {

  @Test
  def loadSyntheticData(): Unit = {
    ProcessGraphRunner.run("/processgraphs/load_synthetic_data.json")
  }

  @Test
  @Disabled
  def bug(): Unit = {
    ProcessGraphRunner.run("/processgraphs/bug.json")
  }

}
