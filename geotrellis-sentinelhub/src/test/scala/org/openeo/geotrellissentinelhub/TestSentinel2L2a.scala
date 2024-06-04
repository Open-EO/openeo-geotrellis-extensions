package org.openeo.geotrellissentinelhub

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestSentinel2L2a {

  @Test
  def extractTileId(): Unit = {
    val productId = "S2B_MSIL2A_20240424T104619_N0510_R051_T31UFS_20240424T122434"
    assertEquals(Some("31UFS"), Sentinel2L2a.extractTileId(productId))
  }
}
