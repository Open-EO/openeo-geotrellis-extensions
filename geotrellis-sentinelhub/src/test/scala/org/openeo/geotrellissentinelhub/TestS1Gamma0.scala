package org.openeo.geotrellissentinelhub

import java.time.{LocalDate, ZoneId}

import geotrellis.vector.Extent
import org.junit.{Ignore, Test}

class TestS1Gamma0 {

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def testGamma0(): Unit = {
    val bbox = new Extent(-5948635.289265557,-1252344.2714243263,-5792092.255337516,-1095801.2374962857)

    val date = LocalDate.of(2019, 6, 1).atStartOfDay(ZoneId.systemDefault())

    val datasetId = "S1GRD"
    retrieveTileFromSentinelHub(datasetId, bbox, date, 256, 256, Seq("VV", "VH", "HV", "HH"), clientId, clientSecret)
  }

}
