package org.openeo.geotrellissentinelhub

import java.time.{LocalDate, ZoneId}
import java.util

import geotrellis.proj4.WebMercator
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Test

class TestS1Gamma0 {

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def testGamma0(): Unit = {
    val bbox = ProjectedExtent(Extent(-5948635.289265557,-1252344.2714243263,-5792092.255337516,-1095801.2374962857), WebMercator)

    val date = LocalDate.of(2019, 6, 1).atStartOfDay(ZoneId.systemDefault())

    val datasetId = "S1GRD"
    retrieveTileFromSentinelHub(datasetId, bbox, date, width = 256, height = 256, Seq("VV", "VH", "HV", "HH"),
      SampleType.FLOAT32, processingOptions = util.Collections.emptyMap[String, Any], clientId, clientSecret)
  }

}
