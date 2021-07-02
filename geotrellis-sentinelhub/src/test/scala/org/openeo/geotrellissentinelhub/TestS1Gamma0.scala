package org.openeo.geotrellissentinelhub

import java.time.{LocalDate, ZoneId}
import java.util.Collections

import geotrellis.proj4.WebMercator
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Test

class TestS1Gamma0 {
  private def accessToken: String = new AuthApi().authenticate(Utils.clientId, Utils.clientSecret).access_token

  @Test
  def testGamma0(): Unit = {
    val bbox = ProjectedExtent(Extent(-5948635.289265557,-1252344.2714243263,-5792092.255337516,-1095801.2374962857), WebMercator)

    val date = LocalDate.of(2019, 6, 1).atStartOfDay(ZoneId.systemDefault())

    val endpoint = "https://services.sentinel-hub.com"
    val datasetId = "S1GRD"

    new DefaultProcessApi(endpoint).getTile(datasetId, bbox, date, width = 256, height = 256, Seq("VV", "VH", "HV", "HH", "localIncidenceAngle"),
      SampleType.FLOAT32, additionalDataFilters = Collections.singletonMap("orbitDirection", "DESCENDING"),
      processingOptions = Collections.singletonMap("orthorectify", true), accessToken)
  }

}
