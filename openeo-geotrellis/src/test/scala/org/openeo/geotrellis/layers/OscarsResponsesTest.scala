package org.openeo.geotrellis.layers

import java.io.{PrintWriter, StringWriter}
import java.net.URL

import geotrellis.vector.Extent
import org.junit.Assert._
import org.junit.Test

import scala.io.{Codec, Source}

class OscarsResponsesTest {

  private def loadJsonResource(classPathResourceName: String, codec: Codec = Codec.UTF8): String = {
    val jsonFile = Source.fromURL(getClass.getResource(classPathResourceName))(codec)

    try jsonFile.mkString
    finally jsonFile.close()
  }

  @Test
  def parseProductsResponse(): Unit = {
    val productsResponse = loadJsonResource("/org/openeo/geotrellis/layers/oscarsProductsResponse.json")
    val features = OscarsResponses.FeatureCollection.parse(productsResponse).features

    assertEquals(1, features.length)
    assertEquals(Extent(35.6948436874, -0.991331687854, 36.6805874343, 0), features.head.bbox)

    val Some(dataUrl) = features.head.links
      .find(_.title contains "SCENECLASSIFICATION_20M")
      .map(_.href)

    assertEquals(new URL("https://oscars-dev.vgt.vito.be/download" +
      "/CGS_S2_FAPAR/2019/11/28/S2A_20191128T075251Z_36MZE_CGS_V102_000/S2A_20191128T075251Z_36MZE_FAPAR_V102/10M" +
      "/S2A_20191128T075251Z_36MZE_SCENECLASSIFICATION_20M_V102.tif"), dataUrl)
  }

  @Test
  def parseNoProductsResponse(): Unit = {
    val productsResponse = loadJsonResource("/org/openeo/geotrellis/layers/oscarsNoProductsResponse.json")
    val features = OscarsResponses.FeatureCollection.parse(productsResponse).features

    assertEquals(0, features.length)
  }

  @Test
  def parseCollectionsResponse(): Unit = {
    val collectionsResponse = loadJsonResource("/org/openeo/geotrellis/layers/oscarsCollectionsResponse.json")
    val features = OscarsResponses.FeatureCollection.parse(collectionsResponse).features

    assertEquals(8, features.length)

    val Some(s2Fapar) = features
      .find(_.id == "urn:eop:VITO:CGS_S2_FAPAR")

    assertEquals(Extent(-179.999, -84, 179.999, 84), s2Fapar.bbox)
  }

  @Test
  def parseIncompleteResponseExceptionContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("/org/openeo/geotrellis/layers/oscarsProductsResponse.json")
    val incompleteResponse = productsResponse.take(productsResponse.length / 2)

    try OscarsResponses.FeatureCollection.parse(incompleteResponse)
    catch {
      case e =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  @Test
  def parseFaultyResponseExceptionContainsStackTraceContainsUsefulInformation(): Unit = {
    val productsResponse = loadJsonResource("/org/openeo/geotrellis/layers/oscarsProductsResponse.json")
    val faultyResponse = productsResponse.replace("features", "featurez")

    try OscarsResponses.FeatureCollection.parse(faultyResponse)
    catch {
      case e =>
        assertTrue(e.getMessage, e.getMessage contains """"type": "FeatureCollection"""")

        val stackTrace = this.stackTrace(e)
        assertTrue(stackTrace, stackTrace contains getClass.getName)
    }
  }

  private def stackTrace(e: Throwable): String = {
    val s = new StringWriter
    val out = new PrintWriter(s)

    try e.printStackTrace(out)
    finally out.close()

    s.toString
  }
}
