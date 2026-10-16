package org.openeo.sar.provider

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test
import org.openeo.sar.metadata.Polarisation.{VH, VV}
import org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider.{polarisationBandIndex, requestedPolarisation}

import java.net.URI

class Sentinel1GrdRasterSourceProviderTest {
  private val safeRoot = URI.create("s3://eodata/S1B_IW_GRDH_1SDV_TEST.SAFE")
  private def measurement(pol: String) = s"$safeRoot/measurement/s1b-iw-grd-$pol-20191010t060611-001-cog.tiff"

  private def bandIndex(dataPath: String, bandName: String = null, assetBandNames: Option[Seq[String]] = None,
                        bandIndex: Int = 0, scenePols: Seq[org.openeo.sar.metadata.Polarisation] = Seq(VH, VV)): Int =
    polarisationBandIndex(requestedPolarisation(dataPath, bandName, assetBandNames, bandIndex), dataPath, scenePols, safeRoot)

  @Test
  def resolvesPolarisationFromMeasurementPath(): Unit = {
    assertEquals(0, bandIndex(measurement("vh")))
    assertEquals(1, bandIndex(measurement("vv")))
    assertEquals(1, bandIndex(measurement("VV").toUpperCase))
    // the path takes precedence over band metadata
    assertEquals(0, bandIndex(measurement("vh"), bandName = "VV"))
  }

  @Test
  def resolvesSafeRootFromBandName(): Unit = {
    assertEquals(0, bandIndex(safeRoot.toString, bandName = "VH"))
    assertEquals(1, bandIndex(safeRoot.toString, bandName = "sigma0_vv"))
  }

  @Test
  def resolvesSafeRootFromAssetBandNames(): Unit = {
    // asset band order differs from the scene's: the asset's band index is mapped through its band name
    assertEquals(0, bandIndex(safeRoot.toString, bandName = "B1", assetBandNames = Some(Seq("VV", "VH")), bandIndex = 1))
    assertEquals(1, bandIndex(safeRoot.toString, bandName = "B0", assetBandNames = Some(Seq("VV", "VH")), bandIndex = 0))
  }

  @Test
  def failsWhenPolarisationCannotBeDetermined(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => bandIndex(s"$safeRoot/measurement/unknown.tiff"))
    assertThrows(classOf[IllegalArgumentException], () => bandIndex(safeRoot.toString, bandName = "angle"))
    // "vvx" is not a polarisation token
    assertThrows(classOf[IllegalArgumentException], () => bandIndex(safeRoot.toString, bandName = "vvx"))
  }

  @Test
  def failsOnPolarisationMissingFromScene(): Unit =
    assertThrows(classOf[IllegalStateException], () => bandIndex(measurement("vh"), scenePols = Seq(VV)))
}
