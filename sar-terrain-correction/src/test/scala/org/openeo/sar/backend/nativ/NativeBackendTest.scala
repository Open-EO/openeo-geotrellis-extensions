package org.openeo.sar.backend.nativ

import geotrellis.proj4.LatLng
import geotrellis.raster.{CellSize, FloatArrayTile, Raster}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test
import org.openeo.sar.backend.TerrainCorrectionBackend
import org.openeo.sar.geom.Vec3
import org.openeo.sar.metadata.{ImageTiming, Lut2D, PolarisationMetadata, Polarisation, S1GrdMetadata, SrgrPoly, SrgrPolyList}
import org.openeo.sar.orbit.{OrbitInterpolator, StateVector}
import org.openeo.sar.{SarProcessingConfig, TileComputeContext, TileRequest}

import java.nio.file.Files

class NativeBackendTest {

  @Test
  def computeReturnsEmptyTileWhenNoDemPixelsAreValid(): Unit = {
    val request = TileRequest(
      extent = Extent(0.0, 0.0, 2.0, 2.0),
      cellSize = CellSize(1.0, 1.0),
      crs = LatLng,
      polarisations = Seq(Polarisation.VV),
      config = SarProcessingConfig(shadowLayoverMask = true)
    )

    val demPath = Files.createTempFile("nan-dem-", ".tif")
    val demTile = FloatArrayTile.fill(Float.NaN, request.cols, request.rows)
    GeoTiff(Raster(demTile, request.extent), request.crs).write(demPath.toString, true)

    val ctx = TileComputeContext(
      request = request,
      metadata = fakeMetadata,
      sarSources = Map.empty,
      demSource = GeoTiffRasterSource(demPath.toString),
      geoidSource = None
    )

    val result = new NativeBackend().compute(ctx)

    assertEquals(request.config.bandCount(request.polarisations.size), result.bandCount)
    assertEquals(request.cols, result.cols)
    assertEquals(request.rows, result.rows)

    val maskBandIndex = request.polarisations.size
    val shadowLayoverBandIndex = request.polarisations.size + 1

    assertEquals(Double.NaN, result.band(maskBandIndex).getDouble(0, 0), 0.0)
    assertEquals(Double.NaN, result.band(shadowLayoverBandIndex).getDouble(0, 0), 0.0)
  }

  @Test
  def computeReturnsTileForLargerRequest(): Unit = {
    val request = TileRequest(
      extent = Extent(0.0, 0.0, 50.0, 50.0),
      cellSize = CellSize(1.0, 1.0),
      crs = LatLng,
      polarisations = Seq(Polarisation.VV),
      config = SarProcessingConfig(shadowLayoverMask = true)
    )

    val demPath = Files.createTempFile("larger-dem-", ".tif")
    val demTile = FloatArrayTile.fill(1000.0f, request.cols, request.rows)
    GeoTiff(Raster(demTile, request.extent), request.crs).write(demPath.toString, true)

    val ctx = TileComputeContext(
      request = request,
      metadata = fakeMetadata.copy(
        polarisations = Map(
          Polarisation.VV -> PolarisationMetadata(
            pol = Polarisation.VV,
            measurementUri = "dummy",
            sigmaLut = new Lut2D(Array(0), Array(0), Array(Array(1.0f))),
            noiseLut = new Lut2D(Array(0), Array(0), Array(Array(0.0f))),
            srgr = new SrgrPolyList(IndexedSeq(SrgrPoly(0.0, 1.0, 0.0, Array(1.0))))
          )
        )
      ),
      sarSources = Map.empty,
      demSource = GeoTiffRasterSource(demPath.toString),
      geoidSource = None
    )

    val result = new NativeBackend().compute(ctx)

    assertEquals(request.config.bandCount(request.polarisations.size), result.bandCount)
    assertEquals(request.cols, result.cols)
    assertEquals(request.rows, result.rows)
    assertEquals(Double.NaN, result.band(0).getDouble(10, 10), 0.0)
    assertEquals(Double.NaN, result.band(1).getDouble(10, 10), 0.0)
    assertEquals(Double.NaN, result.band(2).getDouble(10, 10), 0.0)
  }

  private def fakeMetadata: S1GrdMetadata = {
    val orbit = new OrbitInterpolator(IndexedSeq.tabulate(8) { i =>
      StateVector(i.toDouble, Vec3(7000000.0, 0.0, 0.0), Vec3(0.0, 1.0, 0.0))
    })

    S1GrdMetadata(
      sceneEpochUtcSecs = 0.0,
      timing = ImageTiming(
        firstLineUtcSecs = 0.0,
        lineTimeInterval = 1.0,
        numberOfLines = 10,
        numberOfPixels = 10,
        rangePixelSpacing = 1.0
      ),
      orbit = orbit,
      polarisations = Map(
        Polarisation.VV -> PolarisationMetadata(
          pol = Polarisation.VV,
          measurementUri = "dummy",
          sigmaLut = new Lut2D(Array(0), Array(0), Array(Array(1.0f))),
          noiseLut = new Lut2D(Array(0), Array(0), Array(Array(0.0f))),
          srgr = new SrgrPolyList(IndexedSeq.empty)
        )
      )
    )
  }
}
