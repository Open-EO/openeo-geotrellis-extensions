package org.openeo.sar.backend.nativ

import geotrellis.proj4.LatLng
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.{CellSize, FloatArrayTile, Raster}
import geotrellis.vector.Extent
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.openeo.sar.dem.DemItem
import org.openeo.sar.geom.{Ecef, Vec3}
import org.openeo.sar.metadata._
import org.openeo.sar.orbit.{OrbitInterpolator, StateVector}
import org.openeo.sar._

import java.nio.file.Files

class NativeBackendTest {

  @Test
  def computeReturnsEmptyTileWhenDemCannotBeRead(): Unit = {
    val request = TileRequest(
      extent = Extent(0.0, 0.0, 2.0, 2.0),
      cellSize = CellSize(1.0, 1.0),
      crs = LatLng,
      polarisations = Seq(Polarisation.VV),
      config = SarProcessingConfig(shadowLayoverMask = true, softErrors = true)
    )

    val unreadableDem = DemItem(request.extent, "/does/not/exist/dem.tif")

    val ctx = TileComputeContext(
      request = request,
      metadata = fakeMetadata,
      sarSources = Map.empty,
      demItems = Seq(unreadableDem),
      geoidSourceFactory = None
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
      config = SarProcessingConfig(shadowLayoverMask = true, softErrors = true)
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
      demItems = Seq(DemItem(request.extent, demPath.toString)),
      geoidSourceFactory = None
    )

    val result = new NativeBackend().compute(ctx)

    assertEquals(request.config.bandCount(request.polarisations.size), result.bandCount)
    assertEquals(request.cols, result.cols)
    assertEquals(request.rows, result.rows)
    assertEquals(Double.NaN, result.band(0).getDouble(10, 10), 0.0)
    assertEquals(Double.NaN, result.band(1).getDouble(10, 10), 0.0)
    assertEquals(Double.NaN, result.band(2).getDouble(10, 10), 0.0)
  }

  @Test
  def blocksCoverTheTileAndFit(): Unit = {
    val fits = (cols: Int, rows: Int) => cols.toLong * rows <= 10000
    val tileBlocks = NativeBackend.blocks(250, 130, fits)
    assertTrue(tileBlocks.size > 1)
    assertTrue(tileBlocks.forall(b => fits(b.cols, b.rows)))
    val covered = Array.ofDim[Int](130, 250)
    for (b <- tileBlocks; r <- b.row0 until b.row0 + b.rows; c <- b.col0 until b.col0 + b.cols) covered(r)(c) += 1
    assertTrue(covered.forall(_.forall(_ == 1)))

    assertEquals(Seq(NativeBackend.Block(0, 0, 250, 130)), NativeBackend.blocks(250, 130, (_, _) => true))
    // splitting stops once blocks are at most minSide per side, even if they do not fit
    assertTrue(NativeBackend.blocks(250, 130, (_, _) => false, minSide = 100).forall(b => b.cols > 50 && b.rows > 50 && b.cols <= 100 && b.rows <= 100))
  }

  /** A tile computed in blocks, because of a small memory budget, equals the tile computed at once. */
  @Test
  def tileComputedInBlocksEqualsTileComputedAtOnce(): Unit = {
    val scene = syntheticScene()
    for (algorithm <- Seq(RtcAlgorithm.Angle, RtcAlgorithm.BilinearDistribution, RtcAlgorithm.Auto)) {
      val config = SarProcessingConfig(normalization = BackscatterNormalization.Gamma0RTC, shadowLayoverMask = true,
        localIncidenceAngle = true, rtcAlgorithm = algorithm, rtcMethodBand = true, softErrors = false)
      val ctx = scene.copy(request = scene.request.copy(config = config))

      val atOnce = new NativeBackend().compute(ctx)
      val gamma0 = atOnce.band(0).toArrayDouble()
      assertTrue(gamma0.count(!_.isNaN) > gamma0.length / 2, s"${algorithm.name}: mostly valid pixels")
      if (algorithm != RtcAlgorithm.Angle)
        assertTrue(atOnce.band(config.bandCount(1) - 1).toArrayDouble().contains(1.0), s"${algorithm.name}: area-based pixels")

      // 4 MB: 2 x 2 blocks on the memory estimate; 400 kB: smaller still, on the exact radar grid size of area-based RTC
      for (budget <- Seq(4L << 20, 400L << 10)) {
        val inBlocks = new NativeBackend(budget).compute(ctx)
        assertEquals(config.bandCount(1), inBlocks.bandCount)
        assertEquals((ctx.request.cols, ctx.request.rows), (inBlocks.cols, inBlocks.rows))

        // band 0: gamma0, 1: local incidence angle, 2: validity, 3: shadow/layover, 4: method
        val geometryBands = Seq(1, 2, 3)
        for (b <- geometryBands ++ (if (algorithm == RtcAlgorithm.Angle) Seq(0, 4) else Seq.empty))
          assertArrayEquals(atOnce.band(b).toArrayDouble(), inBlocks.band(b).toArrayDouble(), 1e-6, s"${algorithm.name}: band $b")

        if (algorithm == RtcAlgorithm.BilinearDistribution && budget == (4L << 20)) {
          // Same radar grid lattice: equal, except for a few pixels in layover with terrain beyond the DEM margin.
          val actual = inBlocks.band(0).toArrayDouble()
          val differing = gamma0.indices.count(i => !(gamma0(i).isNaN && actual(i).isNaN) &&
            !(math.abs(actual(i) - gamma0(i)) <= 1e-6 * math.abs(gamma0(i))))
          assertTrue(differing < gamma0.length / 100, s"${algorithm.name}: $differing pixels differ")
        }
      }
    }
  }

  /** A small scene of hills (with layover and steep slopes) near (0°, 0°), seen from a satellite in a polar orbit
   *  at 4° W, with a synthetic image. */
  private def syntheticScene(): TileComputeContext = {
    val extent = Extent(0.0, 0.0, 0.03, 0.03)
    val cellSize = CellSize(0.0002, 0.0002)
    val polarisation = Polarisation.VV

    val demExtent = extent.buffer(0.01)
    val demCols = 500
    val demTile = FloatArrayTile.empty(demCols, demCols)
    for (r <- 0 until demCols; c <- 0 until demCols) {
      val lon = demExtent.xmin + (c + 0.5) * demExtent.width / demCols
      val lat = demExtent.ymax - (r + 0.5) * demExtent.height / demCols
      demTile.setDouble(c, r, 400 + 350 * math.sin(2 * math.Pi * lon / 0.008) * math.cos(2 * math.Pi * lat / 0.006))
    }
    val demPath = Files.createTempFile("synthetic-dem-", ".tif")
    GeoTiff(Raster(demTile, demExtent), LatLng).write(demPath.toString, true)
    demPath.toFile.deleteOnExit()

    val nLines = 700; val nPixels = 800
    val image = FloatArrayTile.empty(nPixels, nLines)
    for (r <- 0 until nLines; c <- 0 until nPixels) image.setDouble(c, r, 100 + 30 * math.sin(c / 13.0) + 20 * math.cos(r / 17.0))
    val imagePath = Files.createTempFile("synthetic-image-", ".tif")
    GeoTiff(Raster(image, Extent(0, 0, nPixels, nLines)), LatLng).write(imagePath.toString, true)
    imagePath.toFile.deleteOnExit()

    val rs = 6378137.0 + 700000.0
    val lonSat = math.toRadians(-4.0)
    val omega = 7500.0 / rs
    val lat0 = math.toRadians(-0.01)
    val orbit = new OrbitInterpolator(IndexedSeq.tabulate(12) { i =>
      val t = i - 5.0
      val phi = lat0 + omega * t
      StateVector(t,
        Vec3(rs * math.cos(phi) * math.cos(lonSat), rs * math.cos(phi) * math.sin(lonSat), rs * math.sin(phi)),
        Vec3(-rs * omega * math.sin(phi) * math.cos(lonSat), -rs * omega * math.sin(phi) * math.sin(lonSat), rs * omega * math.cos(phi)))
    })

    val sat = Vec3(rs * math.cos(lonSat), rs * math.sin(lonSat), 0.0)
    val near = Ecef.fromGeodetic(math.toRadians(-0.01), 0.0, 0.0)
    val far = Ecef.fromGeodetic(math.toRadians(0.05), 0.0, 0.0)
    val sr0 = (sat - near).norm
    val slantPerGround = ((sat - far).norm - sr0) / (far - near).norm

    val lut = (value: Float) => new Lut2D(Array(0), Array(0), Array(Array(value)))
    val metadata = S1GrdMetadata(
      sceneEpochUtcSecs = 0.0,
      timing = ImageTiming(firstLineUtcSecs = 0.0, lineTimeInterval = 0.0015, numberOfLines = nLines,
        numberOfPixels = nPixels, rangePixelSpacing = 10.0),
      orbit = orbit,
      polarisations = Map(polarisation -> PolarisationMetadata(polarisation, imagePath.toString, lut(1.0f), lut(0.0f),
        new SrgrPolyList(IndexedSeq(SrgrPoly(0.0, sr0, 0.0, Array(sr0, slantPerGround))))))
    )

    TileComputeContext(
      request = TileRequest(extent, cellSize, LatLng, Seq(polarisation), sceneId = "synthetic"),
      metadata = metadata,
      sarSources = Map(polarisation -> GeoTiffRasterSource(imagePath.toString)),
      demItems = Seq(DemItem(demExtent, demPath.toString)),
      geoidSourceFactory = None
    )
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
