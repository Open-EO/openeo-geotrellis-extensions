package org.openeo.geotrellis

import ai.onnxruntime.OrtSession.SessionOptions.ExecutionMode
import ai.onnxruntime.{OnnxTensor, OrtEnvironment, OrtSession}
import geotrellis.layer._
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.OpenEOProcess
import org.slf4j.LoggerFactory

import java.net.URL
import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Scala implementation of WorldCereal seasonal crop-type inference using the Presto
 * model exported to ONNX.
 *
 * == Expected Input Datacube Bands (by index) ==
 *
 * Each SpaceTimeKey tile must carry exactly 15 bands in this order.
 * Cell type must be Float32 (FloatCells).  The nodata sentinel is 65535.
 *
 * {{{
 * Index  Band name (GFMAP)   Description / units
 * -----  -----------------   ----------------------------------------------------
 *   0    S2-L2A-B02          Sentinel-2 Blue,        reflectance × 10 000
 *   1    S2-L2A-B03          Sentinel-2 Green,       reflectance × 10 000
 *   2    S2-L2A-B04          Sentinel-2 Red,         reflectance × 10 000
 *   3    S2-L2A-B05          Sentinel-2 RedEdge1,    reflectance × 10 000
 *   4    S2-L2A-B06          Sentinel-2 RedEdge2,    reflectance × 10 000
 *   5    S2-L2A-B07          Sentinel-2 RedEdge3,    reflectance × 10 000
 *   6    S2-L2A-B08          Sentinel-2 NIR broad,   reflectance × 10 000
 *   7    S2-L2A-B8A          Sentinel-2 NIR narrow,  reflectance × 10 000
 *   8    S2-L2A-B11          Sentinel-2 SWIR1,       reflectance × 10 000
 *   9    S2-L2A-B12          Sentinel-2 SWIR2,       reflectance × 10 000
 *  10    S1-SIGMA0-VV        Sentinel-1 VV backscatter, raw DN (uint16 range 1–65534)
 *  11    S1-SIGMA0-VH        Sentinel-1 VH backscatter, raw DN (uint16 range 1–65534)
 *  12    AGERA5-TMEAN        AgERA5 mean temperature,     in 0.01 K
 *                            (e.g. 27315 represents 273.15 K = 0 °C)
 *  13    AGERA5-PRECIP       AgERA5 total precipitation,  in 0.001 mm/day
 *  14    COP-DEM             Copernicus DEM elevation,    in metres
 * }}}
 *
 * NDVI (required by Presto) is computed internally from bands 6 (B08) and 2 (B04).
 * Slope is not computed; it is set to zero (flat-terrain approximation).
 *
 * == ONNX Model Contract ==
 *
 * The model must accept exactly 5 inputs (queried by position, names are ignored):
 * {{{
 *   #  Name suggestion  Type     Shape           Description
 *   0  x                float32  [B, T, 17]      Normalised feature tensor
 *   1  dynamic_world    int64    [B, T]           Constant 9 ("unknown DW class") per element
 *   2  latlons          float32  [B, 2]           Per-pixel [lat, lon] in WGS-84 degrees
 *   3  mask             int64    [B, T, 17]       1 = nodata/masked, 0 = valid
 *   4  month            int64    [B, T]           0-indexed month of each timestep (0=Jan)
 * }}}
 * where B = rows × cols (pixels per tile) and T = number of timesteps in the group.
 *
 * The expected model output depends on `output_mode` (see [[CroptypeInference.run]]).
 *
 * `"embeddings"` mode — single output (use with `presto_global.onnx`):
 * {{{
 *   #  Name suggestion  Type     Shape    Description
 *   0  embeddings       float32  [B, D]   Global-pooled Presto embedding (D = 128)
 * }}}
 *
 * `"classification"` mode — two outputs (Presto + SeasonalHead fused):
 * {{{
 *   #  Name suggestion     Type     Shape        Description
 *   0  landcover_logits    float32  [B, C_lc]    Landcover class logits (pre-softmax)
 *   1  croptype_logits     float32  [B, C_ct]    Crop-type class logits (pre-softmax)
 * }}}
 * For `"classification"` mode the backbone must be exported with time-pooling and the
 * seasonal head (global mean pooling → linear for landcover; uniform-mask mean pooling
 * → linear for croptype) fused into the same ONNX graph.
 *
 * == Output Bands ==
 *
 * `"classification"` mode (default) — 4 float32 bands:
 * {{{
 *   0  cropland_classification  0.0 = non-cropland, 1.0 = cropland
 *   1  croptype_classification  crop-type argmax class index (0-based);
 *                               [[CroptypeInference.NOCROP_VALUE]] when gated as non-cropland
 *   2  cropland_probability     summed softmax probability of all cropland classes (0.0–1.0)
 *   3  croptype_probability     max softmax probability of predicted crop-type class (0.0–1.0)
 * }}}
 *
 * `"embeddings"` mode — D float32 bands (one per embedding dimension, e.g. 128 for
 * `presto_global.onnx`). Use this mode to inspect Presto representations and validate
 * preprocessing before wiring in a classification head.
 */
object CroptypeInference {

  private val logger = LoggerFactory.getLogger(getClass)

  // ── Public constants ─────────────────────────────────────────────────────────

  /** Sentinel nodata value used throughout the WorldCereal pipeline. */
  val NODATA: Float = 65535f

  /** Number of feature bands the Presto encoder expects. */
  val NUM_PRESTO_BANDS: Int = 17

  /** DynamicWorld "unknown" class index — used as a constant placeholder. */
  val DYNAMIC_WORLD_UNKNOWN: Long = 9L

  /** Written to croptype output for pixels gated as non-cropland. */
  val NOCROP_VALUE: Float = 254f

  // ── Presto normalisation constants ────────────────────────────────────────────
  // Applied as:  normalised = (raw + ADD[i]) / DIV[i]
  // Band order:  VV, VH, B2, B3, B4, B5, B6, B7, B8, B8A, B11, B12,
  //              temperature_2m, total_precipitation, elevation, slope, NDVI

  private val BANDS_ADD: Array[Float] = Array(
    25f,      25f,                                         // VV, VH  (shift dB range)
    0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f,              // S2 reflectance (no offset)
    -272.15f,                                             // temperature K → °C
    0f,                                                   // precipitation
    0f,                                                   // elevation
    0f,                                                   // slope
    0f                                                    // NDVI
  )

  private val BANDS_DIV: Array[Float] = Array(
    25f,      25f,                                         // VV, VH
    1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f,  // S2
    35f,                                                  // temperature (°C range)
    0.03f,                                                // precipitation
    2000f,                                                // elevation
    50f,                                                  // slope
    1f                                                    // NDVI
  )

  // ── Input band indices in the per-timestep MultibandTile ─────────────────────
  // These correspond to the 15 documented input bands.
  private val IN_B2     = 0;  private val IN_B3    = 1;  private val IN_B4    = 2
  private val IN_B5     = 3;  private val IN_B6    = 4;  private val IN_B7    = 5
  private val IN_B8     = 6;  private val IN_B8A   = 7
  private val IN_B11    = 8;  private val IN_B12   = 9
  private val IN_VV     = 10; private val IN_VH    = 11
  private val IN_TEMP   = 12; private val IN_PRECIP = 13
  private val IN_ELEV   = 14

  // ── Presto band indices in the 17-band feature tensor ────────────────────────
  private val P_VV     = 0;  private val P_VH    = 1
  private val P_B2     = 2;  private val P_B3    = 3;  private val P_B4    = 4
  private val P_B5     = 5;  private val P_B6    = 6;  private val P_B7    = 7
  private val P_B8     = 8;  private val P_B8A   = 9
  private val P_B11    = 10; private val P_B12   = 11
  private val P_TEMP   = 12; private val P_PRECIP = 13
  private val P_ELEV   = 14; private val P_SLOPE  = 15; private val P_NDVI  = 16

  // In the companion object:
  private val sessionCache =
    new java.util.concurrent.ConcurrentHashMap[String, OrtSession]()

  Runtime.getRuntime.addShutdownHook(new Thread(() =>
    sessionCache.values().forEach(s => scala.util.Try(s.close()))
  ))

  private def getOrCreateSession(modelPath: String): OrtSession = {
    sessionCache.computeIfAbsent(modelPath, mp => {
      val bytes   = loadModelBytes(mp)
      val env     = OrtEnvironment.getEnvironment()
      val options = new OrtSession.SessionOptions()
      //cpu arena costs more memory
      options.setCPUArenaAllocator(true)
      options.setInterOpNumThreads(3)
      options.setIntraOpNumThreads(1)
      //options.setMemoryPatternOptimization(false)
      options.setExecutionMode(ExecutionMode.PARALLEL)
      //options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT)
      //options.addConfigEntry("session.disable_prepacking", "1")
      //options.addConfigEntry("session.use_ort_model_bytes_directly", "1")
      env.createSession(bytes, options)
    })
  }

  // ── Public entry point ───────────────────────────────────────────────────────

  /**
   * Run WorldCereal seasonal inference over a SpaceTimeKey datacube.
   *
   * @param datacube  SpaceTimeKey datacube with the 15 input bands described in the
   *                  class-level documentation.  Cell type must be FloatCells.
   * @param context   Configuration map.
   *                  Required key:
   *                    "onnx_model_path" (String) — path or URL to the ONNX model file.
   *                  Optional keys:
   *                    "output_mode"            (String,   default "classification")
   *                      — "embeddings"      → return raw ONNX output 0 as D bands per pixel.
   *                                            Use with `presto_global.onnx` for testing.
   *                      — "classification"  → softmax/argmax classification (2-output model)
   *                    "num_landcover_classes"  (Int,      default 4)
   *                    "num_croptype_classes"   (Int,      default 16)
   *                    "cropland_class_indices" (Seq[Int], default Seq(1, 2))
   *                      — landcover class indices considered cropland for gating
   *                    "mask_cropland"          (Boolean,  default true)
   *                      — whether to suppress croptype output on non-cropland pixels
   * @return SpatialKey datacube with output bands depending on output_mode (see class-level docs).
   */
  @OpenEOProcess(
    id          = "croptype_inference",
    description = "Run WorldCereal seasonal crop-type inference using Presto ONNX. " +
                  "Requires 15 input bands (S2, S1, AgERA5, DEM). " +
                  "Returns a SpatialKey datacube with classification or embedding bands.",
    returns     = "datacube"
  )
  def run(
    datacube: MultibandTileLayerRDD[SpaceTimeKey],
    context:  java.util.Map[String, Any]
  ): MultibandTileLayerRDD[SpaceTimeKey] = {

    val scalaContext = context.asScala
    val onnxModelPath    = scalaContext.getOrElse("onnx_model_path", "org/openeo/geotrellis/prometheo/presto_global_shorts.int8.dynamic.onnx").asInstanceOf[String]
    val outputMode       = scalaContext.getOrElse("output_mode", "embeddings").asInstanceOf[String]
    require(outputMode == "classification" || outputMode == "embeddings",
      s"output_mode must be 'classification' or 'embeddings', got: $outputMode")
    val numLcClasses     = scalaContext.getOrElse("num_landcover_classes",  4).asInstanceOf[Int]
    val numCtClasses     = scalaContext.getOrElse("num_croptype_classes",  16).asInstanceOf[Int]
    val maskCropland     = scalaContext.getOrElse("mask_cropland",       true).asInstanceOf[Boolean]
    val croplandClassSet = scalaContext
      .getOrElse("cropland_class_indices", Seq(1, 2))
      .asInstanceOf[Seq[Int]]
      .toSet
    val batchSize = scalaContext.getOrElse("batch_size", 22*22).asInstanceOf[Int]

    val meta   = datacube.metadata
    val layout = meta.layout
    val crs    = meta.crs

    val sc          = SparkContext.getOrCreate()
    val modelPathBC  = sc.broadcast(onnxModelPath)
    val outputModeBC = sc.broadcast(outputMode)
    val crsBC        = sc.broadcast(crs)
    val layoutBC     = sc.broadcast(layout)

    // Callback executed per spatial key on the executors.
    val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpatialKey, MultibandTile] = {
      tiles =>
        val spatialKey = tiles.head._1.spatialKey
        val tileExtent = layoutBC.value.mapTransform(spatialKey)
        val result = inferTile(
          tiles            = tiles,
          tileExtent       = tileExtent,
          crs              = crsBC.value,
          onnxModelPath    = modelPathBC.value,
          outputMode       = outputModeBC.value,
          numLcClasses     = numLcClasses,
          numCtClasses     = numCtClasses,
          maskCropland     = maskCropland,
          croplandClassSet = croplandClassSet,
          batchSize        = batchSize
        )
        Map(spatialKey -> result)
    }

    val processes = new OpenEOProcesses()
    val input =
      if (context.containsKey("tile_size")) {
        val size = context.get("tile_size").asInstanceOf[Int]
        logger.info("CroptypeInference: Retiling datacube to tile_size = " + size)
        processes.retileGeneric(datacube, size, size, 0, 0)
      } else {
        datacube
      }

    val resultRDD: RDD[(SpaceTimeKey, MultibandTile)] = processes.transformTimeDimension[SpatialKey](
      input, applyToTimeseries, reduce = true
    ).map({ case (spatialKey, tile) => (SpaceTimeKey(spatialKey, meta.bounds.get.minKey.temporalKey), tile) })

    val oldBounds = meta.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]]
    val newBounds = KeyBounds(oldBounds.minKey,SpaceTimeKey(oldBounds.maxKey.spatialKey,oldBounds.minKey.temporalKey))
    ContextRDD(
      resultRDD,
      new TileLayerMetadata[SpaceTimeKey](
        cellType = FloatConstantNoDataCellType,
        layout   = layout,
        extent   = meta.extent,
        crs      = crs,
        bounds   = newBounds
      )
    )
  }

  // ── Per-tile inference ───────────────────────────────────────────────────────

  private def inferTile(
    tiles:            Iterable[(SpaceTimeKey, MultibandTile)],
    tileExtent:       Extent,
    crs:              CRS,
    onnxModelPath:    String,
    outputMode:       String,
    numLcClasses:     Int,
    numCtClasses:     Int,
    maskCropland:     Boolean,
    croplandClassSet: Set[Int],
    batchSize:        Int
  ): MultibandTile = {

    val sorted  = sortByTime(tiles)
    val T       = sorted.length
    val refTile = sorted.head._2
    val cols    = refTile.cols
    val rows    = refTile.rows
    val B       = rows * cols

    require(T > 0, "No timesteps found for spatial key")
    require(refTile.bandCount >= 15,
      s"Expected at least 15 input bands, got ${refTile.bandCount}")

    val session     = getOrCreateSession(onnxModelPath)
    val env         = OrtEnvironment.getEnvironment()
    val inputNames  = session.getInputNames.toArray.map(_.asInstanceOf[String])
    val outputNames = session.getOutputNames.toArray.map(_.asInstanceOf[String])

    require(inputNames.length == 5,
      s"ONNX model must have exactly 5 inputs, got ${inputNames.length}: ${inputNames.mkString(", ")}")

    // Pre-compute per-timestep month values (0-indexed) — same for every pixel
    val monthValues = Array.tabulate(T)(t => (sorted(t)._1.time.getMonthValue - 1).toLong)

    // Lat/lon projection parameters
    val wgs84      = CRS.fromEpsgCode(4326)
    val xform      = Transform(crs, wgs84)
    val cellWidth  = tileExtent.width  / cols
    val cellHeight = tileExtent.height / rows

    // Allocate fixed-size direct buffers once; reused across all batches
    val bsz       = math.min(batchSize, B)
    val xBuf      = ByteBuffer.allocateDirect(bsz * T * NUM_PRESTO_BANDS * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val maskBuf   = ByteBuffer.allocateDirect(bsz * T * NUM_PRESTO_BANDS * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val latlonBuf = ByteBuffer.allocateDirect(bsz * 2 * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val monthBuf  = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val dwBuf     = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()

    // DW is constant — fill once for the full buffer capacity
    var i = 0; while (i < bsz * T) { dwBuf.put(i, DYNAMIC_WORLD_UNKNOWN); i += 1 }

    // Accumulate per-output flat results across batches
    val outputAccum = Array.fill(outputNames.length)(new ArrayBuffer[Float]())

    var pStart = 0
    while (pStart < B) {
      val pEnd   = math.min(pStart + bsz, B)
      val batchB = pEnd - pStart

      // ── Fill xBuf and maskBuf ─────────────────────────────────────────────
      // Both filled in a single pass per pixel to avoid reading raster data twice.
      // Mask always written explicitly (0L or 1L) so buffer reuse is safe.
      for (t <- 0 until T) {
        val tile = sorted(t)._2
        var pi   = 0
        while (pi < batchB) {
          val p     = pStart + pi
          val row   = p / cols
          val col   = p % cols
          val base  = (pi * T + t) * NUM_PRESTO_BANDS

          def raw(band: Int): Float = tile.band(band).getDouble(col, row).toFloat

          val rawB2  = raw(IN_B2);  xBuf.put(base + P_B2,  normalizeBand(P_B2,  rawB2));  maskBuf.put(base + P_B2,  if (isNodata(rawB2))  1L else 0L)
          val rawB3  = raw(IN_B3);  xBuf.put(base + P_B3,  normalizeBand(P_B3,  rawB3));  maskBuf.put(base + P_B3,  if (isNodata(rawB3))  1L else 0L)
          val rawB4  = raw(IN_B4);  xBuf.put(base + P_B4,  normalizeBand(P_B4,  rawB4));  maskBuf.put(base + P_B4,  if (isNodata(rawB4))  1L else 0L)
          val rawB5  = raw(IN_B5);  xBuf.put(base + P_B5,  normalizeBand(P_B5,  rawB5));  maskBuf.put(base + P_B5,  if (isNodata(rawB5))  1L else 0L)
          val rawB6  = raw(IN_B6);  xBuf.put(base + P_B6,  normalizeBand(P_B6,  rawB6));  maskBuf.put(base + P_B6,  if (isNodata(rawB6))  1L else 0L)
          val rawB7  = raw(IN_B7);  xBuf.put(base + P_B7,  normalizeBand(P_B7,  rawB7));  maskBuf.put(base + P_B7,  if (isNodata(rawB7))  1L else 0L)
          val rawB8  = raw(IN_B8);  xBuf.put(base + P_B8,  normalizeBand(P_B8,  rawB8));  maskBuf.put(base + P_B8,  if (isNodata(rawB8))  1L else 0L)
          val rawB8A = raw(IN_B8A); xBuf.put(base + P_B8A, normalizeBand(P_B8A, rawB8A)); maskBuf.put(base + P_B8A, if (isNodata(rawB8A)) 1L else 0L)
          val rawB11 = raw(IN_B11); xBuf.put(base + P_B11, normalizeBand(P_B11, rawB11)); maskBuf.put(base + P_B11, if (isNodata(rawB11)) 1L else 0L)
          val rawB12 = raw(IN_B12); xBuf.put(base + P_B12, normalizeBand(P_B12, rawB12)); maskBuf.put(base + P_B12, if (isNodata(rawB12)) 1L else 0L)
          val rawVV  = raw(IN_VV);  xBuf.put(base + P_VV,  normalizeBand(P_VV,  rescaleS1(rawVV)));  maskBuf.put(base + P_VV,  if (isNodata(rawVV))  1L else 0L)
          val rawVH  = raw(IN_VH);  xBuf.put(base + P_VH,  normalizeBand(P_VH,  rescaleS1(rawVH)));  maskBuf.put(base + P_VH,  if (isNodata(rawVH))  1L else 0L)
          val rawTmp = raw(IN_TEMP);   xBuf.put(base + P_TEMP,   normalizeBand(P_TEMP,   rescaleTemperature(rawTmp)));   maskBuf.put(base + P_TEMP,   if (isNodata(rawTmp)) 1L else 0L)
          val rawPrc = raw(IN_PRECIP); xBuf.put(base + P_PRECIP, normalizeBand(P_PRECIP, rescalePrecipitation(rawPrc))); maskBuf.put(base + P_PRECIP, if (isNodata(rawPrc)) 1L else 0L)
          val rawElv = raw(IN_ELEV);   xBuf.put(base + P_ELEV,   normalizeBand(P_ELEV,   rawElv));   maskBuf.put(base + P_ELEV,   if (isNodata(rawElv)) 1L else 0L)
          xBuf.put(base + P_SLOPE, 0f); maskBuf.put(base + P_SLOPE, 0L)
          xBuf.put(base + P_NDVI, computeNdvi(xBuf.get(base + P_B8), xBuf.get(base + P_B4)))
          maskBuf.put(base + P_NDVI, if (isNodata(rawB8) || isNodata(rawB4) || (rawB8 + rawB4) == 0f) 1L else 0L)

          pi += 1
        }

        // Month: same value broadcast to all pixels in this batch for timestep t
        val m = monthValues(t)
        var pi2 = 0; while (pi2 < batchB) { monthBuf.put(pi2 * T + t, m); pi2 += 1 }
      }

      // ── Fill latlonBuf ────────────────────────────────────────────────────
      var pi = 0
      while (pi < batchB) {
        val p    = pStart + pi
        val col  = p % cols
        val row  = p / cols
        val xCtr = tileExtent.xmin + (col + 0.5) * cellWidth
        val yCtr = tileExtent.ymax - (row + 0.5) * cellHeight
        val (lon, lat) = xform(xCtr, yCtr)
        latlonBuf.put(pi * 2,     lat.toFloat)
        latlonBuf.put(pi * 2 + 1, lon.toFloat)
        pi += 1
      }

      // ── Run ONNX for this batch ───────────────────────────────────────────
      // ORT requires buffer.remaining() == shape.product exactly.
      // Set each buffer's limit to the actual element count for this batch
      // before wrapping in OnnxTensor; puts above are already complete.
      xBuf.limit(batchB * T * NUM_PRESTO_BANDS)
      maskBuf.limit(batchB * T * NUM_PRESTO_BANDS)
      latlonBuf.limit(batchB * 2)
      monthBuf.limit(batchB * T)
      dwBuf.limit(batchB * T)

      val xOnnx    = OnnxTensor.createTensor(env, xBuf,      Array[Long](batchB, T, NUM_PRESTO_BANDS))
      val dwOnnx   = OnnxTensor.createTensor(env, dwBuf,     Array[Long](batchB, T))
      val llOnnx   = OnnxTensor.createTensor(env, latlonBuf, Array[Long](batchB, 2))
      val maskOnnx = OnnxTensor.createTensor(env, maskBuf,   Array[Long](batchB, T, NUM_PRESTO_BANDS))
      val monOnnx  = OnnxTensor.createTensor(env, monthBuf,  Array[Long](batchB, T))

      val inputs = java.util.Map.of(
        inputNames(0), xOnnx.asInstanceOf[ai.onnxruntime.OnnxTensorLike],
        inputNames(1), dwOnnx.asInstanceOf[ai.onnxruntime.OnnxTensorLike],
        inputNames(2), llOnnx.asInstanceOf[ai.onnxruntime.OnnxTensorLike],
        inputNames(3), maskOnnx.asInstanceOf[ai.onnxruntime.OnnxTensorLike],
        inputNames(4), monOnnx.asInstanceOf[ai.onnxruntime.OnnxTensorLike]
      )

      val result = session.run(inputs)
      for (oi <- 0 until outputNames.length) {
        val flat = result.get(oi).getValue.asInstanceOf[Array[Array[Float]]].flatten
        outputAccum(oi) ++= flat
      }

      // Restore limits to full capacity so absolute puts in the next batch are unrestricted
      xBuf.limit(xBuf.capacity())
      maskBuf.limit(maskBuf.capacity())
      latlonBuf.limit(latlonBuf.capacity())
      monthBuf.limit(monthBuf.capacity())
      dwBuf.limit(dwBuf.capacity())

      pStart = pEnd
    }

    val outputs = outputAccum.map(_.toArray).toSeq

    outputMode match {
      case "embeddings" =>
        require(outputs.nonEmpty, "ONNX model produced no outputs in embeddings mode")
        buildEmbeddingTile(outputs(0), B, cols, rows)

      case "classification" =>
        require(outputs.length == 2,
          s"ONNX model must produce exactly 2 outputs in classification mode, got ${outputs.length}")
        buildOutputTile(
          lcLogits         = outputs(0),
          ctLogits         = outputs(1),
          cols             = cols,
          rows             = rows,
          numLcClasses     = numLcClasses,
          numCtClasses     = numCtClasses,
          maskCropland     = maskCropland,
          croplandClassSet = croplandClassSet
        )
    }
  }

  // ── Preprocessing steps ──────────────────────────────────────────────────────

  /**
   * Sort tiles in chronological order by their SpaceTimeKey instant.
   *
   * @param tiles  All (key, tile) pairs for one spatial location across time.
   * @return       Sequence sorted ascending by timestamp.
   */
  def sortByTime(
    tiles: Iterable[(SpaceTimeKey, MultibandTile)]
  ): Seq[(SpaceTimeKey, MultibandTile)] =
    tiles.toSeq.sortBy(_._1.instant)

  /**
   * Build the normalised Presto feature tensor of shape [B, T, 17] (flat, row-major).
   *
   * For each pixel ''p'' (row-major index) and timestep ''t'', the 17 Presto bands are
   * assembled from the input MultibandTile and normalised as:
   * {{{
   *   normalised(i) = (raw(i) + BANDS_ADD(i)) / BANDS_DIV(i)
   * }}}
   *
   * Band-level transformations applied before normalisation:
   *  - '''S1 (VV, VH)''': raw uint16 DN → dB via `20 × log₁₀(DN) − 83`.
   *    Pixels where DN == NODATA or DN ≤ 0 remain as NODATA.
   *  - '''S2''': used as-is (reflectance × 10 000).
   *  - '''Temperature''': raw 0.01 K units → K by dividing by 100.
   *  - '''Precipitation''': raw 0.001 mm/day units → m/day by dividing by 100 000.
   *  - '''Elevation''': used as-is (metres).
   *  - '''Slope''': not available as an input band; set to 0 (flat-terrain approximation).
   *  - '''NDVI''': computed from already-normalised B8 and B4 as (B8−B4)/(B8+B4).
   *    Set to 0 when B8+B4 == 0.
   *
   * NODATA pixels are zeroed in the tensor (they are excluded via the mask tensor).
   *
   * @param sortedTiles  Tiles sorted chronologically.
   * @param cols         Tile width in pixels.
   * @param rows         Tile height in pixels.
   * @param T            Number of timesteps.
   * @param B            Total pixels (rows × cols).
   * @return             Flat float32 array of length B × T × 17, layout [B, T, 17].
   */
  def buildBandTensor(
    sortedTiles: Seq[(SpaceTimeKey, MultibandTile)],
    cols:        Int,
    rows:        Int,
    T:           Int,
    B:           Int
  ): java.nio.FloatBuffer = {

    val x = ByteBuffer
      .allocateDirect(B * T * NUM_PRESTO_BANDS * java.lang.Float.BYTES)
      .order(ByteOrder.nativeOrder())
      .asFloatBuffer()

    for (t <- 0 until T) {
      val tile = sortedTiles(t)._2

      for (row <- 0 until rows; col <- 0 until cols) {
        val p    = row * cols + col
        val base = (p * T + t) * NUM_PRESTO_BANDS

        def raw(band: Int): Float = tile.band(band).getDouble(col, row).toFloat

        // S2 — reflectance × 10 000; BANDS_DIV divides by 10 000
        x.put(base + P_B2,  normalizeBand(P_B2,  raw(IN_B2)))
        x.put(base + P_B3,  normalizeBand(P_B3,  raw(IN_B3)))
        x.put(base + P_B4,  normalizeBand(P_B4,  raw(IN_B4)))
        x.put(base + P_B5,  normalizeBand(P_B5,  raw(IN_B5)))
        x.put(base + P_B6,  normalizeBand(P_B6,  raw(IN_B6)))
        x.put(base + P_B7,  normalizeBand(P_B7,  raw(IN_B7)))
        x.put(base + P_B8,  normalizeBand(P_B8,  raw(IN_B8)))
        x.put(base + P_B8A, normalizeBand(P_B8A, raw(IN_B8A)))
        x.put(base + P_B11, normalizeBand(P_B11, raw(IN_B11)))
        x.put(base + P_B12, normalizeBand(P_B12, raw(IN_B12)))

        // S1 — convert raw DN to dB first, then normalise
        x.put(base + P_VV, normalizeBand(P_VV, rescaleS1(raw(IN_VV))))
        x.put(base + P_VH, normalizeBand(P_VH, rescaleS1(raw(IN_VH))))

        // Meteo — scale to physical units first, then normalise
        x.put(base + P_TEMP,   normalizeBand(P_TEMP,   rescaleTemperature(raw(IN_TEMP))))
        x.put(base + P_PRECIP, normalizeBand(P_PRECIP, rescalePrecipitation(raw(IN_PRECIP))))

        // DEM elevation in metres — normalise directly
        x.put(base + P_ELEV,  normalizeBand(P_ELEV, raw(IN_ELEV)))

        // Slope — not available; BANDS_DIV(P_SLOPE)=50 so normalised zero stays zero
        x.put(base + P_SLOPE, 0f)

        // NDVI — derived from already-normalised B8 and B4
        x.put(base + P_NDVI, computeNdvi(x.get(base + P_B8), x.get(base + P_B4)))
      }
    }
    x
  }

  /**
   * Build the nodata mask tensor of shape [B, T, 17] (flat, row-major).
   *
   * Value convention matches Presto: 1 = masked/nodata, 0 = valid.
   * A pixel is masked when its raw value equals [[NODATA]] (65535) or is NaN.
   * NDVI is masked when either B8 or B4 is nodata or their sum is zero.
   * Slope is never masked (we always synthesise a zero value).
   *
   * @param sortedTiles  Tiles sorted chronologically.
   * @param cols         Tile width in pixels.
   * @param rows         Tile height in pixels.
   * @param T            Number of timesteps.
   * @param B            Total pixels (rows × cols).
   * @return             Flat int64 array of length B × T × 17, layout [B, T, 17].
   */
  def buildMaskTensor(
    sortedTiles: Seq[(SpaceTimeKey, MultibandTile)],
    cols:        Int,
    rows:        Int,
    T:           Int,
    B:           Int
  ): java.nio.LongBuffer = {

    val mask = ByteBuffer
      .allocateDirect(B * T * NUM_PRESTO_BANDS * java.lang.Long.BYTES)
      .order(ByteOrder.nativeOrder())
      .asLongBuffer()

    // Mapping from (input band index, Presto band index) for non-derived bands
    val bandPairs = Array(
      (IN_B2, P_B2), (IN_B3, P_B3), (IN_B4, P_B4), (IN_B5, P_B5),
      (IN_B6, P_B6), (IN_B7, P_B7), (IN_B8, P_B8), (IN_B8A, P_B8A),
      (IN_B11, P_B11), (IN_B12, P_B12),
      (IN_VV,  P_VV),  (IN_VH,  P_VH),
      (IN_TEMP, P_TEMP), (IN_PRECIP, P_PRECIP),
      (IN_ELEV, P_ELEV)
    )

    for (t <- 0 until T) {
      val tile = sortedTiles(t)._2

      for (row <- 0 until rows; col <- 0 until cols) {
        val p    = row * cols + col
        val base = (p * T + t) * NUM_PRESTO_BANDS

        for ((inBand, pBand) <- bandPairs) {
          val v = tile.band(inBand).getDouble(col, row).toFloat
          if (isNodata(v)) mask.put(base + pBand, 1L)
        }

        // NDVI masked when either B8 or B4 is nodata or their sum is zero
        val b8 = tile.band(IN_B8).getDouble(col, row).toFloat
        val b4 = tile.band(IN_B4).getDouble(col, row).toFloat
        if (isNodata(b8) || isNodata(b4) || (b8 + b4) == 0f)
          mask.put(base + P_NDVI, 1L)

        // Slope: always valid (synthetic zero)
        mask.put(base + P_SLOPE, 0L)
      }
    }
    mask
  }

  /**
   * Build the DynamicWorld placeholder tensor of shape [B, T] (flat, row-major).
   *
   * Presto was trained with DynamicWorld land-use labels as an auxiliary input.
   * When no DynamicWorld data is available all elements are set to
   * [[DYNAMIC_WORLD_UNKNOWN]] (9), which is the "no-data / unknown" class
   * and is excluded from the attention mask inside the encoder.
   *
   * @return Direct LongBuffer of length B × T, every element equal to 9.
   */
  def buildDynamicWorldTensor(B: Int, T: Int): java.nio.LongBuffer = {
    val buf = ByteBuffer
      .allocateDirect(B * T * java.lang.Long.BYTES)
      .order(ByteOrder.nativeOrder())
      .asLongBuffer()
    var i = 0
    while (i < B * T) { buf.put(i, DYNAMIC_WORLD_UNKNOWN); i += 1 }
    buf
  }

  /**
   * Build the lat/lon tensor of shape [B, 2] (flat, row-major).
   *
   * Pixel centres are derived from the tile extent and reprojected to WGS-84
   * (EPSG:4326).  The result layout per pixel is [latitude, longitude].
   *
   * @param tileExtent  Geographic extent of the tile in the datacube CRS.
   * @param crs         Datacube CRS.
   * @param cols        Tile width in pixels.
   * @param rows        Tile height in pixels.
   * @return            Direct FloatBuffer of length B × 2.
   */
  def buildLatlonTensor(
    tileExtent: Extent,
    crs:        CRS,
    cols:       Int,
    rows:       Int
  ): java.nio.FloatBuffer = {

    val wgs84     = CRS.fromEpsgCode(4326)
    val transform = Transform(crs, wgs84)
    val latlon    = ByteBuffer
      .allocateDirect(rows * cols * 2 * java.lang.Float.BYTES)
      .order(ByteOrder.nativeOrder())
      .asFloatBuffer()

    val cellWidth  = tileExtent.width  / cols
    val cellHeight = tileExtent.height / rows

    for (row <- 0 until rows; col <- 0 until cols) {
      val p    = row * cols + col
      val xCtr = tileExtent.xmin + (col + 0.5) * cellWidth
      val yCtr = tileExtent.ymax - (row + 0.5) * cellHeight
      val (lon, lat) = transform(xCtr, yCtr)   // GeoTrellis Transform returns (lon, lat)
      latlon.put(p * 2,     lat.toFloat)
      latlon.put(p * 2 + 1, lon.toFloat)
    }
    latlon
  }

  /**
   * Build the month tensor of shape [B, T] (flat, row-major).
   *
   * Each element holds the 0-indexed month of the corresponding timestep
   * (0 = January, 11 = December), matching the Presto month-embedding convention.
   * The same month value is broadcast to every pixel within a timestep.
   *
   * @param sortedTiles  Tiles sorted chronologically.
   * @param B            Total pixels (rows × cols).
   * @param T            Number of timesteps.
   * @return             Direct LongBuffer of length B × T.
   */
  def buildMonthTensor(
    sortedTiles: Seq[(SpaceTimeKey, MultibandTile)],
    B:           Int,
    T:           Int
  ): java.nio.LongBuffer = {

    val month = ByteBuffer
      .allocateDirect(B * T * java.lang.Long.BYTES)
      .order(ByteOrder.nativeOrder())
      .asLongBuffer()
    for (t <- 0 until T) {
      val m = (sortedTiles(t)._1.time.getMonthValue - 1).toLong  // 0-indexed
      for (p <- 0 until B) month.put(p * T + t, m)
    }
    month
  }

  // ── Output construction ──────────────────────────────────────────────────────

  /**
   * Build a D-band float32 output tile directly from raw ONNX embeddings.
   *
   * Used in `"embeddings"` mode where the ONNX model (e.g. `presto_global.onnx`)
   * produces a single global-pooled representation of shape [B, D].
   * Each of the D embedding dimensions becomes one output band.
   *
   * @param embeddings  Flat float32 array of length B × D, layout [B, D] row-major.
   * @param B           Total pixels (rows × cols).
   * @param cols        Tile width.
   * @param rows        Tile height.
   * @return            MultibandTile with D float32 bands.
   */
  private def buildEmbeddingTile(
    embeddings: Array[Float],
    B:          Int,
    cols:       Int,
    rows:       Int
  ): MultibandTile = {
    require(embeddings.length % B == 0,
      s"Embeddings length ${embeddings.length} is not divisible by B=$B")
    val D     = embeddings.length / B
    val bands = Array.tabulate(D) { d =>
      val bandData = new Array[Float](B)
      var p = 0
      while (p < B) { bandData(p) = embeddings(p * D + d); p += 1 }
      FloatArrayTile(bandData, cols, rows): Tile
    }
    MultibandTile(bands)
  }

  /**
   * Convert raw logits to a 4-band float32 output tile.
   *
   * For each pixel:
   *  1. Apply softmax to landcover logits → probabilities.
   *  2. Determine argmax class; check whether it belongs to [[croplandClassSet]].
   *  3. Cropland probability = sum of softmax probs for all cropland class indices.
   *  4. If [[maskCropland]] is true and the pixel is non-cropland, write
   *     [[NOCROP_VALUE]] to the crop-type band and 0 to the crop-type probability.
   *  5. Otherwise apply softmax to croptype logits → argmax class + max probability.
   *
   * @param lcLogits         Flat float32 logits for landcover, shape [B × C_lc].
   * @param ctLogits         Flat float32 logits for crop-type, shape [B × C_ct].
   * @param cols             Tile width.
   * @param rows             Tile height.
   * @param numLcClasses     Number of landcover classes (C_lc).
   * @param numCtClasses     Number of crop-type classes (C_ct).
   * @param maskCropland     If true, gate crop-type output using the landcover prediction.
   * @param croplandClassSet Landcover class indices considered cropland for gating.
   * @return                 MultibandTile with 4 float32 bands.
   */
  private def buildOutputTile(
    lcLogits:         Array[Float],
    ctLogits:         Array[Float],
    cols:             Int,
    rows:             Int,
    numLcClasses:     Int,
    numCtClasses:     Int,
    maskCropland:     Boolean,
    croplandClassSet: Set[Int]
  ): MultibandTile = {

    val B = rows * cols
    val croplandClass = new Array[Float](B)
    val croptypeClass = new Array[Float](B)
    val croplandProb  = new Array[Float](B)
    val croptypeProb  = new Array[Float](B)

    for (p <- 0 until B) {

      // ── Landcover ──
      val lcProbs = softmax(lcLogits, p * numLcClasses, numLcClasses)
      val lcPred  = argmax(lcProbs)
      val isCrop  = croplandClassSet.contains(lcPred)

      croplandClass(p) = if (isCrop) 1f else 0f
      croplandProb(p)  = croplandClassSet.foldLeft(0f) { (acc, idx) =>
        if (idx < numLcClasses) acc + lcProbs(idx) else acc
      }

      // ── Crop-type ──
      if (maskCropland && !isCrop) {
        croptypeClass(p) = NOCROP_VALUE
        croptypeProb(p)  = 0f
      } else {
        val ctProbs = softmax(ctLogits, p * numCtClasses, numCtClasses)
        val ctPred  = argmax(ctProbs)
        croptypeClass(p) = ctPred.toFloat
        croptypeProb(p)  = ctProbs(ctPred)
      }
    }

    MultibandTile(
      FloatArrayTile(croplandClass, cols, rows),
      FloatArrayTile(croptypeClass, cols, rows),
      FloatArrayTile(croplandProb,  cols, rows),
      FloatArrayTile(croptypeProb,  cols, rows)
    )
  }

  // ── Band-level preprocessing helpers ─────────────────────────────────────────

  /**
   * Convert raw Sentinel-1 DN (uint16, range 1–65534) to decibels.
   *
   * Formula: dB = 20 × log₁₀(DN) − 83
   *
   * This matches the `rescale_s1_backscatter` step in the Python pipeline, which
   * expands to the same expression after the intermediate linear-power conversion.
   * Returns [[NODATA]] when DN is nodata or non-positive.
   */
  def rescaleS1(rawDn: Float): Float =
    if (isNodata(rawDn) || rawDn <= 0f) NODATA
    else (20f * math.log10(rawDn).toFloat) - 83f

  /**
   * Convert AgERA5 temperature from raw units (0.01 K) to Kelvin.
   * Returns [[NODATA]] unchanged.
   */
  def rescaleTemperature(raw: Float): Float =
    if (isNodata(raw)) NODATA else raw / 100f

  /**
   * Convert AgERA5 precipitation from raw units (0.001 mm/day) to m/day.
   * Returns [[NODATA]] unchanged.
   */
  def rescalePrecipitation(raw: Float): Float =
    if (isNodata(raw)) NODATA else raw / 100000f

  /**
   * Normalise a single Presto band value.
   *
   * Formula: (value + BANDS_ADD[prestoIdx]) / BANDS_DIV[prestoIdx]
   *
   * Returns 0 for nodata / NaN inputs — the value is irrelevant because the
   * corresponding mask tensor entry will be 1 (masked) for such pixels.
   */
  def normalizeBand(prestoIdx: Int, value: Float): Float =
    if (isNodata(value) || value.isNaN) 0f
    else (value + BANDS_ADD(prestoIdx)) / BANDS_DIV(prestoIdx)

  /**
   * Compute NDVI from already-normalised B8 and B4 values.
   * Returns 0 when the sum is zero to avoid division by zero.
   */
  def computeNdvi(b8Norm: Float, b4Norm: Float): Float = {
    val sum = b8Norm + b4Norm
    if (sum == 0f) 0f else (b8Norm - b4Norm) / sum
  }

  // ── Math helpers ─────────────────────────────────────────────────────────────

  /**
   * Numerically stable softmax over a slice of a flat array.
   *
   * @param logits  Source array.
   * @param offset  Starting index of the slice.
   * @param length  Number of elements.
   * @return        New float array of length [[length]] with probabilities summing to 1.
   */
  def softmax(logits: Array[Float], offset: Int, length: Int): Array[Float] = {
    var maxVal = Float.NegativeInfinity
    var i = 0
    while (i < length) { if (logits(offset + i) > maxVal) maxVal = logits(offset + i); i += 1 }
    val exps = new Array[Float](length)
    var sum  = 0f
    i = 0
    while (i < length) {
      exps(i) = math.exp((logits(offset + i) - maxVal).toDouble).toFloat
      sum     += exps(i)
      i += 1
    }
    i = 0
    while (i < length) { exps(i) /= sum; i += 1 }
    exps
  }

  /** Returns the index of the maximum element in an array. */
  def argmax(arr: Array[Float]): Int = {
    var best     = 0
    var bestVal  = arr(0)
    var i        = 1
    while (i < arr.length) { if (arr(i) > bestVal) { bestVal = arr(i); best = i }; i += 1 }
    best
  }

  // ── Internal utilities ───────────────────────────────────────────────────────

  /** True when a pixel value represents nodata or is NaN. */
  private def isNodata(v: Float): Boolean = v == NODATA || v.isNaN

  /**
   * Load an ONNX model as raw bytes from a classpath resource, filesystem path,
   * or HTTP(S) URL — in that order of preference.  Loading via an InputStream
   * means the model can live inside a JAR without needing to be extracted first.
   */
  private def loadModelBytes(model: String): Array[Byte] = {
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream(model)
    if (stream != null) {
      try stream.readAllBytes() finally stream.close()
    } else {
      val path = Paths.get(model)
      if (Files.exists(path)) {
        Files.readAllBytes(path)
      } else {
        new URL(model).openStream() match {
          case s => try s.readAllBytes() finally s.close()
        }
      }
    }
  }
}
