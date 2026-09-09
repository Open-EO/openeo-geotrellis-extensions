package org.openeo.geotrellis

import ai.onnxruntime.{OnnxTensor, OnnxTensorLike, OrtEnvironment, TensorInfo}
import geotrellis.layer._
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.DatacubeSupport.maybeBandLabels
import org.openeo.geotrelliscommon.OpenEOProcess
import org.slf4j.LoggerFactory

import java.nio.{ByteBuffer, ByteOrder}
import java.time.LocalDate
import java.util.Collections
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object CroptypeInference {

  private val logger = LoggerFactory.getLogger(getClass)

  private val NUM_BANDS = 17
  private val DYNAMIC_WORLD_UNKNOWN: Long = 9L

  private val BANDS_ADD: Array[Float] = Array(
    25f, 25f,
    0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f,
    -272.15f,
    0f,
    0f,
    0f,
    0f
  )

  private val BANDS_DIV: Array[Float] = Array(
    25f, 25f,
    1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f, 1e4f,
    35f,
    0.03f,
    2000f,
    50f,
    1f
  )

  private val P_VV      = 0;  private val P_VH      = 1
  private val P_B2      = 2;  private val P_B3      = 3;  private val P_B4      = 4
  private val P_B5      = 5;  private val P_B6      = 6;  private val P_B7      = 7
  private val P_B8      = 8;  private val P_B8A     = 9
  private val P_B11     = 10; private val P_B12     = 11
  private val P_TEMP    = 12; private val P_PRECIP  = 13
  private val P_ELEV    = 14; private val P_SLOPE   = 15; private val P_NDVI    = 16

  /** Resolved input band positions in the source datacube, derived dynamically from band labels. */
  private case class InputBandIndices(
    b2: Int, b3: Int, b4: Int, b5: Int, b6: Int, b7: Int, b8: Int, b8a: Int, b11: Int, b12: Int,
    vv: Int, vh: Int, temp: Int, precip: Int, elev: Int, slope: Int
  )

  @OpenEOProcess(
    id = "croptype_inference",
    description = "Run WorldCereal crop-type ONNX inference over the WorldCereal 15-band datacube.",
    returns = "datacube"
  )
  def run(
    datacube: MultibandTileLayerRDD[SpaceTimeKey],
    args: java.util.Map[String, Any]
  ): MultibandTileLayerRDD[SpaceTimeKey] = {

    val scalaContext = args.getOrDefault("context", Collections.emptyMap()).asInstanceOf[java.util.Map[String,Any]].asScala
    logger.info(s"CroptypeInference: Starting WorldCereal ONNX inference over datacube with ${scalaContext.mkString(",")}" )
    val onnxModelPath = scalaContext
      .getOrElse("onnx_model_path", "org/openeo/geotrellis/worldcereal/worldcereal_seasonal_eu.onnx")
      .asInstanceOf[String]
    val outputEmbeddingsEnabled = scalaContext.getOrElse("output_embeddings", true)
    logger.info(s"CroptypeINference: output_embeddings = $outputEmbeddingsEnabled - ${outputEmbeddingsEnabled.asInstanceOf[Boolean]}")
    val outputEmbeddings = outputEmbeddingsEnabled.asInstanceOf[Boolean]
    val outputProbabilities = true//scalaContext.getOrElse("output_probabilities", true).asInstanceOf[Boolean]
    val outputClassification = scalaContext.getOrElse("output_classification", true).asInstanceOf[Boolean]
    require(outputEmbeddings || outputProbabilities || outputClassification,
      "At least one of output_embeddings, output_probabilities, output_classification must be true")
    // Optional: append one NDVI band per monthly timestep, scaled to fit the uint8 output cube.
    val outputNdvi = scalaContext.getOrElse("output_ndvi", true).asInstanceOf[Boolean]
    val numLcClasses = scalaContext.get("num_landcover_classes").map(_.asInstanceOf[Int])
    val numCtClasses = scalaContext.get("num_croptype_classes").map(_.asInstanceOf[Int])
    val numSeasons = scalaContext.getOrElse("num_seasons", 2).asInstanceOf[Int]
    val seasonWindows = parseSeasonWindows(scalaContext.get("season_windows"))
    val croplandClassSet = scalaContext.getOrElse("cropland_class_indices", Seq(0, 1, 2)).asInstanceOf[Seq[Int]].toSet
    val maskCropland = scalaContext.getOrElse("mask_cropland", true).asInstanceOf[Boolean]
    val batchSize = scalaContext.getOrElse("batch_size", 22 * 22).asInstanceOf[Int]
    // Majority-vote postprocessing (see MajorityVote), enabled by default on both the
    // cropland and croptype classification labels, matching the python reference.
    val majorityVoteEnabled = scalaContext.getOrElse("majority_vote", true).asInstanceOf[Boolean]
    val majorityVoteKernelSize = scalaContext.getOrElse("majority_vote_kernel_size", 5).asInstanceOf[Int]
    val majorityVoteCropland = scalaContext.getOrElse("majority_vote_cropland", true).asInstanceOf[Boolean]
    val majorityVoteCroptype = scalaContext.getOrElse("majority_vote_croptype", true).asInstanceOf[Boolean]

    val meta   = datacube.metadata
    val layout = meta.layout
    val crs    = meta.crs
    val inputBandIndices = resolveInputBandIndices(datacube)

    val sc             = SparkContext.getOrCreate()
    val modelPathBC    = sc.broadcast(onnxModelPath)
    val outputEmbeddingsBC = sc.broadcast(outputEmbeddings)
    val outputProbabilitiesBC = sc.broadcast(outputProbabilities)
    val outputClassificationBC = sc.broadcast(outputClassification)
    val outputNdviBC = sc.broadcast(outputNdvi)
    val crsBC          = sc.broadcast(crs)
    val layoutBC       = sc.broadcast(layout)
    val numSeasonsBC   = sc.broadcast(numSeasons)
    val seasonWindowsBC = sc.broadcast(seasonWindows)
    val numLcClassesBC = sc.broadcast(numLcClasses)
    val numCtClassesBC = sc.broadcast(numCtClasses)
    val inputBandIndicesBC = sc.broadcast(inputBandIndices)
    val majorityVoteEnabledBC = sc.broadcast(majorityVoteEnabled)
    val majorityVoteKernelSizeBC = sc.broadcast(majorityVoteKernelSize)
    val majorityVoteCroplandBC = sc.broadcast(majorityVoteCropland)
    val majorityVoteCroptypeBC = sc.broadcast(majorityVoteCroptype)

    val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpatialKey, MultibandTile] = {
      tiles =>
        val spatialKey = tiles.head._1.spatialKey
        val tileExtent = layoutBC.value.mapTransform(spatialKey)
        val result = inferTile(
          tiles = tiles,
          tileExtent = tileExtent,
          crs = crsBC.value,
          onnxModelPath = modelPathBC.value,
          outputEmbeddings = outputEmbeddingsBC.value,
          outputProbabilities = outputProbabilitiesBC.value,
          outputClassification = outputClassificationBC.value,
          outputNdvi = outputNdviBC.value,
          numLcClassesOverride = numLcClassesBC.value,
          numCtClassesOverride = numCtClassesBC.value,
          numSeasons = numSeasonsBC.value,
          seasonWindows = seasonWindowsBC.value,
          maskCropland = maskCropland,
          croplandClassSet = croplandClassSet,
          batchSize = batchSize,
          inputBandIndices = inputBandIndicesBC.value,
          majorityVoteEnabled = majorityVoteEnabledBC.value,
          majorityVoteKernelSize = majorityVoteKernelSizeBC.value,
          majorityVoteCropland = majorityVoteCroplandBC.value,
          majorityVoteCroptype = majorityVoteCroptypeBC.value
        )
        Map(spatialKey -> result)
    }

    val processes = new OpenEOProcesses()
    val input =
      if (scalaContext.contains("tile_size")) {
        val size = scalaContext("tile_size").asInstanceOf[Int]
        logger.info("CroptypeInference: Retiling datacube to tile_size = " + size)
        processes.retileGeneric(datacube, size, size, 0, 0)
      } else {
        datacube
      }

    val resultRDD: RDD[(SpaceTimeKey, MultibandTile)] = processes.transformTimeDimension[SpatialKey](
      input, applyToTimeseries, reduce = false
    ).map({ case (spatialKey, tile) => (SpaceTimeKey(spatialKey, meta.bounds.get.minKey.temporalKey), tile) })

    val oldBounds = meta.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]]
    val newBounds = KeyBounds(oldBounds.minKey, SpaceTimeKey(oldBounds.maxKey.spatialKey, oldBounds.minKey.temporalKey))
    ContextRDD(
      resultRDD,
      new TileLayerMetadata[SpaceTimeKey](
        cellType = UByteCellType,
        layout = layout,
        extent = meta.extent,
        crs = crs,
        bounds = newBounds
      )
    )
  }

  private def inferTile(
    tiles:               Iterable[(SpaceTimeKey, MultibandTile)],
    tileExtent:          Extent,
    crs:                 CRS,
    onnxModelPath:       String,
    outputEmbeddings:    Boolean,
    outputProbabilities: Boolean,
    outputClassification: Boolean,
    outputNdvi:          Boolean,
    numLcClassesOverride: Option[Int],
    numCtClassesOverride: Option[Int],
    numSeasons:          Int,
    seasonWindows:       Seq[(LocalDate, LocalDate)],
    maskCropland:        Boolean,
    croplandClassSet:    Set[Int],
    batchSize:           Int,
    inputBandIndices:    InputBandIndices,
    majorityVoteEnabled:    Boolean,
    majorityVoteKernelSize: Int,
    majorityVoteCropland:   Boolean,
    majorityVoteCroptype:   Boolean
  ): MultibandTile = {

    val sorted  = OnnxInferenceUtils.sortByTime(tiles)
    val T       = sorted.length
    val refTile = sorted.head._2
    val cols    = refTile.cols
    val rows    = refTile.rows
    val B       = rows * cols

    require(T > 0, "No timesteps found for spatial key")
    require(refTile.bandCount >= 15, s"Expected at least 15 input bands, got ${refTile.bandCount}")

    val session    = OnnxInferenceUtils.getOrCreateSession(onnxModelPath)
    val env        = OrtEnvironment.getEnvironment()

    val seasonPattern = buildSeasonPattern(sorted, seasonWindows, numSeasons)
    val S = seasonPattern.length

    val monthValues = Array.tabulate(T)(t => (sorted(t)._1.time.getMonthValue - 1).toLong)

    val wgs84      = CRS.fromEpsgCode(4326)
    val xform      = Transform(crs, wgs84)
    val cellWidth  = tileExtent.width / cols
    val cellHeight = tileExtent.height / rows

    val bsz       = math.min(batchSize, B)
    val xBuf      = ByteBuffer.allocateDirect(bsz * T * NUM_BANDS * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val maskBuf   = ByteBuffer.allocateDirect(bsz * T * NUM_BANDS * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val latlonBuf = ByteBuffer.allocateDirect(bsz * 2 * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val monthBuf  = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val dwBuf     = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()

    var i = 0
    while (i < bsz * T) { dwBuf.put(i, DYNAMIC_WORLD_UNKNOWN); i += 1 }

    val embeddingAccum = new ArrayBuffer[Float]()
    val landcoverAccum = new ArrayBuffer[Float]()
    val croptypeAccum  = new ArrayBuffer[Float]()
    // One (scaled) NDVI value per pixel per monthly timestep, indexed as p * T + t.
    val ndviAccum: Array[Float] = if (outputNdvi) new Array[Float](B * T) else null

    var detectedLcClasses = numLcClassesOverride.getOrElse(-1)
    var detectedCtClasses = numCtClassesOverride.getOrElse(-1)

    var pStart = 0
    while (pStart < B) {
      val pEnd   = math.min(pStart + bsz, B)
      val batchB = pEnd - pStart

      for (t <- 0 until T) {
        val tile = sorted(t)._2
        var pi   = 0
        while (pi < batchB) {
          val p    = pStart + pi
          val row  = p / cols
          val col  = p % cols
          val base = (pi * T + t) * NUM_BANDS

          def raw(band: Int): Float = if (band >= 0) tile.band(band).getDouble(col, row).toFloat else Float.NaN

          val rawB2  = raw(inputBandIndices.b2);  xBuf.put(base + P_B2, normalizeBand(P_B2, rawB2)); maskBuf.put(base + P_B2, if (OnnxInferenceUtils.isNodata(rawB2)) 1L else 0L)
          val rawB3  = raw(inputBandIndices.b3);  xBuf.put(base + P_B3, normalizeBand(P_B3, rawB3)); maskBuf.put(base + P_B3, if (OnnxInferenceUtils.isNodata(rawB3)) 1L else 0L)
          val rawB4  = raw(inputBandIndices.b4);  xBuf.put(base + P_B4, normalizeBand(P_B4, rawB4)); maskBuf.put(base + P_B4, if (OnnxInferenceUtils.isNodata(rawB4)) 1L else 0L)
          val rawB5  = raw(inputBandIndices.b5);  xBuf.put(base + P_B5, normalizeBand(P_B5, rawB5)); maskBuf.put(base + P_B5, if (OnnxInferenceUtils.isNodata(rawB5)) 1L else 0L)
          val rawB6  = raw(inputBandIndices.b6);  xBuf.put(base + P_B6, normalizeBand(P_B6, rawB6)); maskBuf.put(base + P_B6, if (OnnxInferenceUtils.isNodata(rawB6)) 1L else 0L)
          val rawB7  = raw(inputBandIndices.b7);  xBuf.put(base + P_B7, normalizeBand(P_B7, rawB7)); maskBuf.put(base + P_B7, if (OnnxInferenceUtils.isNodata(rawB7)) 1L else 0L)
          val rawB8  = raw(inputBandIndices.b8);  xBuf.put(base + P_B8, normalizeBand(P_B8, rawB8)); maskBuf.put(base + P_B8, if (OnnxInferenceUtils.isNodata(rawB8)) 1L else 0L)
          val rawB8A = raw(inputBandIndices.b8a); xBuf.put(base + P_B8A, normalizeBand(P_B8A, rawB8A)); maskBuf.put(base + P_B8A, if (OnnxInferenceUtils.isNodata(rawB8A)) 1L else 0L)
          val rawB11 = raw(inputBandIndices.b11); xBuf.put(base + P_B11, normalizeBand(P_B11, rawB11)); maskBuf.put(base + P_B11, if (OnnxInferenceUtils.isNodata(rawB11)) 1L else 0L)
          val rawB12 = raw(inputBandIndices.b12); xBuf.put(base + P_B12, normalizeBand(P_B12, rawB12)); maskBuf.put(base + P_B12, if (OnnxInferenceUtils.isNodata(rawB12)) 1L else 0L)
          val rawVV  = raw(inputBandIndices.vv);  xBuf.put(base + P_VV, normalizeBand(P_VV, OnnxInferenceUtils.rescaleS1(rawVV))); maskBuf.put(base + P_VV, if (OnnxInferenceUtils.isNodata(rawVV)) 1L else 0L)
          val rawVH  = raw(inputBandIndices.vh);  xBuf.put(base + P_VH, normalizeBand(P_VH, OnnxInferenceUtils.rescaleS1(rawVH))); maskBuf.put(base + P_VH, if (OnnxInferenceUtils.isNodata(rawVH)) 1L else 0L)
          val rawTmp = raw(inputBandIndices.temp); xBuf.put(base + P_TEMP, normalizeBand(P_TEMP, OnnxInferenceUtils.rescaleTemperature(rawTmp))); maskBuf.put(base + P_TEMP, if (OnnxInferenceUtils.isNodata(rawTmp)) 1L else 0L)
          val rawPrc = raw(inputBandIndices.precip); xBuf.put(base + P_PRECIP, normalizeBand(P_PRECIP, OnnxInferenceUtils.rescalePrecipitation(rawPrc))); maskBuf.put(base + P_PRECIP, if (OnnxInferenceUtils.isNodata(rawPrc)) 1L else 0L)
          val rawElv = raw(inputBandIndices.elev); xBuf.put(base + P_ELEV, normalizeBand(P_ELEV, rawElv)); maskBuf.put(base + P_ELEV, if (OnnxInferenceUtils.isNodata(rawElv)) 1L else 0L)
          val rawSlope = raw(inputBandIndices.slope); xBuf.put(base + P_SLOPE, normalizeBand(P_SLOPE,rawSlope)); maskBuf.put(base + P_SLOPE, if (OnnxInferenceUtils.isNodata(rawSlope)) 1L else 0L)
          xBuf.put(base + P_NDVI, computeNdvi(xBuf.get(base + P_B8), xBuf.get(base + P_B4)))
          maskBuf.put(base + P_NDVI, if (OnnxInferenceUtils.isNodata(rawB8) || OnnxInferenceUtils.isNodata(rawB4) || (rawB8 + rawB4) == 0f) 1L else 0L)
          if (outputNdvi) ndviAccum(p * T + t) = scaleNdviToByte(xBuf.get(base + P_NDVI))

          pi += 1
        }

        val m = monthValues(t)
        var pi2 = 0
        while (pi2 < batchB) { monthBuf.put(pi2 * T + t, m); pi2 += 1 }
      }

      var pi = 0
      while (pi < batchB) {
        val p    = pStart + pi
        val col  = p % cols
        val row  = p / cols
        val xCtr = tileExtent.xmin + (col + 0.5) * cellWidth
        val yCtr = tileExtent.ymax - (row + 0.5) * cellHeight
        val (lon, lat) = xform(xCtr, yCtr)
        latlonBuf.put(pi * 2, lat.toFloat)
        latlonBuf.put(pi * 2 + 1, lon.toFloat)
        pi += 1
      }

      xBuf.limit(batchB * T * NUM_BANDS)
      maskBuf.limit(batchB * T * NUM_BANDS)
      latlonBuf.limit(batchB * 2)
      monthBuf.limit(batchB * T)
      dwBuf.limit(batchB * T)

      val xOnnx    = OnnxTensor.createTensor(env, xBuf, Array[Long](batchB, T, NUM_BANDS))
      val dwOnnx   = OnnxTensor.createTensor(env, dwBuf, Array[Long](batchB, T))
      val llOnnx   = OnnxTensor.createTensor(env, latlonBuf, Array[Long](batchB, 2))
      val maskOnnx = OnnxTensor.createTensor(env, maskBuf, Array[Long](batchB, T, NUM_BANDS))
      val monOnnx  = OnnxTensor.createTensor(env, monthBuf, Array[Long](batchB, T))
      val smArray  = Array.tabulate(batchB, S, T) { (_, s, t) => seasonPattern(s)(t) }
      val smOnnx   = OnnxTensor.createTensor(env, smArray)
      val inputs: java.util.Map[String, OnnxTensorLike] = java.util.Map.of(
        "x", xOnnx.asInstanceOf[OnnxTensorLike],
        "dynamic_world", dwOnnx.asInstanceOf[OnnxTensorLike],
        "latlons", llOnnx.asInstanceOf[OnnxTensorLike],
        "mask", maskOnnx.asInstanceOf[OnnxTensorLike],
        "month", monOnnx.asInstanceOf[OnnxTensorLike],
        "season_masks", smOnnx.asInstanceOf[OnnxTensorLike]
      )

      val result = session.run(inputs)
      try {
        if (outputEmbeddings) {
          embeddingAccum ++= flatten2d(result.get(0).getValue)
        }
        if (outputProbabilities || outputClassification) {
          // Auto-detect class counts from ONNX output shapes on first batch
          if (detectedLcClasses < 0) {
            val lcShape = result.get(2).getInfo.asInstanceOf[TensorInfo].getShape
            detectedLcClasses = lcShape.last.toInt
            logger.info(s"CroptypeInference: detected $detectedLcClasses landcover classes from output shape ${lcShape.mkString("[", ",", "]")}")
          }
          if (detectedCtClasses < 0) {
            val ctShape = result.get(3).getInfo.asInstanceOf[TensorInfo].getShape
            detectedCtClasses = ctShape.last.toInt
            logger.info(s"CroptypeInference: detected $detectedCtClasses croptype classes from output shape ${ctShape.mkString("[", ",", "]")}")
          }
          landcoverAccum ++= flatten2d(result.get(2).getValue)
          croptypeAccum ++= flattenCroptype(result.get(3).getValue, batchB, numSeasons, detectedCtClasses)
        }
      } finally {
        result.close()
        xOnnx.close()
        dwOnnx.close()
        llOnnx.close()
        maskOnnx.close()
        monOnnx.close()
        smOnnx.close()
      }

      xBuf.limit(xBuf.capacity())
      maskBuf.limit(maskBuf.capacity())
      latlonBuf.limit(latlonBuf.capacity())
      monthBuf.limit(monthBuf.capacity())
      dwBuf.limit(dwBuf.capacity())

      pStart = pEnd
    }

    val outputTiles = new ArrayBuffer[Tile]()
    if (outputEmbeddings) {
      outputTiles ++= OnnxInferenceUtils.buildQuantizedEmbeddingTile(embeddingAccum.toArray, B, cols, rows).bands
      logger.info(s"CroptypeInference: added embeddings ${outputTiles.length} ")
    }
    if (outputProbabilities) {
      outputTiles ++= buildProbabilityTile(landcoverAccum.toArray, croptypeAccum.toArray, cols, rows,
        detectedLcClasses, detectedCtClasses, numSeasons).bands
      logger.info(s"CroptypeInference: added probabilities ${outputTiles.length} for ${numSeasons} seasons.")
    }
    if (outputClassification) {
      outputTiles ++= buildClassificationTileFromProbs(
        lcProbs = landcoverAccum.toArray,
        ctProbs = croptypeAccum.toArray,
        cols = cols,
        rows = rows,
        numLcClasses = detectedLcClasses,
        numCtClasses = detectedCtClasses,
        numSeasons = numSeasons,
        maskCropland = maskCropland,
        croplandClassSet = croplandClassSet,
        majorityVoteEnabled = majorityVoteEnabled,
        majorityVoteKernelSize = majorityVoteKernelSize,
        majorityVoteCropland = majorityVoteCropland,
        majorityVoteCroptype = majorityVoteCroptype
      ).bands
    }
    if (outputNdvi) {
      outputTiles ++= buildNdviTiles(ndviAccum, cols, rows, T)
      logger.info(s"CroptypeInference: added ndvi ${T} monthly bands.")
    }
    logger.info(s"CroptypeInference: Finished for tile at extent $tileExtent, output bands: ${outputTiles.length} outputEmbeddings=$outputEmbeddings, outputProbabilities=$outputProbabilities, outputClassification=$outputClassification")
    MultibandTile(outputTiles.toSeq).convert(UByteCellType)
  }

  /**
   * Build a season pattern of shape [S, T] indicating which timesteps belong to each season.
   * If no season windows are provided, returns uniform all-true masks for numSeasons seasons.
   */
  private def buildSeasonPattern(
    sortedTiles:   Seq[(SpaceTimeKey, MultibandTile)],
    seasonWindows: Seq[(LocalDate, LocalDate)],
    numSeasons:    Int
  ): Array[Array[Boolean]] = {
    val T = sortedTiles.length
    if (seasonWindows.isEmpty) {
      Array.fill(numSeasons)(Array.fill(T)(true))
    } else {
      seasonWindows.map { case (start, end) =>
        Array.tabulate(T) { t =>
          val date = sortedTiles(t)._1.time.toLocalDate
          !date.isBefore(start) && !date.isAfter(end)
        }
      }.toArray
    }
  }

  private def normalizeBand(prestoIdx: Int, value: Float): Float =
    if (OnnxInferenceUtils.isNodata(value) || value.isNaN) 0f
    else (value + BANDS_ADD(prestoIdx)) / BANDS_DIV(prestoIdx)

  private def computeNdvi(b8Norm: Float, b4Norm: Float): Float = {
    val sum = b8Norm + b4Norm
    if (sum == 0f) 0f else (b8Norm - b4Norm) / sum
  }

  /**
   * Clip NDVI to [-0.08, 0.92] and rescale to a [0, 250] integral value suitable for a
   * uint8 output cube: ndvi_scaled = (clip(ndvi, -0.08, 0.92) + 0.08) / 0.004.
   */
  private def scaleNdviToByte(ndvi: Float): Float = {
    val clipped = math.max(-0.08f, math.min(0.92f, ndvi))
    math.round((clipped + 0.08f) / 0.004f).toFloat
  }

  /** Build one band per monthly timestep from a [B * T] (pixel-major) NDVI accumulator. */
  private def buildNdviTiles(ndviAccum: Array[Float], cols: Int, rows: Int, T: Int): Seq[Tile] = {
    val B = cols * rows
    (0 until T).map { t =>
      val bandData = new Array[Float](B)
      var p = 0
      while (p < B) {
        bandData(p) = ndviAccum(p * T + t)
        p += 1
      }
      FloatArrayTile(bandData, cols, rows): Tile
    }
  }

  /** Scale a [0,1] probability to a [0,100] integral value suitable for a uint8 cube. */
  private def scaleProbabilityToByte(prob: Float): Float =
    math.round(math.max(0f, math.min(1f, prob)) * 100f).toFloat

  /**
   * Output all raw probability values as bands for inspection, scaled from [0,1] to [0,100]
   * so the result fits a uint8 output cube.
   * Bands: [lc_0 .. lc_N, ct_s0_0 .. ct_s0_M, ct_s1_0 .. ct_s1_M, ...]
   */
  private def buildProbabilityTile(
    lcProbs:      Array[Float],
    ctProbs:      Array[Float],
    cols:         Int,
    rows:         Int,
    numLcClasses: Int,
    numCtClasses: Int,
    numSeasons:   Int
  ): MultibandTile = {
    val B = rows * cols
    val totalBands = numLcClasses + numSeasons * numCtClasses
    val bands = Array.tabulate(totalBands) { band =>
      val data = new Array[Float](B)
      for (p <- 0 until B) {
        val prob = if (band < numLcClasses) {
          lcProbs(p * numLcClasses + band)
        } else {
          val ctBand = band - numLcClasses
          ctProbs(p * numSeasons * numCtClasses + ctBand)
        }
        data(p) = scaleProbabilityToByte(prob)
      }
      FloatArrayTile(data, cols, rows): Tile
    }
    MultibandTile(bands)
  }

  /**
   * Parse season windows from context.
   * Accepts:
   *  - A java.util.List of java.util.Map with "start" and "end" keys (ISO date strings)
   *  - A java.util.Map of season_id -> java.util.List[String] with [start, end] date strings
   *  - A java.util.List of "ID:START:END" strings
   */
  def parseSeasonWindows(raw: Option[Any]): Seq[(LocalDate, LocalDate)] = {
    raw match {
      case None => Seq.empty
      case Some(list: java.util.List[_]) =>
        list.asScala.toSeq.map {
          case map: java.util.Map[_, _] =>
            val m = map.asInstanceOf[java.util.Map[String, String]].asScala
            (LocalDate.parse(m("start")), LocalDate.parse(m("end")))
          case str: String =>
            val parts = str.split(":").map(_.trim)
            require(parts.length == 3, s"Expected ID:START:END format, got '$str'")
            (LocalDate.parse(parts(1)), LocalDate.parse(parts(2)))
          case entry: java.util.List[_] =>
            val items = entry.asScala.map(_.toString)
            require(items.length >= 2, s"Expected [start, end] date list")
            (LocalDate.parse(items(0)), LocalDate.parse(items(1)))
          case other =>
            throw new IllegalArgumentException(s"Unexpected season window entry: $other")
        }
      case Some(map: java.util.Map[_, _]) =>
        map.asScala.toSeq.map { case (_, v) =>
          val dates = v.asInstanceOf[java.util.List[String]].asScala
          require(dates.length >= 2, "Each season window needs start and end dates")
          (LocalDate.parse(dates(0)), LocalDate.parse(dates(1)))
        }
      case Some(other) =>
        throw new IllegalArgumentException(s"Unexpected season_windows type: ${other.getClass}")
    }
  }

  private def buildClassificationTileFromProbs(
    lcProbs:          Array[Float],
    ctProbs:          Array[Float],
    cols:             Int,
    rows:             Int,
    numLcClasses:     Int,
    numCtClasses:     Int,
    numSeasons:       Int,
    maskCropland:     Boolean,
    croplandClassSet: Set[Int],
    majorityVoteEnabled:    Boolean = true,
    majorityVoteKernelSize: Int = 5,
    majorityVoteCropland:   Boolean = true,
    majorityVoteCroptype:   Boolean = true
  ): MultibandTile = {

    val B = rows * cols
    val croplandClass = new Array[Float](B)
    val croplandProb  = new Array[Float](B)
    val croptypeClassPerSeason = Array.fill(numSeasons)(new Array[Float](B))
    val croptypeProbPerSeason  = Array.fill(numSeasons)(new Array[Float](B))

    for (p <- 0 until B) {
      val lcOffset = p * numLcClasses
      val lcSlice = java.util.Arrays.copyOfRange(lcProbs, lcOffset, lcOffset + numLcClasses)
      val lcPred = OnnxInferenceUtils.argmax(lcSlice)
      //println(s"Pixel $p: lcPred = $lcPred, lcSlice = ${lcSlice.mkString("[", ",", "]")}")
      val isCrop = croplandClassSet.contains(lcPred)

      croplandClass(p) = if (isCrop) 1f else 0f
      croplandProb(p) = scaleProbabilityToByte(croplandClassSet.foldLeft(0f) { (acc, idx) =>
        if (idx < numLcClasses) acc + lcSlice(idx) else acc
      })

      var s = 0
      while (s < numSeasons) {
        if (maskCropland && !isCrop ) {
          croptypeClassPerSeason(s)(p) = OnnxInferenceUtils.NOCROP_VALUE
          croptypeProbPerSeason(s)(p) = 0f
        } else {
          val ctOffset = (p * numSeasons + s) * numCtClasses
          val ctSlice = java.util.Arrays.copyOfRange(ctProbs, ctOffset, ctOffset + numCtClasses)
          val ctPred = OnnxInferenceUtils.argmax(ctSlice)
          croptypeClassPerSeason(s)(p) = ctPred.toFloat
          croptypeProbPerSeason(s)(p) = scaleProbabilityToByte(ctSlice(ctPred))
        }
        s += 1
      }
    }

    val croplandClassTile: Tile =
      if (majorityVoteEnabled && majorityVoteCropland)
        MajorityVote(FloatArrayTile(croplandClass, cols, rows), majorityVoteKernelSize, Set.empty[Int])
      else
        FloatArrayTile(croplandClass, cols, rows)

    // Croptype labels exclude the "no crop" sentinel from voting, matching the python
    // reference's POSTPROCESSING_EXCLUDED_VALUES handling for croptype postprocessing.
    val croptypeExcludedValues = Set(OnnxInferenceUtils.NOCROP_VALUE.toInt)
    val seasonBands = Array.tabulate(numSeasons) { s =>
      val croptypeClassTile: Tile =
        if (majorityVoteEnabled && majorityVoteCroptype)
          MajorityVote(FloatArrayTile(croptypeClassPerSeason(s), cols, rows), majorityVoteKernelSize, croptypeExcludedValues)
        else
          FloatArrayTile(croptypeClassPerSeason(s), cols, rows)
      Array[Tile](
        croptypeClassTile,
        FloatArrayTile(croptypeProbPerSeason(s), cols, rows): Tile
      )
    }.flatten


    MultibandTile(
      (Array[Tile](
        croplandClassTile,
        FloatArrayTile(croplandProb, cols, rows): Tile
      ) ++ seasonBands): _*
    )
  }

  private def flatten2d(value: Any): Array[Float] = value match {
    case arr: Array[Array[Float]] => arr.flatten
    case arr: Array[Float] => arr
    case other =>
      throw new IllegalArgumentException(s"Expected 1D/2D float output, got ${other.getClass.getName}")
  }

  private def flattenCroptype(value: Any, batchB: Int, numSeasons: Int, numCtClasses: Int): Array[Float] = value match {
    case arr: Array[Array[Array[Float]]] =>
      require(arr.length == batchB, s"Expected $batchB croptype batches, got ${arr.length}")
      val out = new Array[Float](batchB * numSeasons * numCtClasses)
      var p = 0
      while (p < batchB) {
        require(arr(p).length >= numSeasons, s"Expected at least $numSeasons seasons, got ${arr(p).length}")
        var s = 0
        while (s < numSeasons) {
          require(arr(p)(s).length == numCtClasses, s"Expected $numCtClasses croptype classes, got ${arr(p)(s).length}")
          System.arraycopy(arr(p)(s), 0, out, (p * numSeasons + s) * numCtClasses, numCtClasses)
          s += 1
        }
        p += 1
      }
      out
    case arr: Array[Array[Float]] =>
      require(numSeasons == 1, s"Got 2D croptype output but numSeasons=$numSeasons")
      arr.flatten
    case other =>
      throw new IllegalArgumentException(s"Expected 2D/3D croptype output, got ${other.getClass.getName}")
  }

  /**
   * Resolve dynamic input band indices from the datacube's band labels, so inference
   * doesn't depend on a fixed input band ordering.
   */
  private def resolveInputBandIndices(datacube: MultibandTileLayerRDD[SpaceTimeKey]): InputBandIndices = {
    val labels = maybeBandLabels(datacube)
      .getOrElse(throw new IllegalArgumentException("Missing band labels in datacube metadata for croptype_inference"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .toArray

    val byLabel = labels.zipWithIndex.toMap

    def idx(name: String): Int =
      byLabel.getOrElse(name, throw new IllegalArgumentException(
        s"Required band label '$name' not found. Available labels: ${labels.mkString(",")}"
      ))

    InputBandIndices(
      b2     = idx("S2-L2A-B02"),
      b3     = idx("S2-L2A-B03"),
      b4     = idx("S2-L2A-B04"),
      b5     = idx("S2-L2A-B05"),
      b6     = idx("S2-L2A-B06"),
      b7     = idx("S2-L2A-B07"),
      b8     = idx("S2-L2A-B08"),
      b8a    = -1,//idx("S2-L2A-B8A"),
      b11    = idx("S2-L2A-B11"),
      b12    = idx("S2-L2A-B12"),
      vv     = idx("S1-SIGMA0-VV"),
      vh     = idx("S1-SIGMA0-VH"),
      slope  = idx("slope"),
      elev   = idx("elevation"),
      precip = idx("AGERA5-PRECIP"),
      temp   = idx("AGERA5-TMEAN")
    )
  }
}
