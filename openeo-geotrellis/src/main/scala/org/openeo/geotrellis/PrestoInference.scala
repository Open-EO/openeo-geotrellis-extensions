package org.openeo.geotrellis

import ai.onnxruntime.{OnnxTensor, OnnxTensorLike, OrtEnvironment}
import geotrellis.layer._
import geotrellis.proj4.{CRS, Transform}
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrelliscommon.OpenEOProcess
import org.slf4j.LoggerFactory

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object PrestoInference {

  private val logger = LoggerFactory.getLogger(getClass)

  val NUM_PRESTO_BANDS: Int = 17
  val DYNAMIC_WORLD_UNKNOWN: Long = 9L

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

  private val IN_B2     = 0;  private val IN_B3     = 1;  private val IN_B4     = 2
  private val IN_B5     = 3;  private val IN_B6     = 4;  private val IN_B7     = 5
  private val IN_B8     = 6;  private val IN_B8A    = 7
  private val IN_B11    = 8;  private val IN_B12    = 9
  private val IN_VV     = 10; private val IN_VH     = 11
  private val IN_TEMP   = 12; private val IN_PRECIP = 13
  private val IN_ELEV   = 14

  private val P_VV      = 0;  private val P_VH      = 1
  private val P_B2      = 2;  private val P_B3      = 3;  private val P_B4      = 4
  private val P_B5      = 5;  private val P_B6      = 6;  private val P_B7      = 7
  private val P_B8      = 8;  private val P_B8A     = 9
  private val P_B11     = 10; private val P_B12     = 11
  private val P_TEMP    = 12; private val P_PRECIP  = 13
  private val P_ELEV    = 14; private val P_SLOPE   = 15; private val P_NDVI    = 16

  @OpenEOProcess(
    id = "presto_inference",
    description = "Run Presto ONNX inference over the WorldCereal 15-band datacube and return embeddings.",
    returns = "datacube"
  )
  def run(
    datacube: MultibandTileLayerRDD[SpaceTimeKey],
    context: java.util.Map[String, Any]
  ): MultibandTileLayerRDD[SpaceTimeKey] = {

    val scalaContext = context.asScala
    val onnxModelPath = scalaContext
      .getOrElse("onnx_model_path", "org/openeo/geotrellis/prometheo/presto_global_shorts.int8.dynamic.onnx")
      .asInstanceOf[String]
    val batchSize = scalaContext.getOrElse("batch_size", 22 * 22).asInstanceOf[Int]

    val meta   = datacube.metadata
    val layout = meta.layout
    val crs    = meta.crs

    val sc          = SparkContext.getOrCreate()
    val modelPathBC = sc.broadcast(onnxModelPath)
    val crsBC       = sc.broadcast(crs)
    val layoutBC    = sc.broadcast(layout)

    val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpatialKey, MultibandTile] = {
      tiles =>
        val spatialKey = tiles.head._1.spatialKey
        val tileExtent = layoutBC.value.mapTransform(spatialKey)
        val result = inferTile(
          tiles = tiles,
          tileExtent = tileExtent,
          crs = crsBC.value,
          onnxModelPath = modelPathBC.value,
          batchSize = batchSize
        )
        Map(spatialKey -> result)
    }

    val processes = new OpenEOProcesses()
    val input =
      if (context.containsKey("tile_size")) {
        val size = context.get("tile_size").asInstanceOf[Int]
        logger.info("PrestoInference: Retiling datacube to tile_size = " + size)
        processes.retileGeneric(datacube, size, size, 0, 0)
      } else {
        datacube
      }

    val resultRDD: RDD[(SpaceTimeKey, MultibandTile)] = processes.transformTimeDimension[SpatialKey](
      input, applyToTimeseries, reduce = true
    ).map({ case (spatialKey, tile) => (SpaceTimeKey(spatialKey, meta.bounds.get.minKey.temporalKey), tile) })

    val oldBounds = meta.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]]
    val newBounds = KeyBounds(oldBounds.minKey, SpaceTimeKey(oldBounds.maxKey.spatialKey, oldBounds.minKey.temporalKey))
    ContextRDD(
      resultRDD,
      new TileLayerMetadata[SpaceTimeKey](
        cellType = FloatConstantNoDataCellType,
        layout = layout,
        extent = meta.extent,
        crs = crs,
        bounds = newBounds
      )
    )
  }

  private def inferTile(
    tiles:         Iterable[(SpaceTimeKey, MultibandTile)],
    tileExtent:    Extent,
    crs:           CRS,
    onnxModelPath: String,
    batchSize:     Int
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
    val inputNames = session.getInputNames.toArray.map(_.asInstanceOf[String])

    require(inputNames.length == 5,
      s"ONNX model must have exactly 5 inputs, got ${inputNames.length}: ${inputNames.mkString(", ")}")

    val monthValues = Array.tabulate(T)(t => (sorted(t)._1.time.getMonthValue - 1).toLong)

    val wgs84      = CRS.fromEpsgCode(4326)
    val xform      = Transform(crs, wgs84)
    val cellWidth  = tileExtent.width / cols
    val cellHeight = tileExtent.height / rows

    val bsz       = math.min(batchSize, B)
    val xBuf      = ByteBuffer.allocateDirect(bsz * T * NUM_PRESTO_BANDS * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val maskBuf   = ByteBuffer.allocateDirect(bsz * T * NUM_PRESTO_BANDS * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val latlonBuf = ByteBuffer.allocateDirect(bsz * 2 * java.lang.Float.BYTES).order(ByteOrder.nativeOrder()).asFloatBuffer()
    val monthBuf  = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()
    val dwBuf     = ByteBuffer.allocateDirect(bsz * T * java.lang.Long.BYTES).order(ByteOrder.nativeOrder()).asLongBuffer()

    var i = 0
    while (i < bsz * T) { dwBuf.put(i, DYNAMIC_WORLD_UNKNOWN); i += 1 }

    val outputAccum = new ArrayBuffer[Float]()

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
          val base = (pi * T + t) * NUM_PRESTO_BANDS

          def raw(band: Int): Float = tile.band(band).getDouble(col, row).toFloat

          val rawB2  = raw(IN_B2);  xBuf.put(base + P_B2, normalizeBand(P_B2, rawB2)); maskBuf.put(base + P_B2, if (OnnxInferenceUtils.isNodata(rawB2)) 1L else 0L)
          val rawB3  = raw(IN_B3);  xBuf.put(base + P_B3, normalizeBand(P_B3, rawB3)); maskBuf.put(base + P_B3, if (OnnxInferenceUtils.isNodata(rawB3)) 1L else 0L)
          val rawB4  = raw(IN_B4);  xBuf.put(base + P_B4, normalizeBand(P_B4, rawB4)); maskBuf.put(base + P_B4, if (OnnxInferenceUtils.isNodata(rawB4)) 1L else 0L)
          val rawB5  = raw(IN_B5);  xBuf.put(base + P_B5, normalizeBand(P_B5, rawB5)); maskBuf.put(base + P_B5, if (OnnxInferenceUtils.isNodata(rawB5)) 1L else 0L)
          val rawB6  = raw(IN_B6);  xBuf.put(base + P_B6, normalizeBand(P_B6, rawB6)); maskBuf.put(base + P_B6, if (OnnxInferenceUtils.isNodata(rawB6)) 1L else 0L)
          val rawB7  = raw(IN_B7);  xBuf.put(base + P_B7, normalizeBand(P_B7, rawB7)); maskBuf.put(base + P_B7, if (OnnxInferenceUtils.isNodata(rawB7)) 1L else 0L)
          val rawB8  = raw(IN_B8);  xBuf.put(base + P_B8, normalizeBand(P_B8, rawB8)); maskBuf.put(base + P_B8, if (OnnxInferenceUtils.isNodata(rawB8)) 1L else 0L)
          val rawB8A = raw(IN_B8A); xBuf.put(base + P_B8A, normalizeBand(P_B8A, rawB8A)); maskBuf.put(base + P_B8A, if (OnnxInferenceUtils.isNodata(rawB8A)) 1L else 0L)
          val rawB11 = raw(IN_B11); xBuf.put(base + P_B11, normalizeBand(P_B11, rawB11)); maskBuf.put(base + P_B11, if (OnnxInferenceUtils.isNodata(rawB11)) 1L else 0L)
          val rawB12 = raw(IN_B12); xBuf.put(base + P_B12, normalizeBand(P_B12, rawB12)); maskBuf.put(base + P_B12, if (OnnxInferenceUtils.isNodata(rawB12)) 1L else 0L)
          val rawVV  = raw(IN_VV);  xBuf.put(base + P_VV, normalizeBand(P_VV, OnnxInferenceUtils.rescaleS1(rawVV))); maskBuf.put(base + P_VV, if (OnnxInferenceUtils.isNodata(rawVV)) 1L else 0L)
          val rawVH  = raw(IN_VH);  xBuf.put(base + P_VH, normalizeBand(P_VH, OnnxInferenceUtils.rescaleS1(rawVH))); maskBuf.put(base + P_VH, if (OnnxInferenceUtils.isNodata(rawVH)) 1L else 0L)
          val rawTmp = raw(IN_TEMP); xBuf.put(base + P_TEMP, normalizeBand(P_TEMP, OnnxInferenceUtils.rescaleTemperature(rawTmp))); maskBuf.put(base + P_TEMP, if (OnnxInferenceUtils.isNodata(rawTmp)) 1L else 0L)
          val rawPrc = raw(IN_PRECIP); xBuf.put(base + P_PRECIP, normalizeBand(P_PRECIP, OnnxInferenceUtils.rescalePrecipitation(rawPrc))); maskBuf.put(base + P_PRECIP, if (OnnxInferenceUtils.isNodata(rawPrc)) 1L else 0L)
          val rawElv = raw(IN_ELEV); xBuf.put(base + P_ELEV, normalizeBand(P_ELEV, rawElv)); maskBuf.put(base + P_ELEV, if (OnnxInferenceUtils.isNodata(rawElv)) 1L else 0L)
          xBuf.put(base + P_SLOPE, 0f); maskBuf.put(base + P_SLOPE, 0L)
          xBuf.put(base + P_NDVI, computeNdvi(xBuf.get(base + P_B8), xBuf.get(base + P_B4)))
          maskBuf.put(base + P_NDVI, if (OnnxInferenceUtils.isNodata(rawB8) || OnnxInferenceUtils.isNodata(rawB4) || (rawB8 + rawB4) == 0f) 1L else 0L)

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

      xBuf.limit(batchB * T * NUM_PRESTO_BANDS)
      maskBuf.limit(batchB * T * NUM_PRESTO_BANDS)
      latlonBuf.limit(batchB * 2)
      monthBuf.limit(batchB * T)
      dwBuf.limit(batchB * T)

      val xOnnx    = OnnxTensor.createTensor(env, xBuf, Array[Long](batchB, T, NUM_PRESTO_BANDS))
      val dwOnnx   = OnnxTensor.createTensor(env, dwBuf, Array[Long](batchB, T))
      val llOnnx   = OnnxTensor.createTensor(env, latlonBuf, Array[Long](batchB, 2))
      val maskOnnx = OnnxTensor.createTensor(env, maskBuf, Array[Long](batchB, T, NUM_PRESTO_BANDS))
      val monOnnx  = OnnxTensor.createTensor(env, monthBuf, Array[Long](batchB, T))
      val inputs = java.util.Map.of(
        inputNames(0), xOnnx.asInstanceOf[OnnxTensorLike],
        inputNames(1), dwOnnx.asInstanceOf[OnnxTensorLike],
        inputNames(2), llOnnx.asInstanceOf[OnnxTensorLike],
        inputNames(3), maskOnnx.asInstanceOf[OnnxTensorLike],
        inputNames(4), monOnnx.asInstanceOf[OnnxTensorLike]
      )

      val result = session.run(inputs)
      try {
        outputAccum ++= result.get(0).getValue.asInstanceOf[Array[Array[Float]]].flatten
      } finally {
        result.close()
        xOnnx.close()
        dwOnnx.close()
        llOnnx.close()
        maskOnnx.close()
        monOnnx.close()
      }

      xBuf.limit(xBuf.capacity())
      maskBuf.limit(maskBuf.capacity())
      latlonBuf.limit(latlonBuf.capacity())
      monthBuf.limit(monthBuf.capacity())
      dwBuf.limit(dwBuf.capacity())

      pStart = pEnd
    }

    OnnxInferenceUtils.buildEmbeddingTile(outputAccum.toArray, B, cols, rows)
  }

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

        x.put(base + P_B2, normalizeBand(P_B2, raw(IN_B2)))
        x.put(base + P_B3, normalizeBand(P_B3, raw(IN_B3)))
        x.put(base + P_B4, normalizeBand(P_B4, raw(IN_B4)))
        x.put(base + P_B5, normalizeBand(P_B5, raw(IN_B5)))
        x.put(base + P_B6, normalizeBand(P_B6, raw(IN_B6)))
        x.put(base + P_B7, normalizeBand(P_B7, raw(IN_B7)))
        x.put(base + P_B8, normalizeBand(P_B8, raw(IN_B8)))
        x.put(base + P_B8A, normalizeBand(P_B8A, raw(IN_B8A)))
        x.put(base + P_B11, normalizeBand(P_B11, raw(IN_B11)))
        x.put(base + P_B12, normalizeBand(P_B12, raw(IN_B12)))

        x.put(base + P_VV, normalizeBand(P_VV, OnnxInferenceUtils.rescaleS1(raw(IN_VV))))
        x.put(base + P_VH, normalizeBand(P_VH, OnnxInferenceUtils.rescaleS1(raw(IN_VH))))

        x.put(base + P_TEMP, normalizeBand(P_TEMP, OnnxInferenceUtils.rescaleTemperature(raw(IN_TEMP))))
        x.put(base + P_PRECIP, normalizeBand(P_PRECIP, OnnxInferenceUtils.rescalePrecipitation(raw(IN_PRECIP))))

        x.put(base + P_ELEV, normalizeBand(P_ELEV, raw(IN_ELEV)))
        x.put(base + P_SLOPE, 0f)
        x.put(base + P_NDVI, computeNdvi(x.get(base + P_B8), x.get(base + P_B4)))
      }
    }
    x
  }

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

    val bandPairs = Array(
      (IN_B2, P_B2), (IN_B3, P_B3), (IN_B4, P_B4), (IN_B5, P_B5),
      (IN_B6, P_B6), (IN_B7, P_B7), (IN_B8, P_B8), (IN_B8A, P_B8A),
      (IN_B11, P_B11), (IN_B12, P_B12),
      (IN_VV, P_VV), (IN_VH, P_VH),
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
          if (OnnxInferenceUtils.isNodata(v)) mask.put(base + pBand, 1L)
        }

        val b8 = tile.band(IN_B8).getDouble(col, row).toFloat
        val b4 = tile.band(IN_B4).getDouble(col, row).toFloat
        if (OnnxInferenceUtils.isNodata(b8) || OnnxInferenceUtils.isNodata(b4) || (b8 + b4) == 0f)
          mask.put(base + P_NDVI, 1L)

        mask.put(base + P_SLOPE, 0L)
      }
    }
    mask
  }

  def buildDynamicWorldTensor(B: Int, T: Int): java.nio.LongBuffer = {
    val buf = ByteBuffer
      .allocateDirect(B * T * java.lang.Long.BYTES)
      .order(ByteOrder.nativeOrder())
      .asLongBuffer()
    var i = 0
    while (i < B * T) { buf.put(i, DYNAMIC_WORLD_UNKNOWN); i += 1 }
    buf
  }

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

    val cellWidth  = tileExtent.width / cols
    val cellHeight = tileExtent.height / rows

    for (row <- 0 until rows; col <- 0 until cols) {
      val p    = row * cols + col
      val xCtr = tileExtent.xmin + (col + 0.5) * cellWidth
      val yCtr = tileExtent.ymax - (row + 0.5) * cellHeight
      val (lon, lat) = transform(xCtr, yCtr)
      latlon.put(p * 2, lat.toFloat)
      latlon.put(p * 2 + 1, lon.toFloat)
    }
    latlon
  }

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
      val m = (sortedTiles(t)._1.time.getMonthValue - 1).toLong
      for (p <- 0 until B) month.put(p * T + t, m)
    }
    month
  }

  def normalizeBand(prestoIdx: Int, value: Float): Float =
    if (OnnxInferenceUtils.isNodata(value) || value.isNaN) 0f
    else (value + BANDS_ADD(prestoIdx)) / BANDS_DIV(prestoIdx)

  def computeNdvi(b8Norm: Float, b4Norm: Float): Float = {
    val sum = b8Norm + b4Norm
    if (sum == 0f) 0f else (b8Norm - b4Norm) / sum
  }
}
