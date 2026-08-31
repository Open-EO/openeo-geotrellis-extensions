package org.openeo.geotrellis

import ai.onnxruntime.OrtSession.SessionOptions.ExecutionMode
import ai.onnxruntime.{OrtEnvironment, OrtSession}
import geotrellis.layer.SpaceTimeKey
import geotrellis.raster._

import java.net.URL
import java.nio.file.{Files, Paths}

object OnnxInferenceUtils {

  val NODATA: Float = 65535f
  val NOCROP_VALUE: Float = 254f

  val sessionCache =
    new java.util.concurrent.ConcurrentHashMap[String, OrtSession]()

  Runtime.getRuntime.addShutdownHook(new Thread(() =>
    sessionCache.values().forEach(s => scala.util.Try(s.close()))
  ))

  def isNodata(v: Float): Boolean = v == NODATA || v.isNaN

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

  def argmax(arr: Array[Float]): Int = {
    var best     = 0
    var bestVal  = arr(0)
    var i        = 1
    while (i < arr.length) { if (arr(i) > bestVal) { bestVal = arr(i); best = i }; i += 1 }
    best
  }

  def getOrCreateSession(modelPath: String): OrtSession = {
    sessionCache.computeIfAbsent(modelPath, mp => {
      val bytes   = loadModelBytes(mp)
      val env     = OrtEnvironment.getEnvironment()
      val options = new OrtSession.SessionOptions()
      options.setCPUArenaAllocator(true)
      options.setInterOpNumThreads(3)
      options.setIntraOpNumThreads(1)
      options.setExecutionMode(ExecutionMode.PARALLEL)
      env.createSession(bytes, options)
    })
  }

  def loadModelBytes(model: String): Array[Byte] = {
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

  def rescaleS1(rawDn: Float): Float =
    if (isNodata(rawDn) || rawDn <= 0f) NODATA
    else (20f * math.log10(rawDn).toFloat) - 83f

  def rescaleTemperature(raw: Float): Float =
    if (isNodata(raw)) NODATA else raw / 100f

  def rescalePrecipitation(raw: Float): Float =
    if (isNodata(raw)) NODATA else raw / 100000f

  def sortByTime(
    tiles: Iterable[(SpaceTimeKey, MultibandTile)]
  ): Seq[(SpaceTimeKey, MultibandTile)] =
    tiles.toSeq.sortBy(_._1.instant)

  def buildEmbeddingTile(
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

  def buildOutputTile(
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
      val lcProbs = softmax(lcLogits, p * numLcClasses, numLcClasses)
      val lcPred  = argmax(lcProbs)
      val isCrop  = croplandClassSet.contains(lcPred)

      croplandClass(p) = if (isCrop) 1f else 0f
      croplandProb(p)  = croplandClassSet.foldLeft(0f) { (acc, idx) =>
        if (idx < numLcClasses) acc + lcProbs(idx) else acc
      }

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
}
