package org.openeo.geotrellis.ard

import geotrellis.raster.{FloatArrayTile, MultibandTile, Tile, isNoData}
import org.apache.commons.math3.util.FastMath
import spire.implicits.cfor

/**
 * A port of the original FORCE - Framework for Operational Radiometric
 * Correction for Environmental monitoring implementation.
 * Originally written by David Frantz
 *
 * https://github.com/davidfrantz/force/blob/master/src/lower-level/resmerge-ll.c
 *
 */
object Improphe {

  def rescale_weight(weight: Float, minWeight: Float, maxWeight: Float): Float = {
    var normweight = 0.0F

    if (weight == 0) normweight = 1.0F
    else if (minWeight == maxWeight) normweight = 1.0F
    else normweight = 1.0F + FastMath.exp(25 * ((weight - minWeight) / (maxWeight - minWeight)) - 7.5F).floatValue()

    return normweight
  }

  def distance_kernel(nk:Int): FloatArrayTile ={

    val kernel = FloatArrayTile.ofDim(nk,nk)
    val h = (nk-1)/2
    // pre-compute kernel distance
    var ki: Int = -1
    cfor(-h)(_ <= h, _ + 1)(ii => {
      ki += 1
      var kj: Int = -1
      cfor(-h)(_ <= h, _ + 1)(jj => {
        kj += 1
        kernel.setDouble(ki,kj,FastMath.sqrt(ii*ii+jj*jj))

      })
    })
    return kernel
  }

  /**
   *
   * Predict Sentinel-2 10m bands (ImproPhe method)
   *
   * This function is the same as improphe, but deals with Sentinel-2 data
   * directly. This avoids copying the 16bit TOA reflectance to FLOAT va-
   * lues, which would significantly increase RAM requirements. Texture is
   * also disabled as it does not make sense for a 2:1 ratio of spatial re-
   * solution.
   *
   * @param QAI     Quality Assurance Information
   * @param toa     TOA reflectance
   * @param bands_m MR bands to use
   * @param bands_c CR bands to predict
   * @param nk      number of kernel pixels, and
   * @param mink    minimum number of pixels for good prediction
   */
  def improphe(QAI: Tile, toa: MultibandTile, bands_m: Array[Int], bands_c: Array[Int], mink: Int,kSize:Int=5): MultibandTile = {

    val h = (kSize - 1) / 2
    val KDIST = distance_kernel(kSize)
    val nx = toa.cols
    val ny = toa.rows
    val ns = 5
    val Sthr = Array(0.0025, 0.005, 0.01, 0.02, 0.025) // cutoff-threshold for S
    val predict = MultibandTile(toa.bands.map(_.toArrayTile().mutable))

    cfor(0)(_ < ny, _ + 1)(i => {
      cfor(0)(_ < nx, _ + 1)(j => {

        // if nodata or cloud/shadow: skip
        if (!isNoData(toa.band(bands_m(0)).get(j, i))) {

          var wn = 0 //wn is valid neighbour pixel index
          val Sclass = Array.ofDim[Int](KDIST.size)
          val Srecord = Array.ofDim[Float](KDIST.size)
          val Srange = Array(Array.fill(ns)(Float.MaxValue), Array.fill(ns)(Float.MinValue))
          val coarsePixels = Array.ofDim[Array[Float]](KDIST.size)
          val Sn = Array.fill[Int](ns)(0)
          /** for each neighbour * */
          var ki: Int = -1
          cfor(-h)(_ <= h, _ + 1)(ii => {
            ki += 1
            var kj: Int = -1
            cfor(-h)(_ <= h, _ + 1)(jj => {
              kj += 1
              val ni = i + ii
              val nj = j + jj
              if (ni >= 0 && nj >= 0 && ni < ny && nj < nx) {

                // if not in circular kernel, skip
                if (KDIST.get(ki, kj) <= h && !isNoData(toa.band(bands_m(0)).get(nj, ni))) {
                  var sum = 0
                  // spectral distance between center pixel and its neighbour (Mean absolute error)
                  val S: Float = bands_m.map(b => {
                    val toaBand = toa.band(b)
                    math.abs((toaBand.get(j, i) - toaBand.get(nj, ni)) / 10000.0f)
                  }).sum / bands_m.length
                  // get cutoff-threshold index for S
                  val Sc = Sthr.indexWhere(S < _)
                  if (Sc >= 0) {
                    Sclass(wn) = Sc
                    // keep S + determine range
                    Srecord(wn) = S
                    if (S > 0) {
                      cfor(Sc)(_ < ns, _ + 1)(s => {
                        if (S > Srange(1)(s)) Srange(1)(s) = S;
                        if (S < Srange(0)(s)) Srange(0)(s) = S;
                      })
                    }
                    // keep coarse data
                    coarsePixels(wn) = bands_c.map(toa.band(_).getDouble(nj, ni).toFloat / 10000.0f)
                    wn = wn + 1
                    (Sc until ns).foreach { s => (Sn(s) = Sn(s) + 1) }
                  }
                }
              }

            })
          })
          // if no valid neighbour... damn.. use CR
          if (wn == 0) {
            //keep coarse value, so do nothing?
            //bands_c.foreach(toa.band(_).se)
          } else {
            var Sc = Sn.indexWhere(_ >= mink)
            if (Sc < 0) {
              Sc = ns - 1
            }

            var weight = 0.0
            val weightxdata: Array[Float] = Array.fill(bands_c.length)(0.0F)
            cfor(0)(_ < wn, _ + 1)(k => {
              if (Sclass(k) <= Sc) {
                val SS: Float = rescale_weight(Srecord(k), Srange(0)(Sc), Srange(1)(Sc))
                val W = 1.0F / SS
                var f = 0
                while (f < weightxdata.length) {
                  weightxdata(f) = weightxdata(f) + W * coarsePixels(k)(f)
                  f += 1
                }
                weight += W

              }
            })

            // prediction -> weighted mean
            var f: Int = 0
            bands_c.foreach(index_c => {
              predict.band(index_c).mutable.setDouble(j, i, ((weightxdata(f) / weight) * 10000.0))
              f += 1
            })
          }
        }


      })
    })

    return predict
  }

}
