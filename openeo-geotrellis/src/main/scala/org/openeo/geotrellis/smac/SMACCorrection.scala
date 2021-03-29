package org.openeo.geotrellis.smac

import java.time.ZonedDateTime
import org.apache.commons.math3.util.FastMath
import org.openeo.geotrellis.smac.SMACCorrection.Coeff
import spire.implicits._
import scala.collection.mutable
import scala.io.Source
import org.openeo.geotrellis.icor.CorrectionDescriptor


/**
 * Scala port of http://tully.ups-tlse.fr/olivier/smac-python
 * Original version provided under GPL V3 by Olivier Hagolle
 *
 * Scientific reference:
 * Rahman, H., & Dedieu, G. (1994). SMAC: a simplified method for the atmospheric correction of satellite measurements in the solar spectrum. REMOTE SENSING, 15(1), 123-143.
 *
 */
object SMACCorrection{

  class Coeff(smaccoeffStream:java.io.InputStream) extends Serializable {

    val lines: Array[String]={
      
//      val bufferedSource = Source.fromFile(smac_filename)
      val bufferedSource = Source.fromInputStream(smaccoeffStream)
      
      try{
        bufferedSource.getLines().toArray
      } finally {
        bufferedSource.close()
      }
    }
    //H20
    var temp:Array[String]=lines(0).trim().split("\\s+")
    val ah2o=(temp(0)).toFloat
    val nh2o=(temp(1)).toFloat
    //O3
    temp=lines(1).trim().split("\\s+")
    val ao3=(temp(0)).toFloat
    val no3=(temp(1)).toFloat
    //O2
    temp=lines(2).trim().split("\\s+")
    val ao2=(temp(0)).toFloat
    val no2=(temp(1)).toFloat
    val po2=(temp(2)).toFloat
    //CO2
    temp=lines(3).trim().split("\\s+")
    val aco2=(temp(0)).toFloat
    val nco2=(temp(1)).toFloat
    val pco2=(temp(2)).toFloat
    //NH4
    temp=lines(4).trim().split("\\s+")
    val ach4=(temp(0)).toFloat
    val nch4=(temp(1)).toFloat
    val pch4=(temp(2)).toFloat
    //NO2
    temp=lines(5).trim().split("\\s+")
    val ano2=(temp(0)).toFloat
    val nno2=(temp(1)).toFloat
    val pno2=(temp(2)).toFloat
    //NO2
    temp=lines(6).trim().split("\\s+")
    val aco=(temp(0)).toFloat
    val nco=(temp(1)).toFloat
    val pco=(temp(2)).toFloat

    //rayleigh and aerosol scattering

    temp=lines(7).trim().split("\\s+")
    val a0s=(temp(0)).toFloat
    val a1s=(temp(1)).toFloat
    val a2s=(temp(2)).toFloat
    val a3s=(temp(3)).toFloat
    temp=lines(8).trim().split("\\s+")
    val a0T=(temp(0)).toFloat
    val a1T=(temp(1)).toFloat
    val a2T=(temp(2)).toFloat
    val a3T=(temp(3)).toFloat
    temp=lines(9).trim().split("\\s+")
    val taur=(temp(0)).toFloat
    val sr=(temp(0)).toFloat
    temp=lines(10).trim().split("\\s+")
    val a0taup  = (temp(0)).toFloat
    val a1taup  = (temp(1)).toFloat
    temp=lines(11).trim().split("\\s+")
    val wo      = (temp(0)).toFloat
    val gc      = (temp(1)).toFloat
    temp=lines(12).trim().split("\\s+")
    val a0P     = (temp(0)).toFloat
    val a1P     = (temp(1)).toFloat
    val a2P     = (temp(2)).toFloat
    temp=lines(13).trim().split("\\s+")
    val a3P     = (temp(0)).toFloat
    val a4P     = (temp(1)).toFloat
    temp=lines(14).trim().split("\\s+")
    val Rest1   = (temp(0)).toFloat
    val Rest2   = (temp(1)).toFloat
    temp=lines(15).trim().split("\\s+")
    val Rest3   = (temp(0)).toFloat
    val Rest4   = (temp(1)).toFloat
    temp=lines(16).trim().split("\\s+")
    val Resr1   = (temp(0)).toFloat
    val Resr2   = (temp(1)).toFloat
    val Resr3   = (temp(2)).toFloat
    temp=lines(17).trim().split("\\s+")
    val Resa1   = (temp(0)).toFloat
    val Resa2   = (temp(1)).toFloat
    temp=lines(18).trim().split("\\s+")
    val Resa3   = (temp(0)).toFloat
    val Resa4   = (temp(1)).toFloat
  }


  def PdeZ(Z:Float): Float = {
    //PdeZ : Atmospheric pressure (in hpa) as a function of altitude (in meters)
    (1013.25 * math.pow( 1 - 0.0065 * Z / 288.15 , 5.31 )).floatValue()
  }

  /**
   *
   *
   * @param r_toa top of atmosphere reflectance
   * @param tetas solar zenith angle degree
   * @param tetav viewing zenith angle degree
   * @param raa relative azimuth degrees
   * @param pressure
   * @param taup550 AOT at 550 nm
   * @param uo3 Ozone content (cm)  0.3 cm= 300 Dobson Units
   * @param uh2o Water vapour (g/cm2)
   * @param coef
   * @return reflectance
   */
  def smac_inv( r_toa:Double, tetas:Double, tetav:Double, raa:Double,pressure:Float,taup550:Float, uo3:Float, uh2o:Float, coef:Coeff): Double = {

    val ah2o:Float = coef.ah2o
    val nh2o:Float = coef.nh2o
    val ao3:Float = coef.ao3
    val no3:Float = coef.no3
    val ao2:Float = coef.ao2
    val no2:Float = coef.no2
    val po2: Float = coef.po2
    val aco2:Float = coef.aco2
    val nco2:Float = coef.nco2
    val pco2:Float = coef.pco2
    val ach4:Float = coef.ach4
    val nch4:Float = coef.nch4
    val pch4:Float = coef.pch4
    val ano2:Float = coef.ano2
    val nno2:Float = coef.nno2
    val pno2:Float = coef.pno2
    val aco:Float = coef.aco
    val nco:Float = coef.nco
    val pco:Float = coef.pco
    val a0s:Float = coef.a0s
    val a1s:Float = coef.a1s
    val a2s:Float = coef.a2s
    val a3s:Float = coef.a3s
    val a0T:Float = coef.a0T
    val a1T:Float = coef.a1T
    val a2T:Float = coef.a2T
    val a3T:Float = coef.a3T
    val taur:Float = coef.taur
    val sr:Float = coef.sr
    val a0taup: Float = coef.a0taup
    val a1taup: Float = coef.a1taup
    val wo:Float = coef.wo
    val gc:Float = coef.gc
    val a0P:Float = coef.a0P
    val a1P:Float = coef.a1P
    val a2P:Float = coef.a2P
    val a3P:Float = coef.a3P
    val a4P:Float = coef.a4P
    val Rest1:Float = coef.Rest1
    val Rest2:Float = coef.Rest2
    val Rest3:Float = coef.Rest3
    val Rest4:Float = coef.Rest4
    val Resr1: Float = coef.Resr1
    val Resr2:Float = coef.Resr2
    val Resr3:Float = coef.Resr3
    val Resa1:Float = coef.Resa1
    val Resa2:Float = coef.Resa2
    val Resa3:Float = coef.Resa3
    val Resa4:Float = coef.Resa4

    val cdr = math.Pi / 180.0
    val crd = 180 / math.Pi

    /*------:  calcul de la reflectance de surface  smac               :--------*/

    val us = math.cos(tetas * cdr)
    val uv = math.cos(tetav * cdr)
    val Peq = pressure / 1013.25

    /*------:  1) air mass */
    val m = 1 / us + 1 / uv

    /*------:  2) aerosol optical depth in the spectral band, taup     :--------*/
    val taup = (a0taup) + (a1taup) * taup550

    /*------:  3) gaseous transmissions (downward and upward paths)    :--------*/
    /*val to3 = 1.
    val th2o = 1.
    val to2 = 1.
    val tco2 = 1.
    val tch4 = 1.*/

    val uo2 = math.pow(Peq, po2)
    val uco2 = math.pow(Peq, pco2)
    val uch4 = math.pow(Peq, pch4)
    val uno2 = math.pow(Peq, pno2)
    val uco = math.pow(Peq, pco)

    /*------:  4) if uh2o <= 0 and uo3 <=0 no gaseous absorption is computed  :--------*/
    val to3 = math.exp((ao3) * math.pow((uo3 * m) , (no3)))
    val th2o = math.exp((ah2o) * math.pow((uh2o * m) , (nh2o)))
    val to2 = math.exp((ao2) * math.pow((uo2 * m) , (no2)))
    val tco2 = math.exp((aco2) * math.pow((uco2 * m) , (nco2)))
    val tch4 = math.exp((ach4) * math.pow((uch4 * m) , (nch4)))
    val tno2 = math.exp((ano2) * math.pow((uno2 * m) , (nno2)))
    val tco = math.exp((aco) * math.pow((uco * m) , (nco)))
    val tg = th2o * to3 * to2 * tco2 * tch4 * tco * tno2

    /*------:  5) Total scattering transmission                      :--------*/
    val ttetas = (a0T) + (a1T) * taup550 / us + ((a2T) * Peq + (a3T)) / (1.0f + us)
    /* downward */
    val ttetav = (a0T) + (a1T) * taup550 / uv + ((a2T) * Peq + (a3T)) / (1.0f + uv)
    /* upward   */

    /*------:  6) spherical albedo of the atmosphere                 :--------*/
    val s = (a0s) * Peq + (a3s) + (a1s) * taup550 + (a2s) * math.pow(taup550, 2.0)

    /*------:  7) scattering angle cosine                            :--------*/
    var cksi = -((us * uv) + (math.sqrt(1.0 - us * us) * math.sqrt(1.0 - uv * uv) * math.cos((raa) * cdr)))
    if (cksi < -1) {
      //TODO is this the correct translation of python =- ??
      cksi = cksi - 1.0
    }

    /*------:  8) scattering angle in degree 			 :--------*/
    val ksiD = crd * FastMath.acos(cksi)

    /*------:  9) rayleigh atmospheric reflectance 			 :--------*/
    val ray_phase = 0.7190443 * (1.0 + (cksi * cksi)) + 0.0412742
    var ray_ref = (taur * ray_phase) / (4 * us * uv)
    ray_ref = ray_ref * pressure / 1013.25
    val taurz = (taur) * Peq

    /*------:  10) Residu Rayleigh 					 :--------*/
    val Res_ray = Resr1 + Resr2 * taur * ray_phase / (us * uv) + Resr3 * math.pow((taur * ray_phase / (us * uv)), 2.0)

    /*------:  11) aerosol atmospheric reflectance			 :--------*/
    val aer_phase = a0P + a1P * ksiD + a2P * ksiD * ksiD + a3P * (ksiD ** 3) + a4P * (ksiD ** 4)

    val ak2 = (1.0 - wo) * (3.0 - wo * 3.0 * gc)
    val ak = math.sqrt(ak2)
    val e = -3 * us * us * wo / (4 * (1.0 - ak2 * us * us))
    val f = -(1.0 - wo) * 3 * gc * us * us * wo / (4 * (1.0 - ak2 * us * us))
    val dp = e / (3 * us) + us * f
    val d = e + f
    val b = 2 * ak / (3.0 - wo * 3 * gc)
    val delta = math.exp(ak * taup) * (1.0 + b) * (1.0 + b) - math.exp(-ak * taup) * (1.0 - b) * (1.0 - b)
    val ww = wo / 4.0
    val ss = us / (1.0 - ak2 * us * us)
    val q1 = 2.0 + 3 * us + (1.0 - wo) * 3 * gc * us * (1.0 + 2 * us)
    val q2 = 2.0 - 3 * us - (1.0 - wo) * 3 * gc * us * (1.0 - 2 * us)
    val q3 = q2 * math.exp(-taup / us)
    val c1 = ((ww * ss) / delta) * (q1 * math.exp(ak * taup) * (1.0 + b) + q3 * (1.0 - b))
    val c2 = -((ww * ss) / delta) * (q1 * math.exp(-ak * taup) * (1.0 - b) + q3 * (1.0 + b))
    val cp1 = c1 * ak / (3.0 - wo * 3 * gc)
    val cp2 = -c2 * ak / (3.0 - wo * 3 * gc)
    val z = d - wo * 3 * gc * uv * dp + wo * aer_phase / 4.0
    val x = c1 - wo * 3 * gc * uv * cp1
    val y = c2 - wo * 3 * gc * uv * cp2
    val aa1 = uv / (1.0 + ak * uv)
    val aa2 = uv / (1.0 - ak * uv)
    val aa3 = us * uv / (us + uv)

    var aer_ref = x * aa1 * (1.0 - math.exp(-taup / aa1))
    aer_ref = aer_ref + y * aa2 * (1.0 - math.exp(-taup / aa2))
    aer_ref = aer_ref + z * aa3 * (1.0 - math.exp(-taup / aa3))
    aer_ref = aer_ref / (us * uv)

    /*------:  12) Residu Aerosol  					:--------*/
    val Res_aer = (Resa1 + Resa2 * (taup * m * cksi) + Resa3 * ((taup * m * cksi) ** 2)) + Resa4 * ((taup * m * cksi) ** 3)

    /*------:  13)  Terme de couplage molecule / aerosol		:--------*/
    val tautot = taup + taurz
    val Res_6s = (Rest1 + Rest2 * (tautot * m * cksi) + Rest3 * ((tautot * m * cksi) ** 2)) + Rest4 * ((tautot * m * cksi) ** 3)

    /*------:  14) total atmospheric reflectance  			:--------*/
    val atm_ref = ray_ref - Res_ray + aer_ref - Res_aer + Res_6s

    /*------:  15) Surface reflectance  				:--------*/

    var r_surf = r_toa - (atm_ref * tg)
    r_surf = r_surf / ((tg * ttetas * ttetav) + (r_surf * s))

    return r_surf
  }
}


class SMACCorrection extends CorrectionDescriptor(){

  val coeffMap = mutable.Map[String,Coeff]()

  /**
   * This function performs the pixel-wise correction: src is a pixel value belonging to band (as from getBandFromName).
   * If band is out of range, the function should return src (since any errors of mis-using bands should be caught upstream, before the pixel-wise loop).
   *
   * @param bandName band id
   * @param src  to be converted: this may be digital number, reflectance, radiance, ... depending on the specific correction, and it should clearly be documented there!
   * @param sza  degree
   * @param vza  degree
   * @param raa  degree
   * @param gnd  km
   * @param aot
   * @param cwv
   * @param ozone
   * @param waterMask
   * @return BOA reflectance * 10000 (i.e. in digital number)
   */
  override def correct(bandName: String, time: ZonedDateTime, src: Double, sza: Double, vza: Double, raa: Double, gnd: Double, aot: Double, cwv: Double, ozone: Double, waterMask: Int): Double = {
    //TODO lookup pressure, ozone, water vapour in ECMWF cams
    var maybeCoeff = coeffMap.get(bandName)
    if(maybeCoeff.isEmpty) {
      val bandPattern = ".*(B[018][0-9A]).*".r
      val coefficients = {
        bandName match {
          case bandPattern(pattern) => "Coef_S2A_CONT_" + pattern + ".dat"
          case _ => throw new IllegalArgumentException("Could not match band name: " + bandName)
        }
      }
      val coeffForBand = new Coeff(SMACCorrection.getClass.getResourceAsStream(coefficients))
      coeffMap(bandName) = coeffForBand
      maybeCoeff = Some(coeffForBand)
    }

    val pressure = 1013 //SMACCorrection.PdeZ(1300);
    val UH2O = 0.3 // Water vapour (g/cm2)
    val r_surf = SMACCorrection.smac_inv(src.doubleValue()/10000.0, sza, vza, raa, pressure.toFloat, aot.toFloat, ozone.toFloat, UH2O.toFloat, maybeCoeff.get)*10000.0
    r_surf
  }

  override def getBandFromName(name: String): Int = 0

  override def getIrradiance(iband: Int): Double = ???

  override def getCentralWavelength(iband: Int): Double = ???

}
