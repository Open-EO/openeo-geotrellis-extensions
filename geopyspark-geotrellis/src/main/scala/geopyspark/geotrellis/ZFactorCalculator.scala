package geopyspark.geotrellis

import _root_.io.circe.parser.parse
import cats.syntax.either._
import geopyspark.geotrellis.Constants.{FEET, FEETATEQUATOR, METERS, METERSATEQUATOR}
import geotrellis.vector._
import org.apache.commons.math3.analysis.interpolation._


class ZFactorCalculator(zFactorProducer: Double => Double) extends Serializable {
  def deriveZFactor(extent: Extent): Double =
    deriveZFactor(extent.center)

  def deriveZFactor(center: Point): Double =
    deriveZFactor(center.y)

  def deriveZFactor(lat: Double): Double =
    zFactorProducer(lat)
}


object ZFactorCalculator {
  def createLatLngZFactorCalculator(units: String): ZFactorCalculator =
    units match {
      case METERS =>
        ZFactorCalculator((lat: Double) => 1 / (METERSATEQUATOR * math.cos(math.toRadians(lat))))
      case FEET =>
        ZFactorCalculator((lat: Double) => 1 / (FEETATEQUATOR * math.cos(math.toRadians(lat))))
    }

  def createZFactorCalculator(table: String): ZFactorCalculator = {
    val zFactorTable =
      parse(table)
        .valueOr(throw _)
        .as[Map[String, String]]
        .valueOr(throw _)
        .map { case (k, v) => k.toDouble -> v.toDouble }

    val lattitudes = zFactorTable.keys.toArray
    val zfactors = zFactorTable.values.toArray

    val interp = new LinearInterpolator()
    val spline = interp.interpolate(lattitudes, zfactors)

    ZFactorCalculator((lat: Double) => spline.value(lat))
  }

  def apply(producer: Double => Double): ZFactorCalculator =
    new ZFactorCalculator(producer)
}
