package org.openeo.sar

import org.openeo.geotrelliscommon.ExtraProcessingParameters

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Extra processing parameters for the openEO `sar_backscatter` process, as implemented by
 * [[org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider]]. Set it on
 * `DataCubeParameters.extraProcessingParameters` to pass these arguments down to the
 * raster source provider.
 *
 * Mirrors the Python `SarBackscatterArgs` NamedTuple used in the openEO process graph layer:
 * {{{
 * class SarBackscatterArgs(NamedTuple):
 *     """Arguments for the `sar_backscatter` process."""
 *     coefficient: Union[str, None] = "gamma0-terrain"
 *     elevation_model: Union[str, None] = None
 *     mask: bool = False
 *     contributing_area: bool = False
 *     local_incidence_angle: bool = False
 *     ellipsoid_incidence_angle: bool = False
 *     noise_removal: bool = True
 *     # Additional (non-standard) fine-tuning options
 *     options: dict = {}
 * }}}
 *
 * Like [[org.openeo.geotrelliscommon.DataCubeParameters]], this is a plain mutable class
 * with a no-arg constructor and setters, so it is easy to create and fill in from Python
 * via py4j:
 * {{{
 * params = jvm.org.openeo.sar.SarBackscatterParameters()
 * params.setCoefficient("gamma0-terrain")
 * params.setMask(True)
 * params.setOptions({"some_option": "value"})
 * data_cube_parameters.setExtraProcessingParameters(params)
 * }}}
 *
 * For convenience, [[SarBackscatterParameters.apply]] and [[SarBackscatterParameters.fromMap]]
 * (the latter accepting a `java.util.Map`, e.g. a Python dict such as
 * `SarBackscatterArgs._asdict()` passed over py4j) can be used to fill in every field in one call.
 */
class SarBackscatterParameters extends ExtraProcessingParameters {
  var coefficient: Option[String] = Some("gamma0-terrain")
  var elevationModel: Option[String] = None
  var mask: Boolean = false
  var contributingArea: Boolean = false
  var localIncidenceAngle: Boolean = false
  var ellipsoidIncidenceAngle: Boolean = false
  var noiseRemoval: Boolean = true
  var options: util.Map[String, Object] = Collections.emptyMap()

  def setCoefficient(coefficient: String): Unit = this.coefficient = Option(coefficient)
  def setElevationModel(elevationModel: String): Unit = this.elevationModel = Option(elevationModel)
  def setMask(mask: Boolean): Unit = this.mask = mask
  def setContributingArea(contributingArea: Boolean): Unit = this.contributingArea = contributingArea
  def setLocalIncidenceAngle(localIncidenceAngle: Boolean): Unit = this.localIncidenceAngle = localIncidenceAngle
  def setEllipsoidIncidenceAngle(ellipsoidIncidenceAngle: Boolean): Unit = this.ellipsoidIncidenceAngle = ellipsoidIncidenceAngle
  def setNoiseRemoval(noiseRemoval: Boolean): Unit = this.noiseRemoval = noiseRemoval
  def setOptions(options: util.Map[String, Object]): Unit = this.options = options

  override def toString: String =
    s"SarBackscatterParameters(coefficient=$coefficient, elevationModel=$elevationModel, mask=$mask, " +
      s"contributingArea=$contributingArea, localIncidenceAngle=$localIncidenceAngle, " +
      s"ellipsoidIncidenceAngle=$ellipsoidIncidenceAngle, noiseRemoval=$noiseRemoval, options=$options)"

  /**
   * Translate these openEO-level arguments into the [[SarProcessingConfig]] consumed by
   * [[org.openeo.sar.provider.Sentinel1GrdRasterSourceProvider]], overlaying `base` (typically
   * the provider's configured default) with the fields that have a direct equivalent:
   *  - `coefficient` starting with "gamma0" selects [[BackscatterNormalization.Gamma0RTC]],
   *    any other (non-null) value selects [[BackscatterNormalization.Sigma0]]; `None` keeps
   *    `base`'s normalization.
   *  - `mask` maps to `shadowLayoverMask` (the openEO spec's "data mask" for `sar_backscatter`).
   *  - `localIncidenceAngle` / `ellipsoidIncidenceAngle` map 1:1.
   *
   * `elevationModel`, `contributingArea`, `noiseRemoval` and `options` are not yet wired into
   * the terrain-correction backend and are currently ignored by this conversion; they remain
   * available on this class for callers/backends that do support them.
   */
  def toSarProcessingConfig(base: SarProcessingConfig = SarProcessingConfig.default): SarProcessingConfig =
    base.copy(
      normalization = coefficient.map(_.toLowerCase) match {
        case Some(c) if c.startsWith("gamma0") => BackscatterNormalization.Gamma0RTC
        case Some(_)                           => BackscatterNormalization.Sigma0
        case None                              => base.normalization
      },
      shadowLayoverMask = mask,
      localIncidenceAngle = localIncidenceAngle,
      ellipsoidIncidenceAngle = ellipsoidIncidenceAngle
    )
}

object SarBackscatterParameters {

  /**
   * Convenience factory that mirrors the field order/defaults of the Python
   * `SarBackscatterArgs` NamedTuple, so all fields can be filled in with a single py4j call
   * instead of one call per setter.
   */
  def apply(
             coefficient: String = "gamma0-terrain",
             elevationModel: String = null,
             mask: Boolean = false,
             contributingArea: Boolean = false,
             localIncidenceAngle: Boolean = false,
             ellipsoidIncidenceAngle: Boolean = false,
             noiseRemoval: Boolean = true,
             options: util.Map[String, Object] = Collections.emptyMap()
           ): SarBackscatterParameters = {
    val params = new SarBackscatterParameters
    params.coefficient = Option(coefficient)
    params.elevationModel = Option(elevationModel)
    params.mask = mask
    params.contributingArea = contributingArea
    params.localIncidenceAngle = localIncidenceAngle
    params.ellipsoidIncidenceAngle = ellipsoidIncidenceAngle
    params.noiseRemoval = noiseRemoval
    params.options = options
    params
  }

  /**
   * Build a [[SarBackscatterParameters]] from a `java.util.Map[String, Object]`, keyed by the
   * same (snake_case) field names as the Python `SarBackscatterArgs` NamedTuple. This is the
   * most convenient way to go from Python over py4j, e.g.:
   * {{{
   * args = SarBackscatterArgs(mask=True, coefficient="sigma0-ellipsoid")
   * params = jvm.org.openeo.sar.SarBackscatterParameters.fromMap(args._asdict())
   * }}}
   * Keys that are absent from the map fall back to the documented defaults; a `None`/`null`
   * value for `coefficient` or `elevation_model` is preserved as such.
   */
  def fromMap(args: util.Map[String, Object]): SarBackscatterParameters = {
    val m = args.asScala
    val params = new SarBackscatterParameters
    if (m.contains("coefficient")) params.coefficient = Option(m("coefficient")).map(_.toString)
    if (m.contains("elevation_model")) params.elevationModel = Option(m("elevation_model")).map(_.toString)
    m.get("mask").foreach(v => params.mask = v.asInstanceOf[Boolean])
    m.get("contributing_area").foreach(v => params.contributingArea = v.asInstanceOf[Boolean])
    m.get("local_incidence_angle").foreach(v => params.localIncidenceAngle = v.asInstanceOf[Boolean])
    m.get("ellipsoid_incidence_angle").foreach(v => params.ellipsoidIncidenceAngle = v.asInstanceOf[Boolean])
    m.get("noise_removal").foreach(v => params.noiseRemoval = v.asInstanceOf[Boolean])
    m.get("options").foreach {
      case om: util.Map[String, Object]@unchecked => params.options = om
      case _ =>
    }
    params
  }
}
