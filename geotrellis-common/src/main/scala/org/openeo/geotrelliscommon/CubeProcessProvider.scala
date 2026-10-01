package org.openeo.geotrelliscommon

/**
 * SPI interface for auto-registering Scala objects that contain
 * [[OpenEOProcess]]-annotated methods in [[CubeProcessRegistry]].
 *
 * == How to register a new provider ==
 *
 * 1. Create a public no-arg class that implements this trait and returns
 *    the Scala object singleton:
 *    {{{
 *      class MyCubeProcessProvider extends CubeProcessProvider {
 *        def getInstance(): AnyRef = MyCubeProcessObject
 *      }
 *    }}}
 *
 * 2. Add the fully-qualified class name to:
 *    {{{
 *      src/main/resources/META-INF/services/org.openeo.geotrelliscommon.CubeProcessProvider
 *    }}}
 *
 * [[CubeProcessRegistry]] calls [[getInstance]] during its first use and
 * passes the result to [[CubeProcessRegistry.register]], scanning it for
 * `@OpenEOProcess`-annotated methods.
 */
trait CubeProcessProvider {
  /** Return the Scala object singleton whose methods should be registered. */
  def getInstance(): AnyRef
}
