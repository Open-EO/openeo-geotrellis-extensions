package org.openeo.geotrellis.layers

import geotrellis.raster.SourcePath

/**
 * SourcePath is a trait, so we need to subclass it to instantiate.
 */
case class OpenEoSourcePath(value: String) extends SourcePath
