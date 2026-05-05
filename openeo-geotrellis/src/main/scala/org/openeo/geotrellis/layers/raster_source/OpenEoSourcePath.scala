package org.openeo.geotrellis.layers.raster_source

import geotrellis.raster.SourcePath

/**
 * SourcePath is a trait, so we need to subclass it to instantiate.
 */
case class OpenEoSourcePath(value: String) extends SourcePath
