package org.openeo.geotrellis.stac

import java.util

case class Asset(path: String, bandIndices: util.List[Int] = null, metadata:util.Map[String,Any] = null)
