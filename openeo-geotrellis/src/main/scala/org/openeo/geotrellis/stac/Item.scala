package org.openeo.geotrellis.stac

import geotrellis.vector.Extent

import java.util

case class Item(id: String, timestamp: String, bbox: Extent, assets: util.Map[String, Asset])
