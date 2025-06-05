package org.openeo.geotrellis.stac

import geotrellis.vector.Extent

import java.util

case class Item(id: String, datetime: String, bbox: Extent, assets: util.Map[String, Asset]) // TODO: order assets by key?
