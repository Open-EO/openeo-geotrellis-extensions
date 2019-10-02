package org.openeo.geotrelliss3

import java.io.InputStreamReader

import com.google.common.io.CharStreams
import geotrellis.vector.GeometryCollectionFeature
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.locationtech.jts.index.strtree.STRtree
import spray.json.{JsObject, JsString, JsValue, JsonReader}

object GridParser {

  val utmGrid: STRtree = {
    val json = getClass.getResourceAsStream("/S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00.json")
    val jsonString = CharStreams.toString(new InputStreamReader(json))

    val jsonFeatureCollection = GeoJson.parse[JsonFeatureCollection](jsonString)

    implicit val jsonReader = new JsonReader[String] {
      def read(json: JsValue): String = json.asInstanceOf[JsObject].getFields("Name").toList.head.asInstanceOf[JsString].value
    }

    val features = jsonFeatureCollection.getAllFeatures[GeometryCollectionFeature[String]]

    val tree = new STRtree()
    features.foreach { f =>
      f.geom.geometries.map(_.envelope.jtsEnvelope)
        .foreach(e => tree.insert(e, f.data))
    }

    tree
  }
}