package geopyspark.geotrellis.io

import _root_.io.circe.Json
import _root_.io.circe.parser.parse
import _root_.io.circe.syntax._
import cats.syntax.either._
import geotrellis.layer._
import geotrellis.store._
import geotrellis.store.cog._

/**
  * Base wrapper class for various types of attribute store wrappers.
  */
class AttributeStoreWrapper(uri: String) {
  val attributeStore: AttributeStore = AttributeStore(uri)

  def readMetadata(name: String, zoom: Int): String = {
    val id = LayerId(name, zoom)
    val header = produceHeader(attributeStore, id)

    val json =
      header.layerType match {
        case COGLayerType =>
          header.keyClass match {
            case "geotrellis.layer.SpatialKey" =>
              attributeStore
                .readMetadata[COGLayerStorageMetadata[SpatialKey]](LayerId(id.name, 0))
                .metadata
                .tileLayerMetadata(id.zoom)
                .asJson
            case "geotrellis.layer.SpaceTimeKey" =>
              attributeStore
                .readMetadata[COGLayerStorageMetadata[SpaceTimeKey]](LayerId(id.name, 0))
                .metadata
                .tileLayerMetadata(id.zoom)
                .asJson
          }
        case _ => attributeStore.readMetadata[Json](id)
      }
    json.noSpaces
  }

  /** Read any attribute store value as JSON object.
   * Returns null if attribute is not found in the store.
   */
  def read(layerName: String, zoom: Int, attributeName: String): String = {
    val id = LayerId(layerName, zoom)
    try {
      val json = attributeStore.read[Json](id, attributeName)
      return json.noSpaces
    } catch {
      case e: AttributeNotFoundError =>
        return null
    }
  }

  /** Write JSON formatted string into catalog */
  def write(layerName: String, zoom: Int, attributeName: String, value: String): Unit = {
    val id = LayerId(layerName, zoom)
    if (value == null) return
    val json = parse(value).valueOr(throw _) // ensure we actually have JSON here
    attributeStore.write(id, attributeName, json)
  }

  def delete(layerName: String, zoom: Int, name: String): Unit = {
    val id = LayerId(layerName, zoom)
    attributeStore.delete(id, name)
  }

  def delete(layerName: String, zoom: Int): Unit = {
    val id = LayerId(layerName, zoom)
    attributeStore.delete(id)
  }

  def contains(layerName: String, zoom: Int): Boolean = {
    val id = LayerId(layerName, zoom)
    attributeStore.layerExists(id)
  }
}
