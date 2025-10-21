package geopyspark.geotrellis.io

import geopyspark.geotrellis._
import geopyspark.util.PythonTranslator
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.store._
import geotrellis.store.cog._
import protos.tileMessages._

import java.time.ZonedDateTime



/**
  * General interface for reading.
  */
class ValueReaderWrapper(uri: String) {
  val attributeStore = AttributeStore(uri)

  lazy val cogReader: COGValueReader[LayerId] = COGValueReader(uri)
  lazy val avroReader: ValueReader[LayerId] = ValueReader(uri)

  def getValueClass(id: LayerId): String =
    attributeStore.readHeader[LayerHeader](id).valueClass

  def readTile(
    layerName: String,
    zoom: Int,
    col: Int,
    row: Int,
    zdt: String
  ): Array[Byte] = {
    val id = LayerId(layerName, zoom)

    val header = produceHeader(attributeStore, id)

    val valueReader: Either[COGValueReader[LayerId], ValueReader[LayerId]] =
      header.layerType match {
        case COGLayerType => Left(cogReader)
        case _ => Right(avroReader)
      }

    try {
      (header.keyClass, header.valueClass) match {
        case ("geotrellis.layer.SpatialKey", "geotrellis.raster.Tile") => {
          val spatialKey = SpatialKey(col, row)
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpatialKey, Tile](id).read(spatialKey)
            case Right(avroReader) => avroReader.reader[SpatialKey, Tile](id).read(spatialKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](MultibandTile(result))
        }
        case ("geotrellis.layer.SpatialKey", "geotrellis.raster.MultibandTile") => {
          val spatialKey = SpatialKey(col, row)
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpatialKey, MultibandTile](id).read(spatialKey)
            case Right(avroReader) => avroReader.reader[SpatialKey, MultibandTile](id).read(spatialKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](result)
        }
        case ("geotrellis.layer.SpaceTimeKey", "geotrellis.raster.Tile") => {
          val spaceKey = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpaceTimeKey, Tile](id).read(spaceKey)
            case Right(avroReader) => avroReader.reader[SpaceTimeKey, Tile](id).read(spaceKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](MultibandTile(result))
        }
        case ("geotrellis.layer.SpaceTimeKey", "geotrellis.raster.MultibandTile") => {
          val spaceKey = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpaceTimeKey, MultibandTile](id).read(spaceKey)
            case Right(avroReader) => avroReader.reader[SpaceTimeKey, MultibandTile](id).read(spaceKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](result)
        }
      }
    } catch {
      case e: ValueNotFoundError => return null
    }
  }
}
