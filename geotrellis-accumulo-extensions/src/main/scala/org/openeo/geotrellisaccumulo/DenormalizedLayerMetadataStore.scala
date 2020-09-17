package org.openeo.geotrellisaccumulo

import java.time.ZonedDateTime

import cats.syntax.either._
import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.store.accumulo.AccumuloLayerReader
import geotrellis.store.{AttributeStore, LayerId}
import geotrellis.store.accumulo.{AccumuloAttributeStore, AccumuloInstance}
import geotrellis.vector.ProjectedExtent
import io.circe.parser._
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

object DenormalizedLayerMetadataStore {
  private val InstantsAttribute = "instants"

  def apply(accumuloInstance: AccumuloInstance)(implicit sc: SparkContext): DenormalizedLayerMetadataStore =
    new DenormalizedLayerMetadataStore(accumuloInstance)(sc)
}

class DenormalizedLayerMetadataStore(accumuloInstance: AccumuloInstance)(implicit sc: SparkContext) {
  import DenormalizedLayerMetadataStore.InstantsAttribute

  def refresh(accumuloLayerName: String): (ProjectedExtent, Array[ZonedDateTime]) = {
    val attributeStore = AccumuloAttributeStore(accumuloInstance)

    val layerIds = attributeStore.layerIds
      .filter(_.name == accumuloLayerName)

    val minZoomLayerId = layerIds.minBy(_.zoom)
    val multiBand = attributeStore.isMultiBand(minZoomLayerId)

    val reader = AccumuloLayerReader(accumuloInstance)

    val layerKeys = if (multiBand) reader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](minZoomLayerId).keys
    else reader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](minZoomLayerId).keys

    val uniqueDates: Array[ZonedDateTime] =
      layerKeys.map(_.temporalKey)
        .distinct()
        .sortBy(identity)
        .map(_.time)
        .collect()

    for (layerId <- layerIds) {
      attributeStore.write[Array[ZonedDateTime]](layerId, InstantsAttribute, uniqueDates)
    }

    val projectedExtent = {
      val metadata = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](minZoomLayerId)
      ProjectedExtent(metadata.extent, metadata.crs)
    }

    (projectedExtent, uniqueDates)
  }

  def load(accumuloLayerName: String): Option[(ProjectedExtent, Array[ZonedDateTime])] = {
    val attributeStore = AccumuloAttributeStore(accumuloInstance)

    val layerIds = attributeStore.layerIds
      .filter(_.name == accumuloLayerName)

    if (layerIds.isEmpty) None
    else {
      val maxZoomLayerId = layerIds.maxBy(_.zoom)

      val scanner = attributeStore.connector.createScanner(attributeStore.attributeTable, new Authorizations())

      try {
        scanner.setRange(new Range(attributeStore.layerIdText(maxZoomLayerId)))

        val layerAttributes = scanner.iterator.asScala.toArray

        val bounds = layerAttributes
          .find(entry => entry.getKey.getColumnFamily == new Text(AttributeStore.Fields.metadata))
          .map { entry =>
            val (_, metadata) = parse(entry.getValue.toString).flatMap(_.as[(LayerId, TileLayerMetadata[SpaceTimeKey])]).valueOr(throw _)
            ProjectedExtent(metadata.extent, metadata.crs)
          }

        val instants = layerAttributes
          .find(entry => entry.getKey.getColumnFamily == new Text(InstantsAttribute))
          .map { entry =>
            val (_, instants) = parse(entry.getValue.toString).flatMap(_.as[(LayerId, Array[ZonedDateTime])]).valueOr(throw _)
            instants
          }
          .getOrElse(Array())

        bounds.map(extent => (extent, instants))
      } finally {
        scanner.close()
      }
    }
  }
}
