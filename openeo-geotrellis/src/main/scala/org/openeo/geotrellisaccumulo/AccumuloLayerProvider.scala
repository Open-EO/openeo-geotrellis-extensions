package org.openeo.geotrellisaccumulo

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.spark.store.accumulo.AccumuloLayerReader
import geotrellis.spark.{MultibandTileLayerRDD, TileLayerRDD}
import geotrellis.store.accumulo.AccumuloInstance
import geotrellis.store.avro.AvroRecordCodec
import geotrellis.store.{Between, Intersects, LayerId, LayerQuery}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.aggregate_polygon.intern.PixelRateValidator
import org.openeo.geotrellis.layers.LayerProvider

import scala.reflect.ClassTag

class AccumuloLayerProvider(val layerName: String)(implicit accumuloSupplier: () => AccumuloInstance) extends LayerProvider {

  override def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerRDD[SpaceTimeKey] =
    readLayer(from, to, boundingBox, zoom, sc)

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    readLayer(from, to, boundingBox, zoom, sc)

  private def readLayer[V: AvroRecordCodec: ClassTag](from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int, sc: SparkContext): RDD[(SpaceTimeKey, V)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val (reader, layerId, metadata) = readCache.getUnchecked((zoom, sc))

    PixelRateValidator(boundingBox, from, to, metadata, sc)

    val query = {
      val temporalQuery = new LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]]
        .where(Between(from, to))

      if (boundingBox == null) temporalQuery
      else temporalQuery.where(Intersects(boundingBox.reproject(metadata.crs)))
    }

    reader.read[SpaceTimeKey, V, TileLayerMetadata[SpaceTimeKey]](layerId, query)
  }

  override def readMetadata(zoom: Int, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    readCache.getUnchecked((zoom, sc))._3
  }

  override def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])] =
    DenormalizedLayerMetadataStore(accumuloSupplier())(sc).load(layerName)

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) =
    DenormalizedLayerMetadataStore(accumuloSupplier())(sc).refresh(layerName)

  override def toString: String = s"${getClass.getSimpleName}($layerName)"

  private val readCache: LoadingCache[(Int, SparkContext), (AccumuloLayerReader, LayerId, TileLayerMetadata[SpaceTimeKey])] = {
    CacheBuilder.newBuilder()
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(
        new CacheLoader[(Int, SparkContext), (AccumuloLayerReader, LayerId, TileLayerMetadata[SpaceTimeKey])] {
          def load(key: (Int, SparkContext)): (AccumuloLayerReader, LayerId, TileLayerMetadata[SpaceTimeKey]) = {
            val reader = AccumuloLayerReader(accumuloSupplier())(key._2)
            val attributeStore = reader.attributeStore

            val layerId = attributeStore.baseLayer(layerName, suggestedZoom = key._1)
            val metadata = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId)

            (reader, layerId, metadata)
          }
        })
  }
}
