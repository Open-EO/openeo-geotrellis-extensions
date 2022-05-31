package org.openeo.geotrellisaccumulo

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import geotrellis.layer._
import geotrellis.proj4.LatLng
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, TileLayerRDD}
import geotrellis.store.accumulo.{AccumuloAttributeStore, AccumuloInstance}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.openeo.geotrellis.aggregate_polygon.intern.PixelRateValidator
import org.openeo.geotrellis.layers.LayerProvider

import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

class PyramidFactoryAccumuloLayerProvider(val layerName: String)(implicit accumuloSupplier: () => AccumuloInstance) extends LayerProvider {

  override def readTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): TileLayerRDD[SpaceTimeKey] = {
    val rdd = readLayer(from, to, boundingBox, zoom, sc)
    ContextRDD(rdd.mapValues(_.band(0)), rdd.metadata)
  }

  override def readMultibandTileLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int = Int.MaxValue, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] =
    readLayer(from, to, boundingBox, zoom, sc)

  private def readLayer(from: ZonedDateTime, to: ZonedDateTime, boundingBox: ProjectedExtent = null, zoom: Int, sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val (pyramidFactory, metadata) = readCache.get(zoom)

    PixelRateValidator(boundingBox, from, to, metadata, sc)

    val (extent, srs) =
      if (boundingBox == null) (LatLng.worldExtent, "EPSG:4326")
      else boundingBox.crs.epsgCode match {
        case Some(epsgCode) => (boundingBox.extent, s"EPSG:$epsgCode")
        case _ => (boundingBox.reproject(LatLng), "EPSG:4326")
      }

    val pyramid = pyramidFactory.pyramid_seq(layerName, extent, srs, Some(from), Some(to))

    val (_, layer) = pyramid
      .find { case (z, _) => z == zoom }
      .getOrElse(pyramid.maxBy { case (z, _) => z })

    layer
  }

  override def readMetadata(zoom: Int, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    val (_, metadata) = readCache.get(zoom)
    metadata
  }

  override def loadMetadata(sc: SparkContext): Option[(ProjectedExtent, Array[ZonedDateTime])] =
    DenormalizedLayerMetadataStore(accumuloSupplier())(sc).load(layerName)

  override def collectMetadata(sc: SparkContext): (ProjectedExtent, Array[ZonedDateTime]) =
    DenormalizedLayerMetadataStore(accumuloSupplier())(sc).refresh(layerName)

  override def toString: String = s"${getClass.getSimpleName}($layerName)"

  // TODO: is this caching necessary?
  private val readCache: LoadingCache[Integer, (PyramidFactory, TileLayerMetadata[SpaceTimeKey])] = {
    CacheBuilder.newBuilder()
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(
        new CacheLoader[Integer, (PyramidFactory, TileLayerMetadata[SpaceTimeKey])] {
          def load(zoom: Integer): (PyramidFactory, TileLayerMetadata[SpaceTimeKey]) = {
            val accumuloInstance = accumuloSupplier()

            val pyramidFactory = new PyramidFactory(accumuloInstance)

            val attributeStore = AccumuloAttributeStore(accumuloInstance)
            val layerId = attributeStore.baseLayer(layerName, suggestedZoom = zoom)
            val metadata = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId)

            (pyramidFactory, metadata)
          }
        })
  }
}
