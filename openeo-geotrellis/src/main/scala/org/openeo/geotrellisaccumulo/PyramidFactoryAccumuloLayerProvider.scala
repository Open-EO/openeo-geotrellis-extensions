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
    val (pyramidFactory, metadata) = readCache.getUnchecked(zoom)

    PixelRateValidator(boundingBox, from, to, metadata, sc)

    boundingBox.crs.toString()

    val (extent, srs) = if (boundingBox != null) {
      if (boundingBox.crs.epsgCode.isDefined) {
        (boundingBox.extent, s"EPSG:${boundingBox.crs.epsgCode.get}")
      } else {
        (boundingBox.reproject(LatLng), "EPSG:4326")
      }
    } else {
      (LatLng.worldExtent, "EPSG:4326")
    }

    val pyramidSeq = pyramidFactory.pyramid_seq(layerName, extent, srs, Some(from), Some(to))
    pyramidSeq.find(_._1 == zoom).getOrElse(pyramidSeq.sortBy(_._1).reverse.head)._2
  }

  override def readMetadata(zoom: Int, sc: SparkContext): TileLayerMetadata[SpaceTimeKey] = {
    readCache.getUnchecked(zoom)._2
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
            val pyramidFactory = new PyramidFactory("hdp-accumulo-instance", "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")

            val attributeStore = AccumuloAttributeStore(accumuloSupplier())
            val layerId = attributeStore.baseLayer(layerName, suggestedZoom = zoom)
            val metadata = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerId)

            (pyramidFactory, metadata)
          }
        })
  }
}
