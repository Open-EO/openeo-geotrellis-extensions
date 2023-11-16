package geopyspark.geotrellis

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.util._
import geotrellis.vector._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._

import scala.collection.JavaConverters._
import scala.reflect._


/**
 * RDD of Rasters, untiled and unsorted
 */
abstract class RasterLayer[K](implicit ev0: ClassTag[K], ev1: Component[K, ProjectedExtent]) extends TileLayer[K] with Serializable {
  def rdd: RDD[(K, MultibandTile)]

  def repartition(numPartitions: Int): RasterLayer[K] = withRDD(rdd.repartition(numPartitions))

  def partitionBy(partitionStrategy: PartitionStrategy): RasterLayer[K] =
    withRDD(rdd.partitionBy(partitionStrategy.producePartitioner(rdd.getNumPartitions).get))

  def toProtoRDD(): JavaRDD[Array[Byte]]

  def collectKeys(): java.util.ArrayList[Array[Byte]]

  def bands(band: Int): RasterLayer[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(band) })

  def bands(bands: java.util.ArrayList[Int]): RasterLayer[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(bands.asScala) })

  def collectMetadata(layoutType: LayoutType): String
  def collectMetadata(layoutDefinition: LayoutDefinition): String

  def convertDataType(newType: String): RasterLayer[K] =
    withRDD(rdd.mapValues { _.convert(CellType.fromName(newType)) })

  def withNoData(newNoData: Double): RasterLayer[K] =
    withRDD(rdd.mapValues { _.withNoData(Some(newNoData)) })

  def tileToLayout(
    tileLayerMetadata: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[_]

  def tileToLayout(
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[_]

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[_]

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): RasterLayer[K]

  def reproject(
    targetCRS: String,
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[_]

  def reproject(
    targetCRS: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[_]

  protected def withRDD(result: RDD[(K, MultibandTile)]): RasterLayer[K]

  def merge(partitionStrategy: PartitionStrategy): RasterLayer[K] =
    partitionStrategy match {
      case ps: PartitionStrategy => withRDD(rdd.merge(ps.producePartitioner(rdd.getNumPartitions)))
      case null => withRDD(rdd.merge())
    }
}
