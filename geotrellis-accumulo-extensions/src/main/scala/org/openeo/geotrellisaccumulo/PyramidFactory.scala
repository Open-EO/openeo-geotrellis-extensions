package org.openeo.geotrellisaccumulo

import java.time.ZonedDateTime

import be.vito.eodata.extracttimeseries.geotrellis.ComputeStatsGeotrellisHelpers
import be.vito.eodata.geopysparkextensions.KerberizedAccumuloInstance
import geotrellis.layer.{Bounds, EmptyBounds, KeyBounds, Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.store.accumulo.{AccumuloAttributeStore, AccumuloKeyEncoder, AccumuloLayerHeader}
import geotrellis.store.avro.AvroRecordCodec
import geotrellis.store.{LayerQuery, _}
import geotrellis.util._
import geotrellis.vector._
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.data.{Range => AccumuloRange}
import org.apache.accumulo.core.util.{Pair => AccumuloPair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.reflect.ClassTag

object PyramidFactory {

  val zoomRangeCache = new TtlCache[String, (Int, Int)](ttl = 3600)

}

  class PyramidFactory(instanceName: String, zooKeeper: String) {

    var splitRanges: Boolean = false

    def setSplitRanges(split: Boolean): Unit ={
      splitRanges = split
    }

    private def zoomRange(layerName: String): (Int, Int) = {
      PyramidFactory.zoomRangeCache.getOrElseUpdate(
        layerName,
        {
          val layerIds = AccumuloAttributeStore(accumuloInstance).layerIds.filter(_.name == layerName)
          (layerIds.minBy(_.zoom).zoom, layerIds.maxBy(_.zoom).zoom)
        }
      )
    }

    private def accumuloInstance = {
      KerberizedAccumuloInstance(zooKeeper,instanceName)
    }

    def rdd[V : AvroRecordCodec: ClassTag](layerName:String,zoom:Int=0,tileQuery: LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]] = new LayerQuery[SpaceTimeKey,TileLayerMetadata[SpaceTimeKey]]() ): RDD[(SpaceTimeKey, V)] with Metadata[TileLayerMetadata[SpaceTimeKey]] ={
      implicit val sc = SparkContext.getOrCreate()
      val id = LayerId(layerName, zoom)

      val attributeStore = AccumuloAttributeStore(accumuloInstance)
      if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

      val LayerAttributes(header, metadata, keyIndex, writerSchema) = try {
        attributeStore.readLayerAttributes[AccumuloLayerHeader, TileLayerMetadata[SpaceTimeKey], SpaceTimeKey](id)
      } catch {
        case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
      }

      val queryKeyBounds = tileQuery(metadata)
      val layerBounds = metadata.getComponent[Bounds[SpaceTimeKey]]
      val layerMetadata = metadata.setComponent[Bounds[SpaceTimeKey]](queryKeyBounds.foldLeft(EmptyBounds: Bounds[SpaceTimeKey])(_ combine _))

      val table = header.tileTable

      val decompose: KeyBounds[SpaceTimeKey] => Seq[AccumuloRange] =
        if(queryKeyBounds.size == 1 && queryKeyBounds.head.contains(layerBounds)) {
          // This query is asking for all the keys of the layer;
          // avoid a heavy set of accumulo ranges by not setting any at all,
          // which equates to a full request.
          { _ => Seq(new AccumuloRange()) }
        } else {
          (bounds: KeyBounds[SpaceTimeKey]) => {
            keyIndex.indexRanges(bounds).map { case (min, max) =>
              new AccumuloRange(new Text(AccumuloKeyEncoder.long2Bytes(min)), new Text(AccumuloKeyEncoder.long2Bytes(max)))
            }
          }
        }

      val job = Job.getInstance(sc.hadoopConfiguration)

      accumuloInstance.setAccumuloConfig(job)

      InputFormatBase.setInputTableName(job, table)

      val ranges = queryKeyBounds.flatMap(decompose).asJava
      InputFormatBase.setRanges(job, ranges)
      InputFormatBase.fetchColumns(job, List(new AccumuloPair(new Text(geotrellis.store.accumulo.columnFamily(id)), null: Text)).asJava)
      InputFormatBase.setBatchScan(job, true)

      val configuration = job.getConfiguration
      val rdd = new GeotrellisAccumuloRDD(sc,configuration,splitRanges)

      return new GeotrellisRasterRDD[V](keyIndex,writerSchema,rdd,layerMetadata,sc)
    }

    def pyramid_seq(layerName:String,bbox: Extent, bbox_srs: String,startDate: String, endDate:String ): immutable.Seq[(Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]])] = {
      val start = if(startDate!=null ) Some(ZonedDateTime.parse(startDate)) else Option.empty
      val end = if(endDate!=null) Some(ZonedDateTime.parse(endDate)) else Option.empty
      return pyramid_seq(layerName,bbox,bbox_srs,start,end)
    }

    def pyramid_seq(layerName: String, polygons: Array[MultiPolygon], polygons_crs: CRS, startDate: String, endDate: String): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
      val query = {
        // 500m buffer for kernel operations
        val bufferDistanceInMeters = 500.0
        val bufferDistance = Extent(0.0, 0.0, bufferDistanceInMeters, 1.0).reproject(WebMercator, polygons_crs).width

        val intersectsPolygons = polygons
          .map(polygon =>
            polygon.buffer(bufferDistance) match {
              case polygon: Polygon => MultiPolygon(polygon)
              case multiPolygon: MultiPolygon => multiPolygon
            })
          .map(bufferedPolygon => Intersects(bufferedPolygon, polygons_crs): LayerFilter.Expression[Intersects.type, (MultiPolygon, CRS)])
          .reduce(_ or _)

        val spatialQuery = new LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]]
          .where(intersectsPolygons)

        (Option(startDate).map(ZonedDateTime.parse), Option(endDate).map(ZonedDateTime.parse)) match {
          case (Some(start), Some(end)) => spatialQuery.where(Between(start, end))
          case _ => spatialQuery
        }
      }

      val sc = SparkContext.getOrCreate()

      ComputeStatsGeotrellisHelpers.authenticated(sc) {
        val (minLevel: Int, maxLevel: Int) = zoomRange(layerName)

        val attributeStore = AccumuloAttributeStore(accumuloInstance)
        val id = LayerId(layerName, maxLevel)
        if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

        val header: LayerHeader = attributeStore.readHeader[LayerHeader](id)

        val pyramid = for (z <- maxLevel to minLevel by -1) yield {
          header.valueClass match {
            case "geotrellis.raster.Tile" =>
              (z, rdd[Tile](layerName, z, query).withContext(_.mapValues(MultibandTile(_))))
            case "geotrellis.raster.MultibandTile" =>
              (z, rdd[MultibandTile](layerName, z, query))
          }
        }

        pyramid
      }
    }

    def load_rdd(layerName:String,level:Int,bbox: Extent, bbox_srs: String,startDate: Option[ZonedDateTime]=Option.empty, endDate:Option[ZonedDateTime]=Option.empty ): MultibandTileLayerRDD[SpaceTimeKey]  = {
      val attributeStore = AccumuloAttributeStore(accumuloInstance)
      val id = LayerId(layerName, level)
      if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

      val header: LayerHeader = attributeStore.readHeader[LayerHeader](id)
      val query = createQuery(attributeStore,id,bbox,bbox_srs,startDate,endDate)
      implicit val sc = SparkContext.getOrCreate()

      val result: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ComputeStatsGeotrellisHelpers.authenticated(sc) {
        header.valueClass match {
          case "geotrellis.raster.Tile" =>
            rdd[Tile](layerName, level, query).withContext(_.mapValues {
              MultibandTile(_)
            })
          case "geotrellis.raster.MultibandTile" =>
            rdd[MultibandTile](layerName, level, query)
        }
      }
      return new ContextRDD(result,result.metadata)
    }

    def createQuery(attributeStore: AccumuloAttributeStore, id: LayerId, bbox: Extent, bbox_srs: String, startDate: Option[ZonedDateTime], endDate: Option[ZonedDateTime]): LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]] = {
      val metadata = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](id)

      val extent = ProjectedExtent(bbox, CRS.fromName(bbox_srs)).reproject(metadata.crs)
      if (!metadata.extent.intersects(extent)) {
        throw new IllegalArgumentException("Requested bounding box does not intersect the layer. Layer: " + id.name + ".Bounding box: '" + extent + "' Layer bounding box: " + metadata.extent)
      }

      var query = new LayerQuery[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]]
      query = query.where(Intersects(extent))
      //startDate can be null because py4j calls this method directly
      if (startDate != null && endDate != null && startDate.isDefined && endDate.isDefined) {
        query = query.where(Between(startDate.get, endDate.get))
      }
      return query
    }


    def pyramid_seq(layerName:String, bbox: Extent, bbox_srs: String, startDate: Option[ZonedDateTime]=Option.empty, endDate:Option[ZonedDateTime]=Option.empty ): immutable.Seq[(Int, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]])] = {
      val (minLevel:Int, maxLevel:Int) = zoomRange(layerName)
      val attributeStore = AccumuloAttributeStore(accumuloInstance)
      val id = LayerId(layerName, maxLevel)
      if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)

      val header: LayerHeader = attributeStore.readHeader[LayerHeader](id)

      val query = createQuery(attributeStore,id,bbox,bbox_srs,startDate,endDate)

      implicit val sc = SparkContext.getOrCreate()

      return ComputeStatsGeotrellisHelpers.authenticated(sc)({
        val seq = for (z <- maxLevel to minLevel by -1) yield {
          header.valueClass match {
            case "geotrellis.raster.Tile" =>
              (z, rdd[Tile](layerName, z,query).withContext(_.mapValues{MultibandTile(_)}))
            case "geotrellis.raster.MultibandTile" =>
              (z, rdd[MultibandTile](layerName, z,query))
          }

        }
        return seq
      })




      //val reader = AccumuloLayerReader(accumuloInstance)
      //Pyramid.fromLayerReader[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]](layerName,reader)
    }

    def pyramid(layerName:String,bbox: Extent,bbox_srs: String,startDate: String, endDate:String ): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] = {
        return pyramid(layerName,bbox,bbox_srs,Some(ZonedDateTime.parse(startDate)),Some(ZonedDateTime.parse(endDate)))
    }

    def pyramid(layerName:String,bbox: Extent,bbox_srs: String,startDate: Option[ZonedDateTime]=Option.empty, endDate:Option[ZonedDateTime]=Option.empty ): Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] = {

      val seq = pyramid_seq(layerName,bbox,bbox_srs,startDate,endDate)
      return Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]](seq.toMap)

      //val reader = AccumuloLayerReader(accumuloInstance)
      //Pyramid.fromLayerReader[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]](layerName,reader)
    }

    def lookup(level: Int, pyramid :Pyramid[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] ): Unit ={

      /*val codec = KryoWrapper(KeyValueRecordCodec[SpaceTimeKey,MultibandTile], MultibandTile)

      val sc = SparkContext.getOrCreate()
      val job = Job.getInstance(sc.hadoopConfiguration)
      accumuloInstance.setAccumuloConfig(job)
      InputFormatBase.setInputTableName(job, table)

      val ranges = queryKeyBounds.flatMap(decomposeBounds).asJava
      InputFormatBase.setRanges(job, ranges)
      InputFormatBase.fetchColumns(job, List(new Nothing(columnFamily, null))) TextasJava
        InputFormatBase.setBatchScan(job, true)

      val kwWriterSchema = KryoWrapper(writerSchema)
      val accumuloRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[AccumuloInputFormat], classOf[SpaceTimeKey], classOf[MultibandTile])
      accumuloRDD.asInstanceOf[NewHadoopRDD].mapPartitionsWithInputSplit()*/

      // val tile = pyramid.lookup(0,SpaceTimeKey(0,0,ZonedDateTime.now()))
      val rdd = pyramid.level(8)
      val partitions = rdd.partitions
      //partitions.filter(p.asInstanceOf[NewHadoopPartition])
      println(partitions)
      //partitions.filter()
      //PartitionPruningRDD.create()
      //println(tile)
    }
  }
