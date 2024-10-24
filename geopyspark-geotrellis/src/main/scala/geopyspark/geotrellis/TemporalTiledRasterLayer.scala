package geopyspark.geotrellis

import geopyspark.util._
import geopyspark.geotrellis._
import geopyspark.geotrellis.GeoTrellisUtils._

import protos.tileMessages._
import protos.keyMessages._
import protos.tupleMessages._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.distance._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression._
import geotrellis.raster.mapalgebra.focal.{Square, Slope}
import geotrellis.raster.mapalgebra.focal.hillshade._
import geotrellis.raster.rasterize._
import geotrellis.raster.render._
import geotrellis.raster.resample.{ResampleMethod, PointResampleMethod, Resample}
import geotrellis.raster.buffer.BufferedTile
import geotrellis.spark._
import geotrellis.spark.buffer._
import geotrellis.spark.costdistance.IterativeCostDistance
import geotrellis.spark.filter._
import geotrellis.store._
import geotrellis.store.json._
import geotrellis.spark.mapalgebra.local._
import geotrellis.spark.mapalgebra.focal._
import geotrellis.layer.mask.Mask
import geotrellis.spark.mask.MaskRDD
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.util._
import geotrellis.layer._
import geotrellis.util._
import geotrellis.vector._
import geotrellis.vector.io.wkb.WKB
import geotrellis.vector.triangulation._
import geotrellis.vector.voronoi._

import _root_.io.circe.syntax._
import _root_.io.circe.parser.parse
import cats.syntax.either._
import spire.syntax.cfor._

import org.locationtech.jts.geom.Coordinate

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import java.util.ArrayList
import java.time.{ZonedDateTime, ZoneId}

import scala.reflect._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


class TemporalTiledRasterLayer(
  val zoomLevel: Option[Int],
  val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
) extends TiledRasterLayer[SpaceTimeKey] {

  def resample_to_power_of_two(
    col_power: Int,
    row_power: Int,
    resampleMethod: ResampleMethod
  ): TiledRasterLayer[SpaceTimeKey] = {
    val cols = 1<<col_power
    val rows = 1<<row_power
    val rdd2 = rdd.mapValues({ tile => tile.resample(cols, rows, resampleMethod) })

    val metadata = rdd.metadata
    val layout = LayoutDefinition(
      metadata.extent,
      TileLayout(
        metadata.layout.tileLayout.layoutCols,
        metadata.layout.tileLayout.layoutRows,
        cols,
        rows
      )
    )
    val metadata2 = metadata.copy(layout = layout)

    TemporalTiledRasterLayer(None, ContextRDD(rdd2, metadata2))
  }

  def mask(geometries: Seq[MultiPolygon]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MaskRDD(rdd, geometries, Mask.Options.DEFAULT))

  def mask(
    groupedRDD: RDD[(SpatialKey, Iterable[Geometry])],
    options: Rasterizer.Options
  ): TiledRasterLayer[SpaceTimeKey] = {
    val mapTrans = rdd.metadata.layout.mapTransform

    val rekeyedRDD: RDD[(SpatialKey, (SpaceTimeKey, MultibandTile))] =
      rdd.map { case (k, v) => (k.getComponent[SpatialKey], (k, v)) }

    val joinedRDD: RDD[(SpatialKey, ((SpaceTimeKey, MultibandTile), Iterable[Geometry]))] =
      groupedRDD.partitioner match {
        case Some(p) =>
          rekeyedRDD.join(groupedRDD, p)
        case None =>
          rekeyedRDD.join(groupedRDD)
      }

    val maskedRDD: RDD[(SpaceTimeKey, MultibandTile)] =
      joinedRDD.mapPartitions ({ partition =>
        partition.map { case (k, ((tk, v), geoms)) =>
          (tk, v.mask(mapTrans(k), geoms, options))
        }
      }, preservesPartitioning = true)

    TemporalTiledRasterLayer(zoomLevel, ContextRDD(maskedRDD, rdd.metadata))
  }

  private def wkbsToMultiPolygons(wkbs: java.util.ArrayList[Array[Byte]]) = {
    wkbs
      .asScala.map({ wkb => WKB.read(wkb) })
      .flatMap({
        case p: Polygon => Some(MultiPolygon(p))
        case m: MultiPolygon => Some(m)
        case _ => None
      })
  }

  private def wkbsToMultiPolygon(wkbs: java.util.ArrayList[Array[Byte]]) =
    MultiPolygon(
      wkbsToMultiPolygons(wkbs)
        .map({ mp => mp.polygons })
        .foldLeft(List.empty[Polygon])(_ ++ _)
    )

  def sumSeries(
    wkbs: java.util.ArrayList[Array[Byte]]
  ): Array[(Long, Double)] = {
    val polygon: MultiPolygon = wkbsToMultiPolygon(wkbs)
    val metadata = rdd.metadata
    ContextRDD(rdd.mapValues({ m => m.bands(0) }), metadata)
      .sumSeries(polygon)
      .toArray
      .map { case (dt, v) => (dt.toInstant.toEpochMilli, v) }
      .sortWith({ (t1, t2) => (t1._1.compareTo(t2._1) <= 0) })
  }

  def minSeries(
    wkbs: java.util.ArrayList[Array[Byte]]
  ): Array[(Long, Double)] = {
    val polygon: MultiPolygon = wkbsToMultiPolygon(wkbs)
    val metadata = rdd.metadata
    ContextRDD(rdd.mapValues({ m => m.bands(0) }), metadata)
      .minSeries(polygon)
      .toArray
      .map { case (dt, v) => (dt.toInstant.toEpochMilli, v) }
      .sortWith({ (t1, t2) => (t1._1.compareTo(t2._1) <= 0) })
  }

  def maxSeries(
    wkbs: java.util.ArrayList[Array[Byte]]
  ): Array[(Long, Double)] = {
    val polygon: MultiPolygon = wkbsToMultiPolygon(wkbs)
    val metadata = rdd.metadata
    ContextRDD(rdd.mapValues({ m => m.bands(0) }), metadata)
      .maxSeries(polygon)
      .toArray
      .map { case (dt, v) => (dt.toInstant.toEpochMilli, v) }
      .sortWith({ (t1, t2) => (t1._1.compareTo(t2._1) <= 0) })
  }

  def meanSeries(
    wkbs: java.util.ArrayList[Array[Byte]]
  ): Array[(Long, Double)] = {
    val polygon: MultiPolygon = wkbsToMultiPolygon(wkbs)
    val metadata = rdd.metadata
    ContextRDD(rdd.mapValues({ m => m.bands(0) }), metadata)
      .meanSeries(polygon)
      .toArray
      .map { case (dt, v) => (dt.toInstant.toEpochMilli, v) }
      .sortWith({ (t1, t2) => (t1._1.compareTo(t2._1) <= 0) })
  }

  def histogramSeries(
    wkbs: java.util.ArrayList[Array[Byte]]
  ): Array[(Long, Histogram[Double])] = {
    val polygon: MultiPolygon = wkbsToMultiPolygon(wkbs)
    val metadata = rdd.metadata
    ContextRDD(rdd.mapValues({ m => m.bands(0) }), metadata)
      .histogramSeries(polygon)
      .toArray
      .map { case (dt, v) => (dt.toInstant.toEpochMilli, v) }
      .sortWith({ (t1, t2) => (t1._1.compareTo(t2._1) <= 0) })
  }

  def reproject(
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TemporalTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)
    val crs = TileLayer.getCRS(targetCRS).get
    val targetLayout = FloatingLayoutScheme(rdd.metadata.layout.tileCols, rdd.metadata.layout.tileRows)
    val (zoom, reprojected) = rdd.reproject(crs, targetLayout, resampleMethod, partitioner)
    TemporalTiledRasterLayer(Some(zoom), reprojected)
  }

  def reproject(
    targetCRS: String,
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TemporalTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)
    val crs = TileLayer.getCRS(targetCRS).get

    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = rdd.reproject(crs, scheme, resampleMethod, partitioner)
        TemporalTiledRasterLayer(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = TileRDDReproject(rdd, crs, Right(scheme.levelForZoom(zoom).layout), resampleMethod, partitioner)
        TemporalTiledRasterLayer(zoom, reprojected)

      case LocalLayout(tileCols, tileRows) =>
        val (_, reprojected) = rdd.reproject(crs, FloatingLayoutScheme(tileCols, tileRows), resampleMethod, partitioner)
        TemporalTiledRasterLayer(None, reprojected)
    }
  }

  def reproject(
    targetCRS: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TemporalTiledRasterLayer = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val (zoom, reprojected) = TileRDDReproject(rdd, TileLayer.getCRS(targetCRS).get, Right(layoutDefinition), resampleMethod, partitioner)
    TemporalTiledRasterLayer(Some(zoom), reprojected)
  }

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    zoom: Option[Int],
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val baseTransform = rdd.metadata.layout.mapTransform
    val targetTransform = layoutDefinition.mapTransform
    val crs = rdd.metadata.crs

    val temporalRDD = rdd.map { case (k, v) =>
      (TemporalProjectedExtent(baseTransform(k), crs, k.instant), v)
    }

    val bounds = rdd.metadata.bounds.get
    val spatialBounds = KeyBounds(targetTransform(rdd.metadata.extent))
    val retiledLayerMetadata = rdd.metadata.copy(
      layout = layoutDefinition,
      bounds = KeyBounds(
        minKey = bounds.minKey.setComponent[SpatialKey](spatialBounds.minKey),
        maxKey = bounds.maxKey.setComponent[SpatialKey](spatialBounds.maxKey)
      )
    )

    val options = getTilerOptions(resampleMethod, partitionStrategy)
    val tileLayer =
      MultibandTileLayerRDD(temporalRDD.tileToLayout(retiledLayerMetadata, options), retiledLayerMetadata)

    TemporalTiledRasterLayer(zoom, tileLayer)
  }

  def pyramid(resampleMethod: ResampleMethod, partitionStrategy: PartitionStrategy): Array[TiledRasterLayer[SpaceTimeKey]] = {
    require(! rdd.metadata.bounds.isEmpty, "Can not pyramid an empty RDD")

    val partitioner =
      partitionStrategy match {
        case ps: PartitionStrategy => ps.producePartitioner(rdd.getNumPartitions)
        case null => None
      }

    val (baseZoom, scheme) =
      zoomLevel match {
        case Some(zoom) =>
          zoom -> ZoomedLayoutScheme(rdd.metadata.crs, rdd.metadata.tileRows)

        case None =>
          val zoom = LocalLayoutScheme.inferLayoutLevel(rdd.metadata.layout)
          zoom -> new LocalLayoutScheme
      }

    Pyramid.levelStream(
      rdd, scheme, baseZoom, 0,
      Pyramid.Options(resampleMethod=resampleMethod, partitioner=partitioner)
    ).map{ x =>
      TemporalTiledRasterLayer(Some(x._1), x._2)
    }.toArray
  }

  def focal(
    operation: String,
    neighborhood: String,
    param1: Double,
    param2: Double,
    param3: Double,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpaceTimeKey] = {
    val singleTileLayerRDD: TileLayerRDD[SpaceTimeKey] = TileLayerRDD(
      rdd.mapValues({ v => v.band(0) }),
      rdd.metadata
    )

    val _neighborhood = getNeighborhood(neighborhood, param1, param2, param3)
    val cellSize = rdd.metadata.layout.cellSize
    val op: ((Tile, Option[GridBounds[Int]]) => Tile) = getOperation(operation, _neighborhood, cellSize, param1)

    val result: TileLayerRDD[SpaceTimeKey] =
      partitionStrategy match {
        case ps: PartitionStrategy =>
          FocalOperation(singleTileLayerRDD, _neighborhood, ps.producePartitioner(rdd.getNumPartitions))(op)
        case null =>
          FocalOperation(singleTileLayerRDD, _neighborhood, None)(op)
      }

    val multibandRDD: MultibandTileLayerRDD[SpaceTimeKey] =
      MultibandTileLayerRDD(result.mapValues{ x => MultibandTile(x) }, result.metadata)

    TemporalTiledRasterLayer(None, multibandRDD)
  }

  def slope(zFactorCalculator: ZFactorCalculator): TemporalTiledRasterLayer = {
    val mt = rdd.metadata.mapTransform
    val cellSize = rdd.metadata.cellSize

    TemporalTiledRasterLayer(
      zoomLevel,
      rdd.withContext { rdd =>
        rdd.bufferTiles(bufferSize = 1).mapPartitions[(SpaceTimeKey, MultibandTile)](
        { iter =>
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val zfactor = zFactorCalculator.deriveZFactor(mt.keyToExtent(key))
            val slopeTile = Slope(tile.bands(0), Square(1), Some(bounds), cellSize, zfactor).interpretAs(FloatConstantNoDataCellType)

            key -> MultibandTile(slopeTile)
          }
        }, preservesPartitioning = true
        )
      }.mapContext(_.copy(cellType = FloatConstantNoDataCellType))
    )
  }

  def hillshade(
    azimuth: Double,
    altitude: Double,
    zFactorCalculator: ZFactorCalculator,
    band: Int
  ): TemporalTiledRasterLayer = {
    val mt = rdd.metadata.mapTransform
    val cellSize = rdd.metadata.cellSize
    val neighborhood = Square(1)
    val gridBounds = rdd.metadata.tileBounds
    val partitioner = rdd.partitioner

    TemporalTiledRasterLayer(
      zoomLevel,
      rdd.withContext { rdd =>
        rdd.bufferTiles(neighborhood.extent, gridBounds, partitioner).mapPartitions[(SpaceTimeKey, MultibandTile)](
        { iter =>
          iter.map { case (key, BufferedTile(tile, bounds)) =>
            val zfactor = zFactorCalculator.deriveZFactor(mt.keyToExtent(key))
            val hillshadeTile = Hillshade(tile.bands(band), neighborhood, Some(bounds), cellSize, azimuth, altitude, zfactor)

            key -> MultibandTile(hillshadeTile)
          }
        }, preservesPartitioning = true
        )
      }.mapContext(_.copy(cellType = ShortConstantNoDataCellType))
    )
  }

  def costDistance(
    sc: SparkContext,
    geometries: Seq[Geometry],
    maxDistance: Double
  ): TiledRasterLayer[SpaceTimeKey] = {
    val singleTileLayer = TileLayerRDD(
      rdd.mapValues({ v => v.band(0) }),
      rdd.metadata
    )

    implicit def convertion(k: SpaceTimeKey): SpatialKey =
      k.spatialKey

    implicit val _sc = sc

    val result: TileLayerRDD[SpaceTimeKey] =
      IterativeCostDistance(singleTileLayer, geometries, maxDistance)

    val multibandRDD: MultibandTileLayerRDD[SpaceTimeKey] =
      MultibandTileLayerRDD(result.mapValues{ x => MultibandTile(x) }, result.metadata)

    TemporalTiledRasterLayer(None, multibandRDD)
  }

  def reclassify(reclassifiedRDD: RDD[(SpaceTimeKey, MultibandTile)]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(reclassifiedRDD, rdd.metadata))

  def reclassifyDouble(reclassifiedRDD: RDD[(SpaceTimeKey, MultibandTile)]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(reclassifiedRDD, rdd.metadata))

  def withRDD(result: RDD[(SpaceTimeKey, MultibandTile)]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(result, rdd.metadata))

  def withContextRDD(
    result: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]
  ): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, result)

  def toInt(converted: RDD[(SpaceTimeKey, MultibandTile)]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(converted, rdd.metadata))

  def toDouble(converted: RDD[(SpaceTimeKey, MultibandTile)]): TiledRasterLayer[SpaceTimeKey] =
    TemporalTiledRasterLayer(zoomLevel, MultibandTileLayerRDD(converted, rdd.metadata))

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(SpaceTimeKey, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(SpaceTimeKey, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(SpaceTimeKey, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    resampleMethod: ResampleMethod,
    decimations: List[Int],
    geoTiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val mapTransform =
      MapKeyTransform(
        rdd.metadata.layout.extent,
        rdd.metadata.layout.layoutCols,
        rdd.metadata.layout.layoutRows
      )

    val crs = rdd.metadata.crs

    val geotiffRDD =
      rdd.map { case (k, v) =>
        val transKey = TemporalProjectedExtent(mapTransform(k), crs, k.instant)

        val geoTiff =
          MultibandGeoTiff(v, transKey.extent, transKey.crs, tags, geoTiffOptions)
            .withOverviews(resampleMethod, decimations)

        (k, geoTiff.toByteArray)
      }

    PythonTranslator.toPython[(SpaceTimeKey, Array[Byte]), ProtoTuple](geotiffRDD)
  }

  def toSpatialLayer(instant: Long): SpatialTiledRasterLayer = {
    val spatialRDD =
      rdd
        .filter { case (key, _) => key.instant == instant }
        .map { x => (x._1.spatialKey, x._2) }

    val (minKey, maxKey) = (spatialRDD.keys.min(), spatialRDD.keys.max())

    val spatialMetadata =
      rdd.metadata.copy(bounds = Bounds(minKey, maxKey))

    SpatialTiledRasterLayer(zoomLevel, ContextRDD(spatialRDD, spatialMetadata))
  }

  def toSpatialLayer(): SpatialTiledRasterLayer = {
    val spatialRDD = rdd.map { x => (x._1.spatialKey, x._2) }

    val bounds = rdd.metadata.bounds.get
    val spatialMetadata =
      rdd.metadata.copy(bounds = Bounds(bounds.minKey.spatialKey, bounds.maxKey.spatialKey))

    SpatialTiledRasterLayer(zoomLevel, ContextRDD(spatialRDD, spatialMetadata))
  }

  def collectKeys(): java.util.ArrayList[Array[Byte]] =
    PythonTranslator.toPython[SpaceTimeKey, ProtoSpaceTimeKey](rdd.keys.collect)

  def getPointValues(
    points: java.util.Map[Long, Array[Byte]],
    resampleMethod: PointResampleMethod
  ): java.util.Map[Long, java.util.Map[String, Array[Double]]] = {
    val mapTrans = rdd.metadata.layout.mapTransform

    val idedKeys: Map[Long, Point] =
      points
        .asScala
        .mapValues { WKB.read(_).asInstanceOf[Point] }
        .toMap

    val pointKeys =
      idedKeys
        .foldLeft(Map[SpatialKey, Array[(Long, Point)]]()) {
          case (acc, elem) =>
            val pointKey = mapTrans(elem._2)

            acc.get(pointKey) match {
              case Some(arr) => acc + (pointKey -> (elem +: arr))
              case None => acc + (pointKey -> Array(elem))
            }
        }

    val matchedKeys =
      resampleMethod match {
        case r: PointResampleMethod => _getPointValues(pointKeys, mapTrans, r)
        case _ => _getPointValues(pointKeys, mapTrans)
      }

    matchedKeys.mapValues { _.asJava }.asJava
  }

  def _getPointValues(
    pointKeys: Map[SpatialKey, Array[(Long, Point)]],
    mapTrans: MapKeyTransform,
    resampleMethod: PointResampleMethod
  ): Map[Long, Map[String, Array[Double]]] = {
    val resamplePoint = (tile: Tile, extent: Extent, point: Point) =>
      Resample(resampleMethod, tile, extent).resampleDouble(point)

    rdd.flatMap { case (k, v) =>
      pointKeys.get(k.getComponent[SpatialKey]) match {
        case Some(arr) =>
          val keyExtent = mapTrans(k)
          val rasterExtent = RasterExtent(keyExtent, v.cols, v.rows)

          arr.map { case (id, point) =>
            (id -> Map(k.time.toString -> v.bands.map { resamplePoint(_, keyExtent, point) }.toArray ))
          }
        case None => Seq()
      }
    }.reduceByKey { case (m1, m2) => m1 ++ m2 }.collect().toMap
  }

  def _getPointValues(
    pointKeys: Map[SpatialKey, Array[(Long, Point)]],
    mapTrans: MapKeyTransform
  ): Map[Long, Map[String, Array[Double]]] =
    rdd.flatMap { case (k, v) =>
      pointKeys.get(k.getComponent[SpatialKey]) match {
        case Some(arr) =>
          val keyExtent = mapTrans(k)
          val rasterExtent = RasterExtent(keyExtent, v.cols, v.rows)

          arr.map { case (id, point) =>
            val (gridCol, gridRow) = rasterExtent.mapToGrid(point)

            val values = Array.ofDim[Double](v.bandCount)

            cfor(0)(_ < v.bandCount, _ + 1){ index =>
              values(index) = v.band(index).getDouble(gridCol, gridRow)
            }

            (id -> Map(k.time.toString -> values))
          }
        case None => Seq()
      }
    }.reduceByKey { case (m1, m2) => m1 ++ m2 }.collect().toMap

  def getCellValueCounts(areaOfInterest: Array[Byte], targetBand: Int): String = {
    val acc = new CountingAccumulator()
    rdd.sparkContext.register(acc)

    val geom =
      areaOfInterest match {
        case arr: Array[Byte] => Some(WKB.read(arr))
        case _ => None
      }

    val md = rdd.metadata

    val singleBandLayer = rdd.withContext { _.mapValues { _.band(targetBand) } }

    singleBandLayer.foreach { case (k, v) =>
      def extent = md.mapTransform(k.getComponent[SpatialKey])
      val rasterExtent = RasterExtent(extent, md.tileCols, md.tileRows)

      Rasterizer.foreachCellByGeometry(geom.getOrElse(extent), rasterExtent) { case (col, row) =>
        acc.add(v.get(col, row).toInt)
      }
    }

    acc
      .value
      .groupBy(_._1)
      .map { case (k, vs) => k.toString -> vs.map { case (_, v) => v }.sum.toString }
      .asJson
      .noSpaces
  }

  def filterByTimes(
    times: java.util.ArrayList[String]
  ): TemporalTiledRasterLayer = {
    val bounds: KeyBounds[SpatialKey] = KeyBounds(rdd.metadata.tileBounds)
    val minKey = bounds.minKey
    val maxKey = bounds.maxKey
    val timeBoundaries: Array[KeyBounds[SpaceTimeKey]] =
      times
        .asScala
        .grouped(2)
        .map { list =>
          list match {
            case scala.collection.mutable.Buffer(a, b) =>
              KeyBounds(
                SpaceTimeKey(minKey.col, minKey.row, ZonedDateTime.parse(a)),
                SpaceTimeKey(maxKey.col, maxKey.row, ZonedDateTime.parse(b))
              )
            case scala.collection.mutable.Buffer(a) =>
              KeyBounds(
                SpaceTimeKey(minKey.col, minKey.row, ZonedDateTime.parse(a)),
                SpaceTimeKey(maxKey.col, maxKey.row, ZonedDateTime.parse(a))
              )
          }
        }.toArray

    val filteredRDD = rdd.filterByKeyBounds(timeBoundaries)

    TemporalTiledRasterLayer(zoomLevel, filteredRDD)
  }
}


object TemporalTiledRasterLayer {
  def fromProtoEncodedRDD(
    javaRDD: JavaRDD[Array[Byte]],
    metadata: String
  ): TemporalTiledRasterLayer = {
    val md = parse(metadata).valueOr(throw _).as[TileLayerMetadata[SpaceTimeKey]].valueOr(throw _)
    val tileLayer = MultibandTileLayerRDD(
      PythonTranslator.fromPython[(SpaceTimeKey, MultibandTile), ProtoTuple](javaRDD, ProtoTuple.parseFrom), md)

    TemporalTiledRasterLayer(None, tileLayer)
  }

  def fromProtoEncodedRDD(
    javaRDD: JavaRDD[Array[Byte]],
    zoomLevel: Int,
    metadata: String
  ): TemporalTiledRasterLayer = {
    val md = parse(metadata).valueOr(throw _).as[TileLayerMetadata[SpaceTimeKey]].valueOr(throw _)
    val tileLayer = MultibandTileLayerRDD(
      PythonTranslator.fromPython[(SpaceTimeKey, MultibandTile), ProtoTuple](javaRDD, ProtoTuple.parseFrom), md)

    TemporalTiledRasterLayer(Some(zoomLevel), tileLayer)
  }

  def apply(
    zoomLevel: Integer,
    rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  ): TemporalTiledRasterLayer =
    zoomLevel match {
      case i: Integer => apply(Some(i.toInt), rdd)
      case null => apply(None, rdd)
    }

  def apply(
    zoomLevel: Option[Int],
    rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  ): TemporalTiledRasterLayer =
    new TemporalTiledRasterLayer(zoomLevel, rdd)

  def rasterize(
    sc: SparkContext,
    geometryBytes: Array[Byte],
    extent: java.util.Map[String, Double],
    crs: String,
    instant: Int,
    cols: Int,
    rows: Int,
    fillValue: Int
  ): TiledRasterLayer[SpaceTimeKey] = {
    val rasterExtent = RasterExtent(extent.toExtent, cols, rows)
    val temporalExtent =
      TemporalProjectedExtent(rasterExtent.extent, TileLayer.getCRS(crs).get, instant.toInt)

    val tile = Rasterizer.rasterizeWithValue(WKB.read(geometryBytes), rasterExtent, fillValue)
    val rdd = sc.parallelize(Array((temporalExtent, MultibandTile(tile))))
    val tileLayout = TileLayout(1, 1, cols, rows)
    val layoutDefinition = LayoutDefinition(rasterExtent.extent, tileLayout)

    val metadata = rdd.collectMetadata[SpaceTimeKey](layoutDefinition)

    TemporalTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(metadata), metadata))
  }

  def unionLayers(sc: SparkContext, layers: ArrayList[TemporalTiledRasterLayer]): TemporalTiledRasterLayer = {
    val scalaLayers = layers.asScala

    val result = sc.union(scalaLayers.map(_.rdd))

    val firstLayer = scalaLayers.head
    val zoomLevel = firstLayer.zoomLevel

    var unionedMetadata = firstLayer.rdd.metadata

    for (x <- 1 until scalaLayers.size) {
      val otherMetadata = scalaLayers(x).rdd.metadata
      unionedMetadata = unionedMetadata.combine(otherMetadata)
    }

    TemporalTiledRasterLayer(zoomLevel, ContextRDD(result, unionedMetadata))
  }

  def combineBands(sc: SparkContext, layers: ArrayList[TemporalTiledRasterLayer]): TemporalTiledRasterLayer = {
    val baseLayer: TemporalTiledRasterLayer = layers.get(0)
    val result: RDD[(SpaceTimeKey, MultibandTile)] =
      TileLayer.combineBands[SpaceTimeKey, TemporalTiledRasterLayer](sc, layers)

    TemporalTiledRasterLayer(baseLayer.zoomLevel, ContextRDD(result, baseLayer.rdd.metadata))
  }
}
