package org.openeo.geotrellis

import io.circe.syntax.EncoderOps
import io.circe.Json
import geotrellis.layer.SpatialKey._
import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata, _}
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.buffer.{BufferSizes, BufferedTile}
import geotrellis.raster.crop.Crop
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, Tags}
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.mapalgebra.local._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.{MultibandTileLayerRDD, _}
import geotrellis.util._
import geotrellis.vector.Extent.toPolygon
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._
import org.apache.spark.{Partitioner, SparkContext}
import org.openeo.geotrellis.OpenEOProcessScriptBuilder.{MaxIgnoreNoData, MinIgnoreNoData, OpenEOProcess, safeConvert}
import org.openeo.geotrellis.focal._
import org.openeo.geotrellis.netcdf.NetCDFRDDWriter.ContextSeq
import org.openeo.geotrelliscommon.{ByTileSpacetimePartitioner, ByTileSpatialPartitioner, ConfigurableSpaceTimePartitioner, DatacubeSupport, FFTConvolve, OpenEORasterCube, OpenEORasterCubeMetadata, SCLConvolutionFilter, SpaceTimeByMonthPartitioner, SparseSpaceOnlyPartitioner, SparseSpaceTimePartitioner, SparseSpatialPartitioner}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, immutable, mutable}
import scala.reflect._


object OpenEOProcesses{

  private val logger = LoggerFactory.getLogger(classOf[OpenEOProcesses])

  private def timeseriesForBand(b: Int, values: Iterable[(SpaceTimeKey, MultibandTile)],cellType: CellType) = {
    MultibandTile(values.toList.sortBy(_._1.instant).map(_._2.band(b)).map( t => {
      if(t.cellType != cellType){
        t.convert(cellType)
      }else{
        t
      }
    }))

  }

  private implicit def sc: SparkContext = SparkContext.getOrCreate()

  private def firstTile(tiles: Iterable[MultibandTile]) = {
    tiles.filterNot(_.isInstanceOf[EmptyMultibandTile]).headOption.getOrElse(tiles.head)
  }
  private def createTemporalCallback(function: OpenEOProcess,context:Map[String,Any], expectedCellType: CellType) = {
    val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpaceTimeKey, MultibandTile] = values => {

      val aTile = firstTile(values.map(_._2))
      val labels = values.map(_._1).toList.sortBy(_.instant)
      val theContext = context + ("array_labels"->labels.map(_.time.format(DateTimeFormatter.ISO_INSTANT)))

      val functionWithContext = function.apply(theContext)
      var range = 0 until aTile.bandCount

      val callback: Int => Iterable[(SpaceTimeKey, (Int, Tile))] = b => {
        val temporalTile = timeseriesForBand(b, values, expectedCellType)
        val resultTiles = functionWithContext(temporalTile.bands)
        var resultLabels: Iterable[(SpaceTimeKey, Tile)] = labels.zip(resultTiles)
        resultLabels.map(t => (t._1, (b, t._2)))
      }

      val resultMap =
      if (aTile.bandCount>1 ) {
        range.par.flatMap(callback).seq
      }else{
        range.flatMap(callback)
      }

      resultMap.groupBy(_._1).map(t=>{
        (t._1,MultibandTile(t._2.map(_._2).toList.sortBy(_._1).map(_._2)))
      })

    }
    applyToTimeseries
  }
}


class OpenEOProcesses extends Serializable {

  import OpenEOProcesses._


  val tileBinaryOp: Map[String, LocalTileBinaryOp] = Map(
    "or" -> Or,
    "and" -> And,
    "divide" -> Divide,
    "max" -> MaxIgnoreNoData,
    "min" -> MinIgnoreNoData,
    "multiply" -> Multiply,
    "product" -> Multiply,
    "add" -> Add,
    "sum" -> Add,
    "subtract" -> Subtract,
    "xor" -> Xor
  )

  def wrapCube[K](datacube:MultibandTileLayerRDD[K]): OpenEORasterCube[K] = {
    return new OpenEORasterCube[K](datacube,datacube.metadata,new OpenEORasterCubeMetadata(Seq.empty))
  }


  def reduceTimeDimension(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any]):MultibandTileLayerRDD[SpatialKey] = {
    val rdd = transformTimeDimension[SpatialKey](datacube, scriptBuilder, context,reduce = true)
    ContextRDD(rdd,new TileLayerMetadata[SpatialKey](cellType=scriptBuilder.getOutputCellType(),layout=datacube.metadata.layout, extent=datacube.metadata.extent,crs=datacube.metadata.crs,bounds=datacube.metadata.bounds.get.toSpatial ))
  }

  /**
   * apply_dimension, over time dimension
   * @param datacube
   * @param scriptBuilder
   * @param context
   * @return
   */
  def applyTimeDimension(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any]):MultibandTileLayerRDD[SpaceTimeKey] = {
    datacube.context.setCallSite(s"apply_dimension target='t' ")
    try{
      val rdd = transformTimeDimension[SpaceTimeKey](datacube, scriptBuilder, context)
      if(datacube.partitioner.isDefined) {
        ContextRDD(rdd.partitionBy(datacube.partitioner.get),datacube.metadata.copy(cellType = scriptBuilder.getOutputCellType()))
      }else{
        ContextRDD(rdd,datacube.metadata.copy(cellType = scriptBuilder.getOutputCellType()))
      }
    }finally{
      datacube.context.clearCallSite()
    }

  }

    private def transformTimeDimension[KT](datacube: MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder: OpenEOProcessScriptBuilder, context: util.Map[String, Any], reduce:Boolean=false): RDD[(KT, MultibandTile)] = {


      val expectedCellType = datacube.metadata.cellType
      val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[KT, MultibandTile] =
        if(reduce){
          val callback = createTemporalCallback(scriptBuilder.inputFunction.asInstanceOf[OpenEOProcess], context.asScala.toMap, expectedCellType)
          val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpatialKey, MultibandTile] = values => {
            callback(values).map(t=>(t._1.spatialKey,t._2))
          }
          applyToTimeseries.asInstanceOf[Iterable[(SpaceTimeKey, MultibandTile)] => Map[KT,MultibandTile]]
        }else{
          createTemporalCallback(scriptBuilder.inputFunction.asInstanceOf[OpenEOProcess], context.asScala.toMap, expectedCellType).asInstanceOf[Iterable[(SpaceTimeKey, MultibandTile)] => Map[KT,MultibandTile]]
        }

      transformTimeDimension(datacube,applyToTimeseries,reduce)
    }

  private def transformTimeDimension[KT](datacube: MultibandTileLayerRDD[SpaceTimeKey],applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[KT, MultibandTile],  reduce:Boolean ): RDD[(KT, MultibandTile)] = {


    val index: Option[PartitionerIndex[SpaceTimeKey]] =
      if (datacube.partitioner.isDefined && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
        Some(datacube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index)
      } else {
        None
      }
    logger.info(s"Applying callback on time dimension of cube with partitioner: ${datacube.partitioner.getOrElse("no partitioner")} - index: ${index.getOrElse("no index")} and metadata ${datacube.metadata}")
    val rdd: RDD[(SpaceTimeKey, MultibandTile)] =
      if (index.isDefined && (index.get.isInstanceOf[SparseSpaceOnlyPartitioner] || index.get == ByTileSpacetimePartitioner )) {
        datacube
      } else {
        val keys: Option[Array[SpaceTimeKey]] = findPartitionerKeys(datacube)
        val spatiallyGroupingIndex =
          if(keys.isDefined){
            new SparseSpaceOnlyPartitioner(keys.get.map(SparseSpaceOnlyPartitioner.toIndex(_, indexReduction = 0)).distinct.sorted, 0, keys)
          }else{
            ByTileSpacetimePartitioner
          }
        logger.info(f"Regrouping data cube along the time dimension, with index $spatiallyGroupingIndex. Cube metadata: ${datacube.metadata}")
        val partitioner: Partitioner = new SpacePartitioner(datacube.metadata.bounds)(implicitly, implicitly, spatiallyGroupingIndex)
        //regular partitionBy doesn't work because Partitioners appear to be equal while they're not
        new ShuffledRDD[SpaceTimeKey,MultibandTile,MultibandTile](datacube, partitioner)
      }
    rdd.mapPartitions(p => {
      val bySpatialKey: Map[SpatialKey, Seq[(SpaceTimeKey, MultibandTile)]] = p.toSeq.groupBy(_._1.spatialKey)
      bySpatialKey.mapValues(applyToTimeseries).flatMap(_._2).iterator
    }, preservesPartitioning = reduce)
  }



  /**
   * apply_dimension, over time dimension
   *
   * @param datacube
   * @param scriptBuilder
   * @param context
   * @return
   */
  def applyTimeDimensionTargetBands(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any]):MultibandTileLayerRDD[SpatialKey] = {
    val expectedCelltype = datacube.metadata.cellType

    val function = scriptBuilder.inputFunction.asInstanceOf[OpenEOProcess]
    val currentTileSize = datacube.metadata.tileLayout.tileSize
    var tileSize = context.getOrDefault("TileSize",0).asInstanceOf[Int]
    if(currentTileSize>=512*512 && tileSize==0) {
      tileSize = 128//right value here depends on how many bands we're going to create, but can be a high number
    }

    val index = if (datacube.partitioner.isDefined && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
      datacube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
    }
    SparkContext.getOrCreate().setCallSite(s"apply_dimension target='bands' TileSize: $tileSize Input index: $index ")

    val retiled =
      if (tileSize > 0 && tileSize <= 1024) {
        val theResult = retileGeneric(datacube, tileSize, tileSize, 0, 0)
        theResult
      } else {
        datacube
      }

    val outputCelltype = scriptBuilder.getOutputCellType()
    val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpatialKey, MultibandTile] = tiles => {
      val aTile = firstTile(tiles.map(_._2))
      val theKey = tiles.head._1.spatialKey

      val labels = tiles.map(_._1).toList.sortBy(_.instant)
      val theContext = context.asScala.toMap + ("array_labels" -> labels.map(_.time.format(DateTimeFormatter.ISO_INSTANT)))
      val tileFunction: Seq[Tile] => Seq[Tile] = function(theContext)

      val range = 0 until aTile.bandCount
      val callback: Int => (Int, Seq[Tile]) = b => {
        val temporalTile = timeseriesForBand(b, tiles, expectedCelltype)
        (b, tileFunction(temporalTile.bands))
      }
      val result = if(aTile.bandCount>1 ) {
        range.par.map(callback).seq.sortBy(_._1)
      }else {
        range.map(callback)
      }

      val resultTile = result.flatMap(_._2)
      if(resultTile.nonEmpty) {
        Map( theKey -> MultibandTile(resultTile) )
      }else{
        // Note: Is this code ever reached? aTile.bandCount is always > 0.
        Map( theKey -> new EmptyMultibandTile(aTile.cols,aTile.rows,outputCelltype) )
      }

    }

    val resultRDD = transformTimeDimension(retiled,applyToTimeseries,reduce = false)

    val newBounds = retiled.metadata.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]].toSpatial

    val keys = findPartitionerKeys(datacube)
    val spatiallyGroupingIndex =
      if(keys.isDefined){
        val spatialKeys: Array[SpatialKey] = keys.get.map(_.spatialKey).distinct
        new SparseSpatialPartitioner(spatialKeys.map(ByTileSpatialPartitioner.toIndex).distinct.sorted, 0, Some(spatialKeys))
      }else{
        ByTileSpatialPartitioner
      }
    val partitioner: Partitioner = new SpacePartitioner(newBounds)(implicitly, implicitly, spatiallyGroupingIndex)

    SparkContext.getOrCreate().clearCallSite()
    ContextRDD(resultRDD.partitionBy(partitioner),retiled.metadata.copy(bounds = newBounds,cellType = outputCelltype))
  }

  private def groupOnTimeDimension(datacube: MultibandTileLayerRDD[SpaceTimeKey]) = {
    val targetBounds = datacube.metadata.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]].toSpatial

    val keys: Option[Array[SpaceTimeKey]] = findPartitionerKeys(datacube)

    val index =
      if (keys.isDefined) {
        new SparseSpatialPartitioner(keys.get.map(SparseSpaceOnlyPartitioner.toIndex(_, indexReduction = 0)).distinct.sorted, 0, keys.map(_.map(_.spatialKey)))
      } else {
        ByTileSpatialPartitioner
      }

    logger.info(f"Regrouping data cube along the time dimension, with index $index. Cube metadata: ${datacube.metadata}")
    val partitioner: Partitioner = new SpacePartitioner(targetBounds)(implicitly, implicitly, index)

    val groupedOnTime = datacube.groupBy[SpatialKey]((t: (SpaceTimeKey, MultibandTile)) => t._1.spatialKey, partitioner)
    groupedOnTime
  }


  def findPartitionerKeys(datacube: MultibandTileLayerRDD[SpaceTimeKey]): Option[Array[SpaceTimeKey]] = {
    val keys: Option[Array[SpaceTimeKey]] = if (datacube.partitioner.isDefined && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
      val index = datacube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
      if (index.isInstanceOf[SparseSpaceTimePartitioner]) {
        index.asInstanceOf[SparseSpaceTimePartitioner].theKeys
      } else if (index.isInstanceOf[SparseSpaceOnlyPartitioner]) {
        index.asInstanceOf[SparseSpaceOnlyPartitioner].theKeys
      } else {
        Option.empty
      }
    } else {
      Option.empty
    }
    keys
  }

  /**
   * @param datacube The datacube to be masked. The celltype is assumed to be Float with NoDataHandling.
   * @param projectedPolygons A list of polygons to mask the original datacube with.
   * @param maskValue The value used for any cell outside of the polygon.
   *                  This allows for a distinction between NoData cells within the polygon,
   *                  and areas outside of it.
   * @return A new RDD with a list of masked tiles for every polygon in projectedPolygons.
   */
  def groupAndMaskByGeometry(datacube: MultibandTileLayerRDD[SpaceTimeKey],
                             projectedPolygons: ProjectedPolygons,
                             maskValue: java.lang.Double
                            ): RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])] = {
    val useNoDataMask = maskValue == null
    val polygonsBC: Broadcast[List[MultiPolygon]] = SparkContext.getOrCreate().broadcast(projectedPolygons.polygons.toList)
    val layout = datacube.metadata.layout

    val groupedAndMaskedByPolygon: RDD[(MultiPolygon, Iterable[(SpaceTimeKey, MultibandTile)])] = datacube.flatMap {
      case (key, tile: MultibandTile) =>
        val tileExtentAsPolygon: Polygon = toPolygon(layout.mapTransform(key.getComponent[SpatialKey]))
        // Filter the polygons that lie in this tile.
        val polygonsInTile: Seq[MultiPolygon] = polygonsBC.value.filter(polygon => tileExtentAsPolygon.intersects(polygon))
        // Mask this tile for every polygon.
        polygonsInTile.map { polygon =>
            val tileExtent: Extent = layout.mapTransform(key)
            val maskedTile = tile.mask(tileExtent, polygon)
            ((polygon), (key, maskedTile))
        }
    }.groupByKey()

    // For every polygon, stitch all tiles with the same date together into one tile,
    // then crop this tile using the polygon.
    val tilePerDateGroupedByPolygon: RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])] = groupedAndMaskedByPolygon.map {
      case (polygon, values) =>
        val valuesGroupedByDate: Map[ZonedDateTime, Iterable[(SpaceTimeKey, MultibandTile)]] = values.groupBy(_._1.time)
        // For every date, make one tile by stitching tiles together.
        val tilePerDate: Iterable[(Extent, Long, MultibandTile)] = valuesGroupedByDate.map {
          case (key, maskedTiles) =>
            // 1. Stitch.
            val tiles: Iterable[(SpatialKey, MultibandTile)] = maskedTiles.map(tile => (tile._1.spatialKey, tile._2))
            val raster: Raster[MultibandTile] = ContextSeq(tiles, layout).stitch()
            // 2. Crop.
            // Take the smallest grid-aligned extent that covers the polygon.
            // Clamp = false: ensures that tiles within the extent that do not exist in the raster show up as NoData.
            val alignedExtent = raster.rasterExtent.createAlignedGridExtent(polygon.extent).extent
            val cropOptions = Crop.Options(clamp = false, force = true)
            val croppedRaster: Raster[MultibandTile] = raster.crop(alignedExtent, cropOptions)
            // 3. Finally, mask out the polygon.
            val maskedTile = croppedRaster.tile.mapBands { (_index, t) =>
              if (!t.cellType.isFloatingPoint)
                throw new IllegalArgumentException("groupAndMaskByGeometry only supports floating point cell types.")
              val re = croppedRaster.rasterExtent
              if (useNoDataMask) {
                t.mask(re.extent, polygon)
              }
              else {
                val cellType = t.cellType.asInstanceOf[FloatCells with NoDataHandling]
                val result = FloatArrayTile.fill(maskValue.floatValue(), t.cols, t.rows, cellType)
                val options = Rasterizer.Options.DEFAULT
                polygon.foreach(re, options)({ (col: Int, row: Int) =>
                  result.setDouble(col, row, t.getDouble(col, row))
                })
                result
              }
            }
            (croppedRaster.extent, maskedTiles.head._1.instant, maskedTile)
        }.toList
        (polygon, tilePerDate)
    }

    tilePerDateGroupedByPolygon
  }

  def mergeGroupedByGeometry(tiles: RDD[(MultiPolygon, Iterable[(Extent, Long, MultibandTile)])], metadata: TileLayerMetadata[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    val keyedByTemporalExtent: RDD[(TemporalProjectedExtent, MultibandTile)] = tiles.flatMap {
      case (_polygon, polygonTiles: Iterable[(Extent, Long, MultibandTile)]) =>
        polygonTiles.map(tile => {
          val projectedExtent: ProjectedExtent = ProjectedExtent(tile._1, metadata.crs)
          val multibandTile: MultibandTile = tile._3
          (TemporalProjectedExtent(projectedExtent, tile._2), multibandTile)
        })
    }
    val keyedBySpatialKey: RDD[(SpaceTimeKey, MultibandTile)] = keyedByTemporalExtent.tileToLayout(metadata)
    ContextRDD(keyedBySpatialKey, metadata)
  }



  def mapInstantToInterval(datacube:MultibandTileLayerRDD[SpaceTimeKey], intervals:java.lang.Iterable[String], labels:java.lang.Iterable[String]) :MultibandTileLayerRDD[SpaceTimeKey] = {
    val timePeriods: Seq[Iterable[Instant]] = JavaConverters.iterableAsScalaIterableConverter(intervals).asScala.map(s => Instant.parse(s)).grouped(2).toList
    val periodsToLabels: Seq[(Iterable[Instant], String)] = timePeriods.zip(labels.asScala)
    val tilesByInterval: RDD[(SpaceTimeKey, MultibandTile)] = datacube.flatMap(tuple => {
      val instant = tuple._1.time.toInstant
      val spatialKey = tuple._1.spatialKey
      val labelsForKey = periodsToLabels.filter(p => {
        val interval = p._1
        val iterator = interval.toIterator
        val leftBound = iterator.next()
        val rightBound = iterator.next()
        (leftBound.isBefore(instant) && rightBound.isAfter(instant)) || leftBound.equals(instant)
      }).map(t => t._2).map(ZonedDateTime.parse(_))

      labelsForKey.map(l => (SpaceTimeKey(spatialKey,TemporalKey(l)),tuple._2))
    })
    return ContextRDD(tilesByInterval, datacube.metadata)

  }

  def aggregateTemporal(datacube:MultibandTileLayerRDD[SpaceTimeKey], intervals:java.lang.Iterable[String],labels:java.lang.Iterable[String], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any]) :MultibandTileLayerRDD[SpaceTimeKey] = {
    return aggregateTemporal(datacube, intervals, labels, scriptBuilder, context,true)
  }
  def aggregateTemporal(datacube:MultibandTileLayerRDD[SpaceTimeKey], intervals:java.lang.Iterable[String],labels:java.lang.Iterable[String], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any], reduce:Boolean ) :MultibandTileLayerRDD[SpaceTimeKey] = {
    if(reduce) {
      datacube.sparkContext.setCallSite(s"aggregate_temporal $intervals")
    }else{
      datacube.sparkContext.setCallSite(s"apply_neighborhood over time intervals")
    }
    val timePeriods: Seq[Iterable[Instant]] = JavaConverters.iterableAsScalaIterableConverter(intervals).asScala.map(s => Instant.parse(s)).grouped(2).toList
    val labelsDates = labels.asScala.map(ZonedDateTime.parse(_))
    val periodsToLabels: Seq[(Iterable[Instant], String)] = timePeriods.zip(labels.asScala)

    val filteredCube = filterNegativeSpatialKeys(datacube)

    val keys = findPartitionerKeys(filteredCube).map(_.filter( k => k.row >= 0 && k.col >= 0 ))
    val allPossibleKeys: immutable.Seq[SpatialKey] = if(keys.isDefined) {
      keys.get.map(_.spatialKey).distinct.toList
    } else{
      filteredCube.metadata.tileBounds
        .coordsIter
        .map { case (x, y) => SpatialKey(x, y) }
        .toList
    }

    val allPossibleSpacetime =  allPossibleKeys.flatMap(x => labelsDates.map(y => (SpaceTimeKey(x, TemporalKey(y)),null)))
    val theNewKeys = allPossibleSpacetime.map(_._1).toArray

    val index: PartitionerIndex[SpaceTimeKey] =
      if(keys.isDefined) {
        new SparseSpaceTimePartitioner(theNewKeys.map(SparseSpaceTimePartitioner.toIndex(_, indexReduction = 4)).distinct.sorted, 4,Some(theNewKeys))
      }else{
        if (datacube.partitioner.isDefined && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
          val index = datacube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
          if (index.isInstanceOf[SparseSpaceOnlyPartitioner]) {
            index//a space only partitioner does not care about time, so can be reused as-is
          } else {
            SpaceTimeByMonthPartitioner
          }
        }else{
          SpaceTimeByMonthPartitioner
        }
      }

    val allKeys = allPossibleSpacetime.map(_._1)
    val minKey = allKeys.reduce((a,b)=>SpaceTimeKey.Boundable.minBound(a,b))
    val maxKey = allKeys.reduce((a,b)=>SpaceTimeKey.Boundable.maxBound(a,b))
    val newBounds = new KeyBounds(minKey,maxKey)
    logger.info(s"aggregate_temporal results in ${allPossibleSpacetime.size} keys, using partitioner index: ${index} with bounds ${newBounds}" )
    val partitioner: SpacePartitioner[SpaceTimeKey] = SpacePartitioner[SpaceTimeKey](newBounds)(implicitly,implicitly, index)


    val sc = SparkContext.getOrCreate()

    val allKeysRDD: RDD[(SpaceTimeKey, Null)] = sc.parallelize(allPossibleSpacetime)


    def mapToNewKey(tuple: (SpaceTimeKey, MultibandTile)): Seq[(SpaceTimeKey, (SpaceTimeKey,MultibandTile))] = {
      val instant = tuple._1.time.toInstant
      val spatialKey = tuple._1.spatialKey
      val labelsForKey = periodsToLabels.filter(p => {
        val interval = p._1
        val iterator = interval.toIterator
        val leftBound = iterator.next()
        val rightBound = iterator.next()
        (leftBound.isBefore(instant) && rightBound.isAfter(instant)) || leftBound.equals(instant)
      }).map(t => t._2).map(ZonedDateTime.parse(_))

      labelsForKey.map(l => (SpaceTimeKey(spatialKey, TemporalKey(l)), tuple))
    }


    val function = scriptBuilder.inputFunction.asInstanceOf[OpenEOProcess]
    val outputCellType = scriptBuilder.getOutputCellType()
    def aggregateTiles(tiles: Iterable[(SpaceTimeKey,MultibandTile)]) = {
      val theContext = context.asScala.toMap + ("array_labels"->tiles.map(_._1.time.format(DateTimeFormatter.ISO_INSTANT)))
      val tilesFunction = function.apply(theContext)
      val aTile = firstTile(tiles.map(_._2))
      val resultTiles: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer[Tile]()
      for (b <- 0 until aTile.bandCount) {
        val temporalTile = MultibandTile(tiles.map(_._2.band(b)))
        val aggregatedTiles: Seq[Tile] = tilesFunction(temporalTile.bands)
        resultTiles += aggregatedTiles.head

      }
      MultibandTile(resultTiles)

    }

    val tilesByInterval: RDD[(SpaceTimeKey, MultibandTile)] =
      if(reduce) {
        if(datacube.partitioner.isDefined && datacube.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]] && datacube.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index.isInstanceOf[SparseSpaceOnlyPartitioner]) {
          filteredCube.mapPartitions(elements =>{
            val byNewKey= elements.flatMap(mapToNewKey).toStream.groupBy(_._1)
            byNewKey.mapValues(v=>aggregateTiles(v.map(_._2))).iterator
          },preservesPartitioning = true)
        }else{
          filteredCube.flatMap(tuple => {
            mapToNewKey(tuple)
          }).groupByKey(partitioner).mapValues( aggregateTiles)
        }

      }else{

        filteredCube.flatMap(tuple => {
          mapToNewKey(tuple)
        }).groupByKey(partitioner).flatMap( t => {
          val applyToTimeseries: Iterable[(SpaceTimeKey, MultibandTile)] => Map[SpaceTimeKey, MultibandTile] = createTemporalCallback(function,context.asScala.toMap, datacube.metadata.cellType)
          applyToTimeseries(t._2)
        })

      }

    val cols = filteredCube.metadata.tileLayout.tileCols
    val rows = filteredCube.metadata.tileLayout.tileRows
    val cellType = datacube.metadata.cellType

    val bandCount = RDDBandCount(datacube)
    val filledRDD: RDD[(SpaceTimeKey, MultibandTile)] = {
      if(reduce) {
        tilesByInterval.rightOuterJoin(allKeysRDD,partitioner).mapValues(_._1.getOrElse(new EmptyMultibandTile(cols, rows, cellType, bandCount)))
      }else{
        tilesByInterval
      }
    }
    val metadata = if(reduce) datacube.metadata.copy(bounds = newBounds,cellType = outputCellType) else datacube.metadata.copy(cellType = outputCellType)
    sc.clearCallSite()
    val aggregatedCube = ContextRDD(filledRDD, metadata)
    aggregatedCube.name = "aggregate_temporal result"
    return aggregatedCube
  }

  def mapBands(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder, context: java.util.Map[String,Any] = new util.HashMap[String, Any]()): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]= {
    mapBandsGeneric(datacube,scriptBuilder,context)
  }

  def mapBandsGeneric[K:ClassTag](datacube:MultibandTileLayerRDD[K], scriptBuilder:OpenEOProcessScriptBuilder, context: java.util.Map[String,Any]): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]={
    val function = if (context.isEmpty) {
      scriptBuilder.generateFunction()
    } else {
      scriptBuilder.generateFunction(context.asScala.toMap)
    }
    return ContextRDD(new org.apache.spark.rdd.PairRDDFunctions[K,MultibandTile](datacube).mapValues(tile => {

      val resultTiles = function(tile.bands)
      if(resultTiles.exists(!_.isNoDataTile)){
        MultibandTile(resultTiles)
      }else{
        new EmptyMultibandTile(tile.cols,tile.rows,resultTiles.head.cellType,resultTiles.length)
      }

    }),datacube.metadata.copy(cellType = scriptBuilder.getOutputCellType()))
  }

  def filterEmptyTile[K:ClassTag](datacube:MultibandTileLayerRDD[K]): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]={
    return datacube.withContext(_.filter(!_._2.isInstanceOf[EmptyMultibandTile]))
  }

  /**
   * Simple vectorize implementation
   * Rasters are combined to a max size of 5120, larger rasters are supported, but resulting features will not be merged
   * Only first band is taken into account, other bands are ignored.
   *
   * @param datacube
   * @return
   */
  def vectorize[K: SpatialComponent: ClassTag](datacube: MultibandTileLayerRDD[K]): (Array[(String, List[PolygonFeature[Int]])], CRS) = {
    val layout = datacube.metadata.layout
    val maxExtent = datacube.metadata.extent
    //naive approach: combine tiles and hope that we don't exceed the max size
    //if we exceed the max, vectorize will run on separate tiles, and we'll need to merge results
    val newCols = Math.min(256*20,layout.cols)
    val newRows = Math.min(256*20,layout.rows)

    val singleBandLayer: TileLayerRDD[K] = datacube.withContext(_.mapValues(_.band(0)))
    val retiled = singleBandLayer.regrid(newCols.intValue(),newRows.intValue())
    // Perform the actual vectorization.
    val vectorizedValues: RDD[(K, List[PolygonFeature[Int]])] = retiled.toRasters.mapValues(_.crop(maxExtent,Crop.Options(force=true,clamp=true)).toVector())
    // We don't require spatial partitioning for features, so we can group by (Time, Band) instead.
    // In the meantime we construct the feature ids as they will appear in the geojson file.
    val featuresWithId: RDD[(String, List[PolygonFeature[Int]])] = vectorizedValues.map(kv => {
      val bandStr = "band0"
      kv._1 match {
        case stk: SpaceTimeKey => (stk.time.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + "_" + bandStr, kv._2)
        case _ => (bandStr, kv._2)
      }
    })
    val featuresWithIdGrouped: RDD[(String, Iterable[List[PolygonFeature[Int]]])] = featuresWithId.groupByKey()
    val featuresWithIdGroupedFlat: RDD[(String, List[PolygonFeature[Int]])] = featuresWithIdGrouped.mapValues(_.flatten.toList)
    return (featuresWithIdGroupedFlat.collect(), datacube.metadata.crs)
  }

  def featuresToGeojson(features: Array[(String, List[PolygonFeature[Int]])], crs: CRS): Json = {
    // Add index to each feature id, so final id will be 'date_band_index'.
    val geojsonFeaturesWithId: Array[Json] = features.flatMap((v) => {
      val key: String = v._1 // (Time, Band) key.
      // Geojson lists properties as a map.
      val feats: List[PolygonFeature[Map[String,Int]]] = v._2.map(_.mapData(v => immutable.Map("value" -> v)))
      feats.zipWithIndex.map({case (f,i) => f.asJson.deepMerge(Json.obj("id" -> (key + "_" + i).asJson))})
    })
    // Add bbox to top level.
    val bboxOption = features.flatMap(_._2).map(_.geom.extent).reduceOption(_ combine _)
    val jsonWithBbox = bboxOption match {
      case Some(bbox) =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "bbox" -> Extent.listEncoder(bbox),
          "features" -> geojsonFeaturesWithId.asJson
        )
      case _ =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "features" -> geojsonFeaturesWithId.asJson
        )
    }
    // Add epsg to top level.
    val epsg = "epsg:"+ crs.epsgCode.get
    val crs_json = _root_.io.circe.parser.parse("""{"crs":{"type":"name","properties":{"name":"THE_CRS"}}}""".replace("THE_CRS",epsg))
    val jsonWithCRS: Json = jsonWithBbox.deepMerge(crs_json.right.get)
    jsonWithCRS
  }

  def vectorize(datacube:Object, outputFile:String): Unit = {
    val (features: Array[(String, List[PolygonFeature[Int]])], crs: CRS) = datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        vectorize(rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]])
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey]  =>
        vectorize(rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]])
      case _ => throw new IllegalArgumentException("Unsupported rdd type to vectorize: ${rdd}")
    }
    val json = featuresToGeojson(features,crs)
    Files.write(Paths.get(outputFile), json.toString().getBytes(StandardCharsets.UTF_8))
  }

  def applySpacePartitioner(datacube: RDD[(SpaceTimeKey, MultibandTile)], keyBounds: KeyBounds[SpaceTimeKey]): RDD[(SpaceTimeKey, MultibandTile)] = {
    datacube.partitionBy( SpacePartitioner(keyBounds))
  }

  def applySparseSpacetimePartitioner(datacube: RDD[(SpaceTimeKey, MultibandTile)],  keys: util.List[SpaceTimeKey],indexReduction:Int): RDD[(SpaceTimeKey, MultibandTile)] = {
    val scalaKeys = keys.asScala
    implicit val newIndex: PartitionerIndex[SpaceTimeKey] = new SparseSpaceTimePartitioner(scalaKeys.map(SparseSpaceTimePartitioner.toIndex(_,indexReduction)).distinct.sorted.toArray,indexReduction,theKeys = Some(scalaKeys.toArray) )
    val bounds = KeyBounds(scalaKeys.min,scalaKeys.max)
    val partitioner = SpacePartitioner[SpaceTimeKey](bounds)(implicitly,implicitly,newIndex)
    datacube.partitionBy( partitioner)
  }

  private def outerJoin[K: Boundable: PartitionerIndex: ClassTag,
    M: GetComponent[*, Bounds[K]],
    M1: GetComponent[*, Bounds[K]]
  ](leftCube: RDD[(K, MultibandTile)] with Metadata[M], rightCube: RDD[(K, MultibandTile)] with Metadata[M1]): RDD[(K, (Option[MultibandTile], Option[MultibandTile]))] with Metadata[Bounds[K]] = {

    val kbLeft: Bounds[K] = leftCube.metadata.getComponent[Bounds[K]]
    val kbRight: Bounds[K] = rightCube.metadata.getComponent[Bounds[K]]
    val kb: Bounds[K] = kbLeft.combine(kbRight)

    val leftCount = maybeBandCount(leftCube)
    val rightCount = maybeBandCount(rightCube)
    //fairly arbitrary heuristic if we're going to create a cube with a high number of bands
    val manyBands = leftCount.getOrElse(1) + rightCount.getOrElse(1) > 25

    val part =if( leftCube.partitioner.isDefined && rightCube.partitioner.isDefined && leftCube.partitioner.get.isInstanceOf[SpacePartitioner[K]] && rightCube.partitioner.get.isInstanceOf[SpacePartitioner[K]]) {
      val leftPart = leftCube.partitioner.get.asInstanceOf[SpacePartitioner[K]]
      val rightPart = rightCube.partitioner.get.asInstanceOf[SpacePartitioner[K]]
      logger.info(s"Merging cubes with spatial indices: ${leftPart.index} - ${rightPart.index}")
      if(leftPart.index == rightPart.index && leftPart.index.isInstanceOf[SparseSpaceTimePartitioner]) {
        val newIndices: Array[BigInt] = (leftPart.index.asInstanceOf[SparseSpaceTimePartitioner].indices ++ rightPart.index.asInstanceOf[SparseSpaceTimePartitioner].indices).distinct.sorted
        implicit val newIndex: PartitionerIndex[K] = new SparseSpaceTimePartitioner(newIndices,leftPart.index.asInstanceOf[SparseSpaceTimePartitioner].indexReduction).asInstanceOf[PartitionerIndex[K]]
        SpacePartitioner[K](kb)(implicitly,implicitly,newIndex)
      }else if(leftPart.index == rightPart.index && leftPart.index.isInstanceOf[SparseSpaceOnlyPartitioner]) {
        val newIndices: Array[BigInt] = (leftPart.index.asInstanceOf[SparseSpaceOnlyPartitioner].indices ++ rightPart.index.asInstanceOf[SparseSpaceOnlyPartitioner].indices).distinct.sorted
        implicit val newIndex: PartitionerIndex[K] = new SparseSpaceOnlyPartitioner(newIndices,leftPart.index.asInstanceOf[SparseSpaceOnlyPartitioner].indexReduction).asInstanceOf[PartitionerIndex[K]]
        SpacePartitioner[K](kb)(implicitly,implicitly,newIndex)
      }
      else if(leftPart.index == rightPart.index && leftPart.index == ByTileSpatialPartitioner ) {
        leftPart
      }
      else if(leftPart.index == rightPart.index  && leftPart.index.isInstanceOf[SparseSpatialPartitioner] ) {
        val newIndices: Array[BigInt] = (leftPart.index.asInstanceOf[SparseSpatialPartitioner].indices ++ rightPart.index.asInstanceOf[SparseSpatialPartitioner].indices).distinct.sorted
        implicit val newIndex: PartitionerIndex[K] = new SparseSpatialPartitioner(newIndices,leftPart.index.asInstanceOf[SparseSpatialPartitioner].indexReduction).asInstanceOf[PartitionerIndex[K]]
        SpacePartitioner[K](kb)(implicitly,implicitly,newIndex)
      }
      else{
        SpacePartitioner[K](kb)
      }
    } else {
      logger.info(s"Merging cubes with partitioners: ${leftCube.partitioner} - ${rightCube.partitioner} - many band case detected: $manyBands")
      if(manyBands) {
        val index: PartitionerIndex[K] = getManyBandsIndexGeneric[K]()
        SpacePartitioner[K](kb)(implicitly,implicitly,index)
      }else{
        SpacePartitioner[K](kb)
      }

    }

    val joinRdd =
      new CoGroupedRDD[K](List(part(leftCube), part(rightCube)), part)
        .flatMapValues { case Array(l, r) =>
          if (l.isEmpty)
            for (v <- r.iterator) yield (None, Some(v))
          else if (r.isEmpty)
            for (v <- l.iterator) yield (Some(v), None)
          else
            for (v <- l.iterator; w <- r.iterator) yield (Some(v), Some(w))
        }.asInstanceOf[RDD[(K, (Option[MultibandTile], Option[MultibandTile]))]]

    ContextRDD(joinRdd, part.bounds)
  }

  def getManyBandsIndexGeneric[K]()(implicit t:ClassTag[K]):PartitionerIndex[K] = {
    import reflect.ClassTag
    val spacetimeKeyTag = classOf[SpaceTimeKey]
    val index: PartitionerIndex[K] = t match {
      case strtag if strtag == ClassTag(spacetimeKeyTag) => SpaceTimeByMonthPartitioner.asInstanceOf[PartitionerIndex[K]]
      case _ => ByTileSpatialPartitioner.asInstanceOf[PartitionerIndex[K]]
    }
    index
  }


  def maybeBandCount[K](cube: RDD[(K, MultibandTile)]): Option[Int] = {
    if (cube.isInstanceOf[OpenEORasterCube[K]] && cube.asInstanceOf[OpenEORasterCube[K]].openEOMetadata.bandCount > 0) {
      val count = cube.asInstanceOf[OpenEORasterCube[K]].openEOMetadata.bandCount
      logger.info(s"Computed band count ${count} from metadata of ${cube}")
      return Some(count)
    }else{
      return None
    }
  }

  def maybeBandLabels[K](cube: RDD[(K, MultibandTile)]): Option[Seq[String]] = {
    if (cube.isInstanceOf[OpenEORasterCube[K]] && cube.asInstanceOf[OpenEORasterCube[K]].openEOMetadata.bandCount > 0) {
      val labels = cube.asInstanceOf[OpenEORasterCube[K]].openEOMetadata.bands
      return Some(labels)
    }else{
      return None
    }
  }

  /**
   * Get band count used in RDD (each tile in RDD should have same band count)
   */
  def RDDBandCount[K](cube: MultibandTileLayerRDD[K]): Int = {
    // For performance reasons we only check a small subset of tile band counts
    maybeBandCount(cube).getOrElse({
      logger.info(s"Computing number of bands in cube: ${cube.metadata}")
      val counts = cube.take(10).map({ case (k, t) => t.bandCount }).distinct

      if (counts.length == 0) {
        if (cube.isEmpty())
          logger.info("This cube is empty, no band count.")
        else
          logger.info("This cube is not empty, but could not determine band count.")
        1
      }else{
        if (counts.length != 1) {
          throw new IllegalArgumentException("Cube doesn't have single consistent band count across tiles: [%s]".format(counts.mkString(", ")))
        }
        counts(0)
      }
    })
  }

  def filterNegativeSpatialKeys(data: (Int, MultibandTileLayerRDD[SpaceTimeKey])):(Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    (data._1,filterNegativeSpatialKeys(data._2))
  }

  def filterNegativeSpatialKeys_spatial(data: (Int, MultibandTileLayerRDD[SpatialKey])):(Int, MultibandTileLayerRDD[SpatialKey]) = {
    (data._1,filterNegativeSpatialKeys(data._2))
  }

  /**
   * Negative spatial keys are not normal, but can occur when a datacube is resampled into a higher resolution.
   * These negative keys are cropped out in any case when the final result is generated, so we preemptively filter them
   * because they are not supported by Space Partitioner indices.
   *
   * For example: take AGERA5 datacube, resample to Sentinel-2 (10m) -> negative indices occur
   * @param data
   * @tparam K
   * @return
   */
  def filterNegativeSpatialKeys[K: SpatialComponent: ClassTag
  ](data: MultibandTileLayerRDD[K]):MultibandTileLayerRDD[K] = {
    val filtered = data.filter( tuple => {
      val sKey =
        tuple._1 match {
          case key: SpatialKey =>
            key
          case key:SpaceTimeKey =>
            key.spatialKey
          case key: Any =>
            throw new IllegalArgumentException(s"Unsupported key type: $key")
        }
      if(sKey.col<0 || sKey.row<0){
        logger.debug("Preemptively filtering negative spatial key: " + sKey)
        false
      }else{
        true
      }
    })
    logger.info("Keybounds before preemptive filtering: " + data.metadata.bounds)
    val minKey = data.metadata.bounds.get.minKey
    val minSpatial: SpatialKey = minKey.getComponent[SpatialKey]
    val res = minKey.setComponent[SpatialKey](SpatialKey(math.max(0,minSpatial._1),math.max(0,minSpatial._2)))
    val newBounds = KeyBounds(res, data.metadata.bounds.get.maxKey)
    logger.info("Keybounds after preemptive filtering: " + newBounds)
    ContextRDD(filtered,data.metadata.copy(bounds = newBounds))
  }

  def resampleCubeSpatial(data: MultibandTileLayerRDD[SpaceTimeKey], target: MultibandTileLayerRDD[SpaceTimeKey], method:ResampleMethod): (Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    if(target.metadata.crs.equals(data.metadata.crs) && target.metadata.layout.equals(data.metadata.layout)) {
      logger.info(s"resample_cube_spatial: No resampling required for cube: ${data.metadata}")
      (0,data)
    }else{
      //construct a partitioner that is compatible with data cube
      val targetPartitioner =
      if(target.partitioner.isDefined && target.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]) {
        val index = target.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].index
        val theIndex = index match {
          case partitioner: SparseSpaceTimePartitioner =>
            new ConfigurableSpaceTimePartitioner(partitioner.indexReduction)
          case _ =>
            index
        }
        Some(SpacePartitioner[SpaceTimeKey](target.metadata.bounds)(implicitly,implicitly,theIndex))
      }else{
        target.partitioner
      }
      val reprojected = org.openeo.geotrellis.reproject.TileRDDReproject(data, target.metadata.crs, Right(target.metadata.layout), 16, method, targetPartitioner)
      filterNegativeSpatialKeys(reprojected)
    }
  }

  def resampleCubeSpatial_spacetime(data: MultibandTileLayerRDD[SpaceTimeKey],crs:CRS,layout:LayoutDefinition, method:ResampleMethod, partitioner:Partitioner): (Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    if(crs.equals(data.metadata.crs) && layout.equals(data.metadata.layout)) {
      logger.info(s"resample_cube_spatial: No resampling required for cube: ${data.metadata}")
      (0,data)
    }else if(partitioner==null) {
      val reprojected = org.openeo.geotrellis.reproject.TileRDDReproject(data, crs, Right(layout), 16, method, new SpacePartitioner(data.metadata.bounds))
      filterNegativeSpatialKeys(reprojected)
    }else{
      val reprojected = org.openeo.geotrellis.reproject.TileRDDReproject(data, crs, Right(layout), 16, method, partitioner)
      filterNegativeSpatialKeys(reprojected)
    }
  }

  def resampleCubeSpatial_spatial(data: MultibandTileLayerRDD[SpatialKey],crs:CRS,layout:LayoutDefinition, method:ResampleMethod, partitioner:Partitioner): (Int, MultibandTileLayerRDD[SpatialKey]) = {
    if(crs.equals(data.metadata.crs) && layout.equals(data.metadata.layout)) {
      logger.info(s"resample_cube_spatial: No resampling required for cube: ${data.metadata}")
      (0,data)
    }else if(partitioner==null) {
      val reprojected = org.openeo.geotrellis.reproject.TileRDDReproject(data, crs, Right(layout), 16, method, new SpacePartitioner(data.metadata.bounds))
      filterNegativeSpatialKeys_spatial(reprojected)
    }else{
      val reprojected = org.openeo.geotrellis.reproject.TileRDDReproject(data, crs, Right(layout), 16, method, partitioner)
      filterNegativeSpatialKeys_spatial(reprojected)
    }
  }

  def mergeCubes_SpaceTime_Spatial(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpatialKey], operator:String, swapOperands:Boolean): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    leftCube.sparkContext.setCallSite("merge_cubes - (x,y,bands,t) + (x,y,bands)")
    val resampled = resampleCubeSpatial_spatial(rightCube,leftCube.metadata.crs,leftCube.metadata.layout,ResampleMethods.NearestNeighbor,rightCube.partitioner.orNull)._2
    checkMetadataCompatible(leftCube.metadata,resampled.metadata)
    val rdd = new SpatialToSpacetimeJoinRdd[MultibandTile](leftCube, resampled)
    if(operator == null) {
      val outputCellType = leftCube.metadata.cellType.union(resampled.metadata.cellType)
      //TODO: what if extent of joined cube is larger than left cube?
      val updatedMetadata = leftCube.metadata.copy(cellType = outputCellType)
      return new ContextRDD(rdd.mapValues({case (l,r) =>
        if(swapOperands) {
          MultibandTile( r.bands.map(t=>safeConvert(t,updatedMetadata.cellType))  ++ l.bands.map(t=>safeConvert(t,updatedMetadata.cellType)))
        }else{
          MultibandTile(l.bands.map(t=>safeConvert(t,updatedMetadata.cellType)) ++ r.bands.map(t=>safeConvert(t,updatedMetadata.cellType)))
        }
      }), updatedMetadata)
    }else{

      val binaryOp = tileBinaryOp.getOrElse(operator, throw new UnsupportedOperationException("The operator: %s is not supported when merging cubes. Supported operators are: %s".format(operator, tileBinaryOp.keys.toString())))
      return new ContextRDD(rdd.mapValues({case (l,r) =>
        if(l.bandCount != r.bandCount){
          if(l.bandCount==0) {
            r
          }else if(r.bandCount==0) {
            l
          }
          throw new IllegalArgumentException("Merging cubes with an overlap resolver is only supported when band counts are the same. I got: %d and %d".format(l.bandCount, r.bandCount))
        }else{
          MultibandTile(l.bands.zip(r.bands).map(t => binaryOp.apply(if(swapOperands){Seq(t._2, t._1)} else Seq(t._1, t._2))))
        }

      }), leftCube.metadata)
    }
  }

  def checkMetadataCompatible[_](left:TileLayerMetadata[_],right:TileLayerMetadata[_]): Unit = {
    if(!left.layout.equals(right.layout)) {
      throw new IllegalArgumentException(s"merge_cubes: Merging cubes with incompatible layout, please use resample_cube_spatial to align layouts. LayoutLeft: ${left.layout} Layout (right): ${right.layout}")
    }
    if(!left.crs.equals(right.crs)) {
      throw new IllegalArgumentException(s"merge_cubes: Merging cubes with incompatible CRS, please use resample_cube_spatial to align coordinate systems. LayoutLeft: ${left.crs} Layout (right): ${right.crs}")
    }
  }

  def mergeSpatialCubes(leftCube: MultibandTileLayerRDD[SpatialKey], rightCube: MultibandTileLayerRDD[SpatialKey], operator:String): ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = {
    leftCube.sparkContext.setCallSite("merge_cubes - (x,y,bands)")
    val resampled = resampleCubeSpatial_spatial(rightCube,leftCube.metadata.crs,leftCube.metadata.layout,NearestNeighbor,leftCube.partitioner.orNull)._2
    checkMetadataCompatible(leftCube.metadata,resampled.metadata)
    val joined = outerJoin(leftCube,resampled)
    val outputCellType = leftCube.metadata.cellType.union(resampled.metadata.cellType)
    val updatedMetadata = leftCube.metadata.copy(bounds = joined.metadata,extent = leftCube.metadata.extent.combine(resampled.metadata.extent),cellType = outputCellType)
    mergeCubesGeneric(joined,operator,updatedMetadata,leftCube,rightCube)
  }

  def mergeCubes(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpaceTimeKey], operator:String): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    leftCube.sparkContext.setCallSite("merge_cubes - (x,y,bands,t)")
    val resampled = resampleCubeSpatial(rightCube,leftCube,NearestNeighbor)._2
    checkMetadataCompatible(leftCube.metadata,resampled.metadata)
    val joined = outerJoin(leftCube,resampled)
    val outputCellType = leftCube.metadata.cellType.union(resampled.metadata.cellType)

    val updatedMetadata = leftCube.metadata.copy(bounds = joined.metadata,extent = leftCube.metadata.extent.combine(resampled.metadata.extent),cellType = outputCellType)
    mergeCubesGeneric(joined,operator,updatedMetadata,leftCube,rightCube)
  }

  private def mergeCubesGeneric[K: Boundable: PartitionerIndex: ClassTag
  ](joined: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))] with Metadata[Bounds[K]], operator:String, metadata:TileLayerMetadata[K],leftCube: MultibandTileLayerRDD[K], rightCube: MultibandTileLayerRDD[K]): ContextRDD[K, MultibandTile, TileLayerMetadata[K]] = {
    val converted = joined.mapValues{t=> (t._1.map( x => safeConvert(x,metadata.cellType)),t._2.map( x => safeConvert(x,metadata.cellType)))}
    if(operator==null) {
      combine_bands(converted, leftCube, rightCube, metadata)
    }else{
      resolve_merge_overlap(converted, operator, metadata)
    }

  }

  private def combine_bands[K](joined: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))], leftCube: MultibandTileLayerRDD[K], rightCube: MultibandTileLayerRDD[K], updatedMetadata: TileLayerMetadata[K])(implicit kt: ClassTag[K], ord: Ordering[K] = null) = {
    leftCube.sparkContext.setJobDescription(s"Merge cubes: get bandcount ${leftCube.name}")
    val leftBandCount = RDDBandCount(leftCube)
    leftCube.sparkContext.setJobDescription(s"Merge cubes: get bandcount ${rightCube.name}")
    val rightBandCount = RDDBandCount(rightCube)
    leftCube.sparkContext.clearJobGroup()
    // Concatenation band counts are allowed to differ, but all resulting multiband tiles should have the same count
    new ContextRDD(joined.mapValues({
      case (None, Some(r)) => MultibandTile(Vector.fill(leftBandCount)(ArrayTile.empty(r.cellType, r.cols, r.rows)) ++ r.bands)
      case (Some(l), None) => MultibandTile(l.bands ++ Vector.fill(rightBandCount)(ArrayTile.empty(l.cellType, l.cols, l.rows)))
      case (Some(l), Some(r)) => {
        if (l.bandCount != leftBandCount || r.bandCount != rightBandCount) {
          throw new IllegalArgumentException(s"The number of bands in the metadata ${leftBandCount}/${rightBandCount} does not match the actual band count in the cubes (left/right): ${l.bandCount}/${r.bandCount}. You can fix this by explicitly specifying correct band labels.")
        } else {
          MultibandTile(l.bands ++ r.bands)
        }
      }
    }), updatedMetadata)
  }

  private def resolve_merge_overlap[K](joinedRDD: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))], operator: String, updatedMetadata: TileLayerMetadata[K])(implicit kt: ClassTag[K], ord: Ordering[K] = null) = {
    // Pairwise merging of bands.
    //in theory we should be able to reuse the OpenEOProcessScriptBuilder instead of using a string.
    //val binaryOp: Seq[Tile] => Seq[Tile] = operator.generateFunction()
    val binaryOp = tileBinaryOp.getOrElse(operator, throw new UnsupportedOperationException("The operator: %s is not supported when merging cubes. Supported operators are: %s".format(operator, tileBinaryOp.keys.toString())))

    new ContextRDD(joinedRDD.mapValues({ case (l, r) =>
      if (r.isEmpty || r.get.bandCount==0) l.get
      else if (l.isEmpty || l.get.bandCount==0) r.get
      else {
        if (l.get.bandCount != r.get.bandCount) {
          throw new IllegalArgumentException("Merging cubes with an overlap resolver is only supported when band counts are the same. I got: %d and %d".format(l.get.bandCount, r.get.bandCount))
        }
        MultibandTile(l.get.bands.zip(r.get.bands).map(t => binaryOp.apply(Seq(t._1, t._2))))
      }
    }), updatedMetadata)
  }

  def remove_overlap(datacube: MultibandTileLayerRDD[SpaceTimeKey], sizeX:Int, sizeY:Int, overlapX:Int, overlapY:Int): MultibandTileLayerRDD[SpaceTimeKey] = {
    datacube.withContext(_.mapValues(_.crop(overlapX,overlapY,overlapX+sizeX-1,overlapY+sizeY-1,Options(clamp=false)).mapBands{ (index,tile) => tile.toArrayTile()}))
  }


  def retile(datacube: Object, sizeX:Int, sizeY:Int, overlapX:Int, overlapY:Int): Object = {

    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        retileGeneric(rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]], sizeX, sizeY, overlapX, overlapY)
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey] =>
        retileGeneric(rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]], sizeX, sizeY, overlapX, overlapY)
      case _ => throw new IllegalArgumentException(s"Unsupported rdd type to retile: ${datacube}")
    }
  }
  def retileGeneric[K: SpatialComponent: ClassTag
  ](datacube: MultibandTileLayerRDD[K], sizeX:Int, sizeY:Int, overlapX:Int, overlapY:Int): MultibandTileLayerRDD[K] = {
    val regridded =
    if(sizeX >0 && sizeY > 0){
      RegridFixed(filterNegativeSpatialKeys(datacube),sizeX,sizeY)
    }else{
      datacube
    }
    if(overlapX >0 && overlapY > 0) {
      regridded.withContext(_.bufferTiles(_ => BufferSizes(overlapX,overlapX,overlapY,overlapY)).mapValues(tile=>{
        makeSquareTile(tile, sizeX, sizeY, overlapX, overlapY)
      }))
    }else{
      regridded
    }

  }


  def makeSquareTile(tile: BufferedTile[MultibandTile], sizeX: Int, sizeY: Int, overlapX: Int, overlapY: Int) = {
    val result = tile.tile

    val fullSizeX = sizeX + 2 * overlapX
    val fullSizeY = sizeY + 2 * overlapY
    if (result.cols == fullSizeX && result.rows == fullSizeY) {
      result
    }
    else if (tile.targetArea.colMin < overlapX && tile.targetArea.rowMin < overlapY) {
      result.mapBands { (index, t) => PaddedTile(t, overlapX, overlapY, fullSizeX, fullSizeY) }
    } else if (tile.targetArea.colMin < overlapX) {
      result.mapBands { (index, t) => PaddedTile(t, overlapX, 0, fullSizeX, fullSizeY) }
    } else if (tile.targetArea.rowMin < overlapY) {
      result.mapBands { (index, t) => PaddedTile(t, 0, overlapY, fullSizeX, fullSizeY) }
    } else {
      result.mapBands { (index, t) => PaddedTile(t, 0, 0, fullSizeX, fullSizeY) }
    }
  }

  def rasterMask_spacetime_spatial(datacube: MultibandTileLayerRDD[SpaceTimeKey], mask: MultibandTileLayerRDD[SpatialKey], replacement: java.lang.Double): MultibandTileLayerRDD[SpaceTimeKey] = {
    val resampledMask = resampleCubeSpatial_spatial(mask, datacube.metadata.crs, datacube.metadata.layout, ResampleMethods.NearestNeighbor, mask.partitioner.orNull)._2
    val joined = new SpatialToSpacetimeJoinRdd[MultibandTile](datacube, resampledMask)

    val replacementInt: Int = if (replacement == null) NODATA else replacement.intValue()
    val replacementDouble: Double = if (replacement == null) doubleNODATA else replacement
    val masked = joined.mapValues(t => {
      val dataTile = t._1

      val maskTile = t._2
      var maskIndex = 0
      dataTile.mapBands((index,tile) =>{
        if(dataTile.bandCount == maskTile.bandCount){
          maskIndex = index
        }
        tile.dualCombine(maskTile.band(maskIndex))((v1,v2) => if (v2 != 0 && isData(v1)) replacementInt else v1)((v1,v2) => if (v2 != 0.0 && isData(v1)) replacementDouble else v1)
      })

    })

    new ContextRDD(masked, datacube.metadata).convert(datacube.metadata.cellType)
  }

  def rasterMask(datacube: MultibandTileLayerRDD[SpaceTimeKey], mask: MultibandTileLayerRDD[SpaceTimeKey], replacement: java.lang.Double): MultibandTileLayerRDD[SpaceTimeKey] = {
    val resampledMask = resampleCubeSpatial_spacetime(mask, datacube.metadata.crs, datacube.metadata.layout, ResampleMethods.NearestNeighbor, mask.partitioner.orNull)._2
    rasterMaskGeneric(datacube,resampledMask,replacement).convert(datacube.metadata.cellType)
  }

  def rasterMask_spatial_spatial(datacube: MultibandTileLayerRDD[SpatialKey], mask: MultibandTileLayerRDD[SpatialKey], replacement: java.lang.Double): MultibandTileLayerRDD[SpatialKey] = {
    val resampledMask = resampleCubeSpatial_spatial(mask, datacube.metadata.crs, datacube.metadata.layout, ResampleMethods.NearestNeighbor, mask.partitioner.orNull)._2
    rasterMaskGeneric(datacube,resampledMask,replacement).convert(datacube.metadata.cellType)
  }

  def rasterMaskGeneric[K: Boundable: PartitionerIndex: ClassTag,M: GetComponent[*, Bounds[K]]]
  (datacube: RDD[(K,MultibandTile)] with Metadata[M], mask: RDD[(K,MultibandTile)] with Metadata[M], replacement: java.lang.Double): RDD[(K,MultibandTile)] with Metadata[M] = {
    datacube.sparkContext.setCallSite("mask with rastercube")
    DatacubeSupport.rasterMaskGeneric(datacube, mask, replacement)
  }

  /**
    * Implementation of openeo apply_kernel
    * https://open-eo.github.io/openeo-api/v/0.4.2/processreference/#apply_kernel
    * celltype is automatically converted to an appropriate celltype, depending on the kernel.
    *
    *
    * @param datacube
    * @param kernel The kernel to be applied on the data cube. The kernel has to be as many dimensions as the data cube has dimensions.
    *
    *               This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
    * @tparam K
    * @return
    */
  def apply_kernel[K: SpatialComponent: ClassTag](datacube:MultibandTileLayerRDD[K],kernel:Tile): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = {
    datacube.sparkContext.setCallSite(s"apply_kernel")
    val k = new Kernel(kernel)
    val outputCellType = datacube.convert(datacube.metadata.cellType.union(kernel.cellType))
    if (kernel.cols > 10 || kernel.rows > 10) {
      MultibandFocalOperation(outputCellType, k, None) { (tile, bounds: Option[GridBounds[Int]]) => {
        FFTConvolve(tile, kernel).crop(bounds.get)
      }
      }
    } else {
      MultibandFocalOperation(outputCellType, k, None) { (tile, bounds) => Convolve(tile, k, bounds, TargetCell.All) }
    }
  }

  /**
    * Apply kernel for spacetime data cubes.
    * @see #apply_kernel
    *
    */
  def apply_kernel_spacetime(datacube:MultibandTileLayerRDD[SpaceTimeKey],kernel:Tile): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    return apply_kernel(datacube,kernel)
  }

  /**
    * Apply kernel for spatial data cubes.
    * @see #apply_kernel
    */
  def apply_kernel_spatial(datacube:MultibandTileLayerRDD[SpatialKey], kernel:Tile): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    return apply_kernel(datacube,kernel)
  }

  def write_geotiffs(datacube:MultibandTileLayerRDD[SpatialKey],location: String, zoom:Int) = {
    Filesystem.ensureDirectory(new File(location).getAbsolutePath)
    //val currentLayout = datacube.metadata.layout.tileLayout
    //datacube.tileToLayout(datacube.metadata.copy(layout =  datacube.metadata.layout.copy(tileLayout = TileLayout() )))
    datacube.toGeoTiffs(Tags.empty,GeoTiffOptions(DeflateCompression)).foreach(t=>{
      val path = location + "/tile" + t._1.col.toString + "_" + t._1.row.toString + ".tiff"
      t._2.write(path,true)
    })
  }

  def crop_spatial(datacube:MultibandTileLayerRDD[SpatialKey], bounds:Extent):MultibandTileLayerRDD[SpatialKey]={
    datacube.crop(bounds,Options(force=false,clamp=true))
  }

  def crop_spacetime(datacube:MultibandTileLayerRDD[SpaceTimeKey], bounds:Extent):MultibandTileLayerRDD[SpaceTimeKey]={
    datacube.crop(bounds,Options(force=false,clamp=true))
  }

  def crop(datacube:Object, bounds:Extent):Object ={
    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        crop_spatial(rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]],bounds)
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey]  =>
        crop_spacetime(rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]],bounds)
      case _ => throw new IllegalArgumentException("Unsupported rdd type to crop: ${rdd}")
    }

  }


  def crop_metadata_generic[K: SpatialComponent: ClassTag](datacube:MultibandTileLayerRDD[K],bounds:Extent):MultibandTileLayerRDD[K] = {
    //taking intersection avoids that we enlarge the extent, and go out of reach for current partitioner, also makes sense that multiple spatial filters take intersection
    val maybeIntersection = datacube.metadata.extent.intersection(bounds)
    val intersection =
    if(!maybeIntersection.isDefined) {
      logger.warn("Intersection between the current extent and extent provided by spatial filtering operation is empty!")
      bounds
    }else{
      maybeIntersection.get
    }

    val newBounds = datacube.metadata.extentToBounds(intersection)
    val updatedMetadata = datacube.metadata.copy(extent = intersection, bounds = datacube.metadata.bounds.get.setSpatialBounds(newBounds))
    ContextRDD(datacube.rdd.filter(t=>{
      val key: SpatialKey = t._1.getComponent[SpatialKey]
      newBounds.contains(key.col,key.row)
    }),updatedMetadata)
  }

  /**
   * Crop a datacube by only cropping metadata and filtering keys that are out of range.
   * This is a less expensive operation than actually cropping all tiles, or regridding, which can be applied by the output format if needed..
   *
   * @param datacube
   * @param bounds
   * @return
   */
  def crop_metadata(datacube:Object, bounds:Extent):Object ={
    datacube match {
      case rdd1 if datacube.asInstanceOf[MultibandTileLayerRDD[SpatialKey]].metadata.bounds.get.maxKey.isInstanceOf[SpatialKey] =>
        crop_metadata_generic(rdd1.asInstanceOf[MultibandTileLayerRDD[SpatialKey]],bounds)
      case rdd2 if datacube.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]].metadata.bounds.get.maxKey.isInstanceOf[SpaceTimeKey]  =>
        crop_metadata_generic(rdd2.asInstanceOf[MultibandTileLayerRDD[SpaceTimeKey]],bounds)
      case _ => throw new IllegalArgumentException("Unsupported rdd type to crop: ${rdd}")
    }

  }

  def toSclDilationMask(datacube: MultibandTileLayerRDD[SpaceTimeKey], erosionKernelSize: Int, mask1Values: util.List[Int], mask2Values: util.List[Int], kernel1Size: Int, kernel2Size: Int): MultibandTileLayerRDD[SpaceTimeKey] = {
    val filter = new SCLConvolutionFilter(erosionKernelSize, mask1Values, mask2Values, kernel1Size, kernel2Size)
    // Buffer each input tile so that the dilation is consistent across tile boundaries.
    val bufferInPixels: Int = filter.bufferInPixels
    val bufferedRDD: RDD[(SpaceTimeKey, BufferedTile[MultibandTile])] = datacube.bufferTiles(bufferInPixels)
    // Create mask.
    val mask: RDD[(SpaceTimeKey, MultibandTile)] = bufferedRDD.mapValues((tile: BufferedTile[MultibandTile]) => {
      val originalBounds = tile.targetArea
      MultibandTile(filter.createMask(tile.tile).crop(originalBounds))
    })
    val updatedMetadata = datacube.metadata.copy(cellType = BitCellType)
    ContextRDD(mask, updatedMetadata)
  }

  def mergeTiles(tiles: MultibandTileLayerRDD[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    ContextRDD(tiles.groupByKey().mapValues { iter => iter.reduce { _ merge _ } }, tiles.metadata)
  }
}



