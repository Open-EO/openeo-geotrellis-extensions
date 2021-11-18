package org.openeo.geotrellis

import geotrellis.layer.SpatialKey._
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.buffer.{BufferSizes, BufferedTile}
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, Tags}
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.mapalgebra.local._
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.{MultibandTileLayerRDD, _}
import geotrellis.util._
import geotrellis.vector.io.json.JsonFeatureCollection
import geotrellis.vector.{Extent, PolygonFeature}
import org.apache.spark.rdd._
import org.apache.spark.{Partitioner, SparkContext}
import org.openeo.geotrellis.focal._
import org.openeo.geotrelliscommon.{ByTileSpatialPartitioner, FFTConvolve, SpaceTimeByMonthPartitioner}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.{Instant, ZonedDateTime}
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, immutable, mutable}
import scala.reflect._


object OpenEOProcesses{
  private def timeseriesForBand(b: Int, values: Iterable[(SpaceTimeKey, MultibandTile)]) = {
    MultibandTile(values.toList.sortBy(_._1.instant).map(_._2.band(b)))
  }
}


class OpenEOProcesses extends Serializable {

  import OpenEOProcesses._

  val unaryProcesses: Map[String, Tile => Tile] = Map(
    "absolute" -> Abs.apply,
    //TODO "exp"
    "ln" -> Log.apply,
    //TODO "log"
    "sqrt" -> Sqrt.apply,
    "ceil" -> Ceil.apply,
    "floor" -> Floor.apply,
    //TODO: "int" integer part of a number
    "round" -> Round.apply,
    "arccos" -> Acos.apply,
    //TODO "arccos" -> Acosh.apply,
    "arcsin" -> Asin.apply,
    "arctan" -> Atan.apply,
    //TODO: arctan 2 is not unary! "arctan2" -> Atan2.apply,
    "cos" -> Cos.apply,
    "cosh" -> Cosh.apply,
    "sin" -> Sin.apply,
    "sinh" -> Sinh.apply,
    "tan" -> Tan.apply,
    "tanh" -> Tanh.apply
  )

  val tileBinaryOp: Map[String, LocalTileBinaryOp] = Map(
    "or" -> Or,
    "and" -> And,
    "divide" -> Divide,
    "max" -> Max,
    "min" -> Min,
    "multiply" -> Multiply,
    "product" -> Multiply,
    "add" -> Add,
    "sum" -> Add,
    "subtract" -> Subtract,
    "xor" -> Xor
  )

  def applyProcess[K](datacube:MultibandTileLayerRDD[K], process:String): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]= {
    val proc = unaryProcesses(process)
    return ContextRDD(
      datacube.map(multibandtile => (
        multibandtile._1,
        multibandtile._2.mapBands((b, tile) => proc(tile))
      )),
      datacube.metadata
    )
  }


  /**
   * apply_dimension, over time dimension
   * @param datacube
   * @param scriptBuilder
   * @param context
   * @return
   */
  def applyTimeDimension(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder,context: java.util.Map[String,Any]):MultibandTileLayerRDD[SpaceTimeKey] = {
    val function = scriptBuilder.generateFunction(context.asScala.toMap)
    val rdd =  groupOnTimeDimension(datacube).flatMap{ tiles => {
      val values = tiles._2
      val aTile = firstTile(values.map(_._2))
      val labels = values.map(_._1).toList.sortBy(_.instant)
      val resultMap: mutable.Map[SpaceTimeKey,mutable.ListBuffer[Tile]] = mutable.Map()
      for( b <- 0 until aTile.bandCount){

        val temporalTile = timeseriesForBand(b, values)
        val resultTiles = function(temporalTile.bands)
        var resultLabels: Iterable[(SpaceTimeKey,Tile)] = labels.zip(resultTiles)
        resultLabels.foreach(result => resultMap.getOrElseUpdate(result._1, mutable.ListBuffer()).append(result._2))

      }
      resultMap.map(tuple => (tuple._1,MultibandTile(tuple._2)))

    }}
    ContextRDD(rdd,datacube.metadata)
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
    val function = scriptBuilder.generateFunction(context.asScala.toMap)
    val groupedOnTime: RDD[(SpatialKey, Iterable[(SpaceTimeKey, MultibandTile)])] = groupOnTimeDimension(datacube)
    val resultRDD = groupedOnTime.mapValues{ tiles => {
      val aTile = firstTile(tiles.map(_._2))
      val resultTile = mutable.ListBuffer[Tile]()
      for( b <- 0 until aTile.bandCount){
        val temporalTile = timeseriesForBand(b, tiles)
        resultTile.appendAll(function(temporalTile.bands))
      }
      if(resultTile.size>0) {
        MultibandTile(resultTile)
      }else{
        new EmptyMultibandTile(aTile.cols,aTile.rows,aTile.cellType)
      }

    }}

    ContextRDD(resultRDD,datacube.metadata.copy(bounds = datacube.metadata.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]].toSpatial))
  }

  private def groupOnTimeDimension(datacube: MultibandTileLayerRDD[SpaceTimeKey]) = {
    val targetBounds = datacube.metadata.bounds.asInstanceOf[KeyBounds[SpaceTimeKey]].toSpatial
    implicit val index = ByTileSpatialPartitioner
    val partitioner: Partitioner = new SpacePartitioner(targetBounds)

    val groupedOnTime = datacube.groupBy[SpatialKey]((t: (SpaceTimeKey, MultibandTile)) => t._1.spatialKey, partitioner)
    groupedOnTime
  }

  private def firstTile(tiles: Iterable[MultibandTile]) = {
    tiles.filterNot(_.isInstanceOf[EmptyMultibandTile]).headOption.getOrElse(tiles.head)
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
    val timePeriods: Seq[Iterable[Instant]] = JavaConverters.iterableAsScalaIterableConverter(intervals).asScala.map(s => Instant.parse(s)).grouped(2).toList
    val labelsDates = labels.asScala.map(ZonedDateTime.parse(_))
    val periodsToLabels: Seq[(Iterable[Instant], String)] = timePeriods.zip(labels.asScala)
    val function = scriptBuilder.generateFunction(context.asScala.toMap)

    val allPossibleKeys: immutable.Seq[SpatialKey] = datacube.metadata.tileBounds
      .coordsIter
      .map { case (x, y) => SpatialKey(x, y) }
      .toList
    val allPossibleSpacetime =  allPossibleKeys.flatMap(x => labelsDates.map(y => (SpaceTimeKey(x, TemporalKey(y)),null)))

    val allKeysRDD: RDD[(SpaceTimeKey, Null)] = SparkContext.getOrCreate().parallelize(allPossibleSpacetime)
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
    }).groupByKey().mapValues( tiles => {
      val aTile = firstTile(tiles)

      val resultTiles: mutable.ArrayBuffer[Tile] = mutable.ArrayBuffer[Tile]()
      for( b <- 0 until aTile.bandCount){
        val temporalTile = MultibandTile(tiles.map(_.band(b)))
        val aggregatedTiles: Seq[Tile] = function(temporalTile.bands)
        resultTiles += aggregatedTiles.head

      }
      MultibandTile(resultTiles)
    })

    val cols = datacube.metadata.tileLayout.tileCols
    val rows = datacube.metadata.tileLayout.tileRows
    val cellType = datacube.metadata.cellType

    //ArrayMultibandTile.empty(datacube.metadata.cellType,1,0,0)
    val filledRDD: RDD[(SpaceTimeKey, MultibandTile)] = tilesByInterval.rightOuterJoin(allKeysRDD).mapValues(_._1.getOrElse(new EmptyMultibandTile(cols,rows,cellType)))
    return ContextRDD(filledRDD, datacube.metadata)

  }

  def mapBands(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]= {
    mapBandsGeneric(datacube,scriptBuilder)
  }

  def mapBandsGeneric[K:ClassTag](datacube:MultibandTileLayerRDD[K], scriptBuilder:OpenEOProcessScriptBuilder): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]={
    val function = scriptBuilder.generateFunction()
    return datacube.withContext(new org.apache.spark.rdd.PairRDDFunctions[K,MultibandTile](_).mapValues(tile => {
      if (!tile.isInstanceOf[EmptyMultibandTile]) {
        val resultTiles = function(tile.bands)
        MultibandTile(resultTiles)
      }else{
        tile
      }
    }).filter(_._2.bands.exists(!_.isNoDataTile)))
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
  def vectorize(datacube:MultibandTileLayerRDD[SpaceTimeKey]): Array[PolygonFeature[Int]] = {
    val layout = datacube.metadata.layout

    //naive approach: combine tiles and hope that we don't exceed the max size
    //if we exceed the max, vectorize will run on separate tiles, and we'll need to merge results
    val newCols = Math.min(256*20,layout.cols)
    val newRows = Math.min(256*20,layout.rows)

    val singleBandLayer: TileLayerRDD[SpaceTimeKey] = datacube.withContext(_.mapValues(_.band(0)))
    val retiled = singleBandLayer.regrid(newCols.intValue(),newRows.intValue())
    val collectedFeatures = retiled.toRasters.mapValues(_.toVector()).flatMap(_._2).collect()
    return collectedFeatures
  }

  def vectorize(datacube:MultibandTileLayerRDD[SpaceTimeKey], outputFile:String): Unit = {
    val features = this.vectorize(datacube)
    val json = JsonFeatureCollection(features).asJson
    val epsg = "epsg:"+datacube.metadata.crs.epsgCode.get
    val crs_json = _root_.io.circe.parser.parse("""{"crs":{"type":"name","properties":{"name":"THE_CRS"}}}""".replace("THE_CRS",epsg))
    val jsonWithCRS = json.deepMerge(crs_json.right.get)
    Files.write(Paths.get(outputFile), jsonWithCRS.toString().getBytes(StandardCharsets.UTF_8))
  }

  def applySpacePartitioner(datacube: RDD[(SpaceTimeKey, MultibandTile)], keyBounds: KeyBounds[SpaceTimeKey]): RDD[(SpaceTimeKey, MultibandTile)] = {
    datacube.partitionBy( SpacePartitioner(keyBounds))
  }

  private def outerJoin[K: Boundable: PartitionerIndex: ClassTag,
    M: GetComponent[*, Bounds[K]],
    M1: GetComponent[*, Bounds[K]]
  ](leftCube: RDD[(K, MultibandTile)] with Metadata[M], rightCube: RDD[(K, MultibandTile)] with Metadata[M1]): RDD[(K, (Option[MultibandTile], Option[MultibandTile]))] with Metadata[Bounds[K]] = {

    val kbLeft: Bounds[K] = leftCube.metadata.getComponent[Bounds[K]]
    val kbRight: Bounds[K] = rightCube.metadata.getComponent[Bounds[K]]
    val kb: Bounds[K] = kbLeft.combine(kbRight)
    val part = SpacePartitioner(kb)

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

  /**
   * Get band count used in RDD (each tile in RDD should have same band count)
   */
  def RDDBandCount[K](cube: MultibandTileLayerRDD[K]): Int = {
    // For performance reasons we only check a small subset of tile band counts
    println("Computing number of bands in cube: " + cube.metadata)
    val counts = cube.take(10).map({ case (k, t) => t.bandCount }).distinct

    if(counts.size==0){
      if(cube.isEmpty())
        println("This cube is empty, no band count.")
      else
        println("This cube is not empty, but could not determine band count.")
      return 1
    }
    if (counts.size != 1) {
      throw new IllegalArgumentException("Cube doesn't have single consistent band count across tiles: [%s]".format(counts.mkString(", ")))
    }
    counts(0)
  }

  def resampleCubeSpatial(data: MultibandTileLayerRDD[SpaceTimeKey], target: MultibandTileLayerRDD[SpaceTimeKey], method:ResampleMethod): (Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    data.reproject(target.metadata.crs,target.metadata.layout,16,method,target.partitioner)
  }

  def resampleCubeSpatial_spacetime(data: MultibandTileLayerRDD[SpaceTimeKey],crs:CRS,layout:LayoutDefinition, method:ResampleMethod, partitioner:Partitioner): (Int, MultibandTileLayerRDD[SpaceTimeKey]) = {
    if(partitioner==null) {
      data.reproject(crs,layout,16,method,new SpacePartitioner(data.metadata.bounds))
    }else{
      data.reproject(crs,layout,16,method,Option(partitioner))
    }
  }

  def resampleCubeSpatial_spatial(data: MultibandTileLayerRDD[SpatialKey],crs:CRS,layout:LayoutDefinition, method:ResampleMethod, partitioner:Partitioner): (Int, MultibandTileLayerRDD[SpatialKey]) = {
    if(partitioner==null) {
      data.reproject(crs,layout,16,method,new SpacePartitioner(data.metadata.bounds))
    }else{
      data.reproject(crs,layout,16,method,Option(partitioner))
    }
  }

  def mergeCubes_SpaceTime_Spatial(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpatialKey], operator:String, swapOperands:Boolean): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val rdd = new SpatialToSpacetimeJoinRdd[MultibandTile](leftCube, rightCube)
    val binaryOp = tileBinaryOp.getOrElse(operator, throw new UnsupportedOperationException("The operator: %s is not supported when merging cubes. Supported operators are: %s".format(operator, tileBinaryOp.keys.toString())))

    return new ContextRDD(rdd.mapValues({case (l,r) =>
      if(l.bandCount != r.bandCount){
        throw new IllegalArgumentException("Merging cubes with an overlap resolver is only supported when band counts are the same. I got: %d and %d".format(l.bandCount, r.bandCount))
      }
      MultibandTile(l.bands.zip(r.bands).map(t => binaryOp.apply(if(swapOperands){Seq(t._2, t._1)} else Seq(t._1, t._2))))

    }), leftCube.metadata)
  }

  def mergeSpatialCubes(leftCube: MultibandTileLayerRDD[SpatialKey], rightCube: MultibandTileLayerRDD[SpatialKey], operator:String): ContextRDD[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]] = {
    val joined = outerJoin(leftCube,rightCube)
    val outputCellType = leftCube.metadata.cellType.union(rightCube.metadata.cellType)
    val updatedMetadata = leftCube.metadata.copy(bounds = joined.metadata,extent = leftCube.metadata.extent.combine(rightCube.metadata.extent),cellType = outputCellType)
    mergeCubesGeneric(joined,operator,updatedMetadata,leftCube,rightCube)
  }

  def mergeCubes(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpaceTimeKey], operator:String): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val joined = outerJoin(leftCube,rightCube)
    val outputCellType = leftCube.metadata.cellType.union(rightCube.metadata.cellType)

    val updatedMetadata = leftCube.metadata.copy(bounds = joined.metadata,extent = leftCube.metadata.extent.combine(rightCube.metadata.extent),cellType = outputCellType)
    mergeCubesGeneric(joined,operator,updatedMetadata,leftCube,rightCube)
  }

  private def mergeCubesGeneric[K: Boundable: PartitionerIndex: ClassTag
  ](joined: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))] with Metadata[Bounds[K]], operator:String, metadata:TileLayerMetadata[K],leftCube: MultibandTileLayerRDD[K], rightCube: MultibandTileLayerRDD[K]): ContextRDD[K, MultibandTile, TileLayerMetadata[K]] = {

    val converted = joined.mapValues{t=> (t._1.map(_.convert(metadata.cellType)),t._2.map(_.convert(metadata.cellType)))}
    if(operator==null) {
      combine_bands(converted, leftCube, rightCube, metadata)
    }else{
      resolve_merge_overlap(converted, operator, metadata)
    }

  }

  private def combine_bands[K](joined: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))], leftCube: MultibandTileLayerRDD[K], rightCube: MultibandTileLayerRDD[K], updatedMetadata: TileLayerMetadata[K])(implicit kt: ClassTag[K], ord: Ordering[K] = null) = {
    leftCube.sparkContext.setJobDescription("Merge cubes: get bandcount")
    val leftBandCount = RDDBandCount(leftCube)
    val rightBandCount = RDDBandCount(rightCube)
    leftCube.sparkContext.clearJobGroup()
    // Concatenation band counts are allowed to differ, but all resulting multiband tiles should have the same count
    new ContextRDD(joined.mapValues({
      case (None, Some(r)) => MultibandTile(Vector.fill(leftBandCount)(ArrayTile.empty(r.cellType, r.cols, r.rows)) ++ r.bands)
      case (Some(l), None) => MultibandTile(l.bands ++ Vector.fill(rightBandCount)(ArrayTile.empty(l.cellType, l.cols, l.rows)))
      case (Some(l), Some(r)) => MultibandTile(l.bands ++ r.bands)
    }), updatedMetadata)
  }

  private def resolve_merge_overlap[K](joinedRDD: RDD[(K, (Option[MultibandTile], Option[MultibandTile]))], operator: String, updatedMetadata: TileLayerMetadata[K])(implicit kt: ClassTag[K], ord: Ordering[K] = null) = {
    // Pairwise merging of bands.
    //in theory we should be able to reuse the OpenEOProcessScriptBuilder instead of using a string.
    //val binaryOp: Seq[Tile] => Seq[Tile] = operator.generateFunction()
    val binaryOp = tileBinaryOp.getOrElse(operator, throw new UnsupportedOperationException("The operator: %s is not supported when merging cubes. Supported operators are: %s".format(operator, tileBinaryOp.keys.toString())))

    new ContextRDD(joinedRDD.mapValues({ case (l, r) =>
      if (r.isEmpty) l.get
      else if (l.isEmpty) r.get
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


  def retile(datacube: MultibandTileLayerRDD[SpaceTimeKey], sizeX:Int, sizeY:Int, overlapX:Int, overlapY:Int): MultibandTileLayerRDD[SpaceTimeKey] = {
    val regridded =
    if(sizeX >0 && sizeY > 0){
      datacube.regrid(sizeX,sizeY)
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
    val joined = new SpatialToSpacetimeJoinRdd[MultibandTile](datacube, mask)

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
    val joined = datacube.spatialLeftOuterJoin(mask)
    val replacementInt: Int = if (replacement == null) NODATA else replacement.intValue()
    val replacementDouble: Double = if (replacement == null) doubleNODATA else replacement
    val masked = joined.mapValues(t => {
      val dataTile = t._1
      if (!t._2.isEmpty) {
        val maskTile = t._2.get
        var maskIndex = 0
        dataTile.mapBands((index,tile) =>{
          if(dataTile.bandCount == maskTile.bandCount){
            maskIndex = index
          }
          tile.dualCombine(maskTile.band(maskIndex))((v1,v2) => if (v2 != 0 && isData(v1)) replacementInt else v1)((v1,v2) => if (v2 != 0.0 && isData(v1)) replacementDouble else v1)
        })

      } else {
        dataTile
      }

    })

    new ContextRDD(masked, datacube.metadata).convert(datacube.metadata.cellType)
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
    datacube.crop(bounds,Options(force=false))
  }

}
