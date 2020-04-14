package org.openeo.geotrellis

import java.io.File
import java.time.{Instant, ZonedDateTime}

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, Tags}
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.mapalgebra.local._
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import org.openeo.geotrellisaccumulo.SpaceTimeByMonthPartitioner
import geotrellis.util.Filesystem
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.openeo.geotrellis.focal._

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class OpenEOProcesses extends Serializable {
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
    return ContextRDD(datacube.map(multibandtile => (multibandtile._1,multibandtile._2.mapBands((b,t) => unaryProcesses.get(process).get(t) ))),datacube.metadata)
  }

  def mapInstantToInterval(datacube:MultibandTileLayerRDD[SpaceTimeKey], intervals:java.lang.Iterable[String],labels:java.lang.Iterable[String]) :MultibandTileLayerRDD[SpaceTimeKey] = {
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

  def mapBands(datacube:MultibandTileLayerRDD[SpaceTimeKey], scriptBuilder:OpenEOProcessScriptBuilder): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]= {
    mapBandsGeneric(datacube,scriptBuilder)
  }

  def mapBandsGeneric[K:ClassTag](datacube:MultibandTileLayerRDD[K], scriptBuilder:OpenEOProcessScriptBuilder): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]={
    val function = scriptBuilder.generateFunction()
    return datacube.withContext(new org.apache.spark.rdd.PairRDDFunctions[K,MultibandTile](_).mapValues(tile => {
      val resultTiles = function(tile.bands)
      MultibandTile(resultTiles)
    }))
  }

  def applySpacePartitioner(datacube: RDD[(SpaceTimeKey, MultibandTile)], keyBounds: KeyBounds[SpaceTimeKey]): RDD[(SpaceTimeKey, MultibandTile)] = {
    datacube.partitionBy( SpacePartitioner(keyBounds))
  }

  def outerJoin(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpaceTimeKey]): RDD[(SpaceTimeKey, (Option[MultibandTile], Option[MultibandTile]))] with Metadata[Bounds[SpaceTimeKey]] = {
    val kb: Bounds[SpaceTimeKey] = leftCube.metadata.bounds.combine(rightCube.metadata.bounds)
    val part = SpacePartitioner(kb)

    val joinRdd =
      new CoGroupedRDD[SpaceTimeKey](List(part(leftCube), part(rightCube)), part)
        .flatMapValues { case Array(l, r) =>
          if (l.isEmpty)
            for (v <- r.iterator) yield (None, Some(v))
          else if (r.isEmpty)
            for (v <- l.iterator) yield (Some(v), None)
          else
            for (v <- l.iterator; w <- r.iterator) yield (Some(v), Some(w))
        }.asInstanceOf[RDD[(SpaceTimeKey, (Option[MultibandTile], Option[MultibandTile]))]]

    ContextRDD(joinRdd, part.bounds)
  }

  /**
   * Get band count used in RDD (each tile in RDD should have same band count)
   */
  def RDDBandCount[K](cube: MultibandTileLayerRDD[K]): Int = {
    val counts = cube.map({ case (k, t) => t.bandCount }).distinct().collect()
    if (counts.length != 1) {
      throw new IllegalArgumentException("Cube doesn't have single consistent band count across tiles: [%s]".format(counts.mkString(", ")))
    }
    counts(0)
  }

  def mergeCubes(leftCube: MultibandTileLayerRDD[SpaceTimeKey], rightCube: MultibandTileLayerRDD[SpaceTimeKey], operator:String): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val leftBandCount = RDDBandCount(leftCube)
    val rightBandCount = RDDBandCount(rightCube)
    val joined = outerJoin(leftCube,rightCube)
    val updatedMetadata: TileLayerMetadata[SpaceTimeKey] = leftCube.metadata.copy(bounds = joined.metadata,extent = leftCube.metadata.extent.combine(rightCube.metadata.extent))

    if(operator==null) {
      // Concatenation
      new ContextRDD(joined.mapValues({
        case (None, Some(r)) => MultibandTile(Vector.fill(leftBandCount)(ArrayTile.empty(r.cellType, r.cols, r.rows)) ++ r.bands)
        case (Some(l), None) => MultibandTile(l.bands ++ Vector.fill(leftBandCount)(ArrayTile.empty(l.cellType, l.cols, l.rows)))
        case (Some(l), Some(r)) => MultibandTile(l.bands ++ r.bands)
      }), updatedMetadata)
    }else{
      // Pairwise merging of bands.
      //in theory we should be able to reuse the OpenEOProcessScriptBuilder instead of using a string.
      //val binaryOp: Seq[Tile] => Seq[Tile] = operator.generateFunction()
      val binaryOp = tileBinaryOp.getOrElse(operator, throw new UnsupportedOperationException("The operator: %s is not supported when merging cubes. Supported operators are: %s".format(operator, tileBinaryOp.keys.toString())))
      if (leftBandCount != rightBandCount) {
        throw new IllegalArgumentException("Merging cubes with an overlap resolver is only supported when band counts are the same. I got: %d and %d".format(leftBandCount, rightBandCount))
      }
      new ContextRDD(joined.mapValues({case (l,r) =>
        if(r.isEmpty) l.get
        else if(l.isEmpty) r.get
        else MultibandTile(l.get.bands.zip(r.get.bands).map(t => binaryOp.apply(Seq(t._1, t._2))))
      }), updatedMetadata)
    }

  }


  def rasterMask(datacube: MultibandTileLayerRDD[SpaceTimeKey], mask: MultibandTileLayerRDD[SpaceTimeKey], replacement: java.lang.Double): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
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

    new ContextRDD(masked, datacube.metadata)
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

}
