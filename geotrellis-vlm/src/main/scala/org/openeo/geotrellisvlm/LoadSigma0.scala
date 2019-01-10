package org.openeo.geotrellisvlm

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId, ZonedDateTime}

import geotrellis.contrib.vlm.RasterSourceRDD.PARTITION_BYTES
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster._
import geotrellis.raster.render.Png
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.{CellSize, MultibandTile, RasterExtent}
import geotrellis.spark.join.SpatialJoin
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuilder
import scala.reflect.ClassTag
import scala.math._

object LoadSigma0 {
  def createRDD(layername: String, envelope: Extent, str: String, startDate: Option[ZonedDateTime], endDate: Option[ZonedDateTime]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {

    val layout = new GlobalLayout(256,14,0.1)
    val localLayout = new LocalLayout(256,256)

    val utm31 = CRS.fromEpsgCode(32631)

    val source = new geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource("/home/niels/Data/S1B_IW_GRDH_SIGMA0_DV_20180502T054914_DESCENDING_37_B39C_V110_VV.tif")
    val source2 = new geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource("/home/niels/Data/S1B_IW_GRDH_SIGMA0_DV_20181224T173153_ASCENDING_161_42B7_V110_VV.tif")
    //in the case of global layout, we need to warp input into the right format
//    val source = new geotrellis.contrib.vlm.gdal.GDALRasterSource("/home/driesj/alldata/CGS_S1/S1B_IW_GRDH_SIGMA0_DV_20181216T054946_DESCENDING_37_130C_V110_VH.tif")
//    val secondfile = "/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2018/12/09/S1B_IW_GRDH_SIGMA0_DV_20181209T055749_DESCENDING_110_520B_V110/S1B_IW_GRDH_SIGMA0_DV_20181209T055749_DESCENDING_110_520B_V110_VH.tif"
//    val source2 = new geotrellis.contrib.vlm.gdal.GDALRasterSource(secondfile)
    val envelope = source.extent
    val croppedSource = source.resampleToRegion(RasterExtent(envelope,source.cellSize))
    val localLayoutDefWithZoom = localLayout.layoutDefinitionWithZoom(utm31,envelope,croppedSource.cellSize)
    val conf = new SparkConf().setMaster("local[4]").setAppName("Geotiffloading")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","1024m")
    implicit val sc = SparkContext.getOrCreate()

    var rdd = loadRasterRegions(Seq(source,source2.resampleToRegion(RasterExtent(envelope,source.cellSize))),localLayoutDefWithZoom._1)

    val tiledLayer = rdd.withContext(r => r.mapValues(f => f.raster.get.tile))

    return tiledLayer

  }



  def test(): Unit = {

//    val layout = GlobalLayout(256,14,0.1)
    val localLayout = new LocalLayout(256,256)

//    val utm31 = CRS.fromEpsgCode(32631)
    val sourceVV = new GeoTiffRasterSource("/home/niels/Data/20180502/S1B_IW_GRDH_SIGMA0_DV_20180502T054914_DESCENDING_37_B39C_V110_VV.tif")
    val sourceVH = new GeoTiffRasterSource("/home/niels/Data/20180502/S1B_IW_GRDH_SIGMA0_DV_20180502T054914_DESCENDING_37_B39C_V110_VH.tif")

//    val layoutDefWithZoom = layout.layoutDefinitionWithZoom(WebMercator, WebMercator.worldExtent, CellSize(10,10))
    //in the case of global layout, we need to warp input into the right format
    //val source = new geotrellis.contrib.vlm.gdal.GDALReprojectRasterSource("/home/driesj/alldata/S1B_IW_GRDH_1SDV_20180713T055010_20180713T055035_011788_015B03_5310.zip.tif",WebMercator, options = Reproject.Options(targetCellSize = Some(layoutDefWithZoom._1.cellSize)))

    val localLayoutDefWithZoom = localLayout.layoutDefinitionWithZoom(WebMercator, sourceVV.extent, sourceVH.cellSize)
//    val layoutTileSourceVV = LayoutTileSource(sourceVV,localLayoutDefWithZoom._1)
//    val layoutTileSourceVH = LayoutTileSource(sourceVH,localLayoutDefWithZoom._1)
//    val spatialKeys = layoutTileSourceVV.keys()
//    
//    var break = 0
//    for (key <- spatialKeys) {
//      if (break < 500) {
//        try {
//          println(key)
//          val tileR = tileForKey(key, layoutTileSourceVV)
//          val tileG = tileForKey(key, layoutTileSourceVH)
//          val tileB = tileR.combineDouble(tileG)((a, b) => a / b)
//          
//          val normTileR = logTile(tileR).normalize(-25, 3, 0, 255)
//          val normTileG = logTile(tileG).normalize(-30, -2, 0, 255)
//          val normTileB = tileB.normalize(0.2, 1, 0, 255)
//          
//          val arrayTile = ArrayMultibandTile(normTileR, normTileG, normTileB)
//          val png: Png = arrayTile.renderPng()
//          val path = "/home/niels/Data/PngTest-4/"
//          createDirs(path)
//          png.write(path + s"$break.png")
//        } catch {
//          case ex: IllegalArgumentException => println(ex.getMessage)
//        }
//      }
//      break += 1
//    }

    implicit val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[4]").setAppName("Geotiffloading")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","1024m"))

    var rdd = loadArrayMultibandTiles(Seq(sourceVV), Seq(sourceVH), localLayoutDefWithZoom._1)
    
    rdd.zipWithIndex().foreach(x => {
      val (input, i) = x //TODO deconstruct in lambda
      val (_, tile) = input
      val png = tile.renderPng()
      val path = "/home/niels/Data/PngTest-RDD/"
      createDirs(path)
      png.write(path + s"$i.png")
    })
    
    //    var rdd = loadRasterRegions(Seq(source),localLayoutDefWithZoom._1)
//    //rdd = ContextRDD(rdd.repartitionAndSortWithinPartitions(SpatialPartitioner(100)),rdd.metadata)
//    //rdd.spatiallyPartition().filterByKeyBounds()
//    println(rdd.partitioner)
//    //val histogram = rdd.histogram()
//    val start = System.currentTimeMillis()
//    println(rdd.count())
//    println("duration: " + (System.currentTimeMillis() - start) / 1000.0)
//    val entries = rdd.take(5)
//
//    println(entries)

  }
  
  def tileForKey(key: SpatialKey, layoutTileSource: LayoutTileSource): Tile = {
    layoutTileSource.rasterRegionForKey(key).raster.get.tile.band(0)
  }
  
  def logTile(tile: Tile): Tile = {
    tile.mapDouble(d => 10 * log10(d))
  }
  
  def createDirs(str: String) {
    val file = new File(str)
    if (!file.exists)
      file.mkdirs()
  }
  
  def loadArrayMultibandTiles(sourcesVV: Seq[RasterSource],
                              sourcesVH: Seq[RasterSource],
                              layout: LayoutDefinition,
                              partitionBytes: Long = PARTITION_BYTES)
                             (implicit sc: SparkContext): RDD[(SpaceTimeKey, ArrayMultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
   
    val allSources = sourcesVV ++ sourcesVH
    val cellTypes = allSources.map { _.cellType }.toSet
    require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = allSources.map { _.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = allSources.map { _.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val spacetimeBounds = KeyBounds(SpaceTimeKey(layerKeyBounds._1,ZonedDateTime.now()),SpaceTimeKey(layerKeyBounds._2,ZonedDateTime.now()))

    val layerMetadata =
      TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, spacetimeBounds)
    
    val sourcesVVRDD: RDD[(SpaceTimeKey, Tile)] = tileRDDFromSources(sourcesVV, layout)
    val sourcesVHRDD: RDD[(SpaceTimeKey, Tile)] = tileRDDFromSources(sourcesVH, layout)
      
    val sourcesRDD: RDD[(SpaceTimeKey, (Tile, Tile))] = sourcesVVRDD.join(sourcesVHRDD)
    
    val resultRDD: RDD[(SpaceTimeKey, ArrayMultibandTile)] =
      sourcesRDD.map(input => {
        val (key, (tileR, tileG)) = input //TODO how to deconstruct in lambda
        val tileB = tileR.combineDouble(tileG)((a, b) => a / b)

        val normTileR = logTile(tileR).normalize(-25, 3, 0, 255)
        val normTileG = logTile(tileG).normalize(-30, -2, 0, 255)
        val normTileB = tileB.normalize(0.2, 1, 0, 255)

        val arrayTile = ArrayMultibandTile(normTileR, normTileG, normTileB)
        
        (key, arrayTile)
      })
    
    ContextRDD(resultRDD, layerMetadata)
  }
  
  def tileRDDFromSources(sources: Seq[RasterSource], layout: LayoutDefinition)(implicit sc: SparkContext): RDD[(SpaceTimeKey, Tile)] = {
    sc.parallelize(sources).flatMap { source =>
      val date = LocalDate.from(DateTimeFormatter.BASIC_ISO_DATE.parse(source.uri.split("_DV_")(1).substring(0,8)))
      val tileSource = new LayoutTileSource(source, layout)
      tileSource.keys().take(2000).map { key => //TODO partition to avoid out of memory
        val tile = tileForKey(key, tileSource)
        (SpaceTimeKey(key, date.atStartOfDay(ZoneId.of("UTC"))), tile)
      }
    }
  }
  
  def loadRasterRegions(
             sources: Seq[RasterSource],
             layout: LayoutDefinition,
             partitionBytes: Long = PARTITION_BYTES
           )(implicit sc: SparkContext): RDD[(SpaceTimeKey, RasterRegion)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    val cellTypes = sources.map { _.cellType }.toSet
    //require(cellTypes.size == 1, s"All RasterSources must have the same CellType, but multiple ones were found: $cellTypes")

    val projections = sources.map { _.crs }.toSet
    require(
      projections.size == 1,
      s"All RasterSources must be in the same projection, but multiple ones were found: $projections"
    )

    val cellType = cellTypes.head
    val crs = projections.head

    val mapTransform = layout.mapTransform
    val extent = mapTransform.extent
    val combinedExtents = sources.map { _.extent }.reduce { _ combine _ }

    val layerKeyBounds = KeyBounds(mapTransform(combinedExtents))
    val spacetimeBounds = KeyBounds(SpaceTimeKey(layerKeyBounds._1,ZonedDateTime.now()),SpaceTimeKey(layerKeyBounds._2,ZonedDateTime.now()))

    val layerMetadata =
      TileLayerMetadata[SpaceTimeKey](cellType, layout, combinedExtents, crs, spacetimeBounds)

    val sourcesRDD: RDD[(RasterSource, Array[SpatialKey])] =
      sc.parallelize(sources).flatMap { source =>
        val keys: Traversable[SpatialKey] =
          extent.intersection(source.extent) match {
            case Some(intersection) =>
              layout.mapTransform.keysForGeometry(intersection.toPolygon)
            case None =>
              Seq.empty[SpatialKey]
          }
        val tileSize = layout.tileCols * layout.tileRows * cellType.bytes
        partition(keys, partitionBytes)( _ => tileSize).map { res => (source, res) }
      }

    sourcesRDD.persist()

    val repartitioned = {
      val count = sourcesRDD.count.toInt
      if (count > sourcesRDD.partitions.size)
        sourcesRDD//.repartition(count)
      else
        sourcesRDD
    }

    val result: RDD[(SpaceTimeKey, RasterRegion)] =
      repartitioned.flatMap { case (source, keys) =>
        val date = LocalDate.from(DateTimeFormatter.BASIC_ISO_DATE.parse(source.uri.split("_DV_")(1).substring(0,8)))
        val tileSource = new LayoutTileSource(source, layout)
        tileSource.keyedRasterRegions().map(key => (SpaceTimeKey(key._1,date.atStartOfDay(ZoneId.of("UTC"))),key._2))
      }

    sourcesRDD.unpersist()

    ContextRDD(result, layerMetadata)
  }

  def partition[T: ClassTag](
                              chunks: Traversable[T],
                              maxPartitionSize: Long
                            )(chunkSize: T => Long = { c: T => 1l }): Array[Array[T]] = {
    if (chunks.isEmpty) {
      Array[Array[T]]()
    } else {
      val partition = ArrayBuilder.make[T]
      partition.sizeHintBounded(128, chunks)
      var partitionSize: Long = 0l
      var partitionCount: Long = 0l
      val partitions = ArrayBuilder.make[Array[T]]

      def finalizePartition() {
        val res = partition.result
        if (res.nonEmpty) partitions += res
        partition.clear()
        partitionSize = 0l
        partitionCount = 0l
      }

      def addToPartition(chunk: T) {
        partition += chunk
        partitionSize += chunkSize(chunk)
        partitionCount += 1
      }

      for (chunk <- chunks) {
        if ((partitionCount == 0) || (partitionSize + chunkSize(chunk)) < maxPartitionSize)
          addToPartition(chunk)
        else {
          finalizePartition()
          addToPartition(chunk)
        }
      }

      finalizePartition()
      partitions.result
    }
  }

  def main(args: Array[String]): Unit = {

    LoadSigma0.test()
  }

}
