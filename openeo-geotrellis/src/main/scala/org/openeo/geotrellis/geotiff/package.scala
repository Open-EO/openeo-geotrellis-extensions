package org.openeo.geotrellis

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.layer._
import geotrellis.proj4.CRS
import geotrellis.raster
import geotrellis.raster.crop.Crop.Options
import geotrellis.raster.crop._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression.{Compression, Compressor, DeflateCompression, Predictor, ZStdCompression}
import geotrellis.raster.io.geotiff.tags.codes.ColorSpace
import geotrellis.raster.render.IndexedColorMap
import geotrellis.raster.resample._
import geotrellis.raster.{ArrayTile, CellSize, CellType, GridBounds, GridExtent, MultibandTile, Raster, RasterExtent, Tile, TileLayout, UByteConstantTile, ubyteNODATA}
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.util._
import geotrellis.vector.{ProjectedExtent, _}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Partitioner, SparkContext, TaskContext}
import org.openeo.geotrellis
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.netcdf.NetCDFRDDWriter.fixedTimeOffset
import org.openeo.geotrellis.stac.{Asset, Item, STACItem}
import org.openeo.geotrellis.tile_grid.TileGrid
import org.openeo.geotrelliscommon.ByKeyPartitioner
import org.slf4j.LoggerFactory
import spire.math.Integral
import spire.syntax.cfor.cfor

import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import java.time.format.DateTimeFormatter
import java.util
import java.util.stream.Collectors
import java.util.{ArrayList, Collections, Map, UUID, List => JList}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.reflect._


class ByKeyPartitionerKnowKeyAmount(numPartitionsArgument: Int) extends Partitioner {
  override def numPartitions: Int = numPartitionsArgument

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[(String, Int)]._2
  }
}

package object geotiff {

  private val logger = LoggerFactory.getLogger(getClass)
  private val secondsPerDay = 86400L
  private val gdalProjLib = Option(System.getenv("OPENEO_GDAL_PROJ_LIB")).getOrElse("/usr/share/proj")

  class MultiBandGeoTiffWithCompression(val t: MultibandGeoTiff) {

    def withCompression(options: GTiffOptions): MultibandGeoTiff = {
      var compression: Compression = {
        options.compressionMethod match {
          case "zstd" => ZStdCompression(options.compressionLevel)
          case "deflate" => DeflateCompression(options.compressionLevel)
          case _ => throw new IllegalArgumentException(f"Compression method ${options.compressionMethod} is not supported, supported methods are: (zstd, deflate (default))")
        }
      }
      if (options.compressionPredictor > 1) {
        compression = compression.withPredictor(Predictor(t.tile.toGeoTiffTile()))
        MultibandGeoTiff(t.tile.toArrayTile(), t.extent, t.crs, t.tags, GeoTiffOptions(compression), t.overviews)
      } else (
        t
        )
    }
  }

  implicit def toMultiBandGeoTiffWithCompression(t: MultibandGeoTiff): MultiBandGeoTiffWithCompression = new MultiBandGeoTiffWithCompression(t)

  class SetAccumulator[T](var value: Set[T]) extends AccumulatorV2[T, Set[T]] {
    def this() = this(Set.empty[T])

    override def isZero: Boolean = value.isEmpty

    override def copy(): AccumulatorV2[T, Set[T]] = new SetAccumulator[T](value)

    override def reset(): Unit = value = Set.empty[T]

    override def add(v: T): Unit = value = value + v

    override def merge(other: AccumulatorV2[T, Set[T]]): Unit = value = value ++ other.value
  }


  type SRDD = RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]

  private def toExtent(extent: Map[String, Double]) = Extent(
    extent.get("xmin"),
    extent.get("ymin"),
    extent.get("xmax"),
    extent.get("ymax")
  )

  // ~ SpatialTiledRasterLayer in GeoPySpark but supports compression
  def saveStitched(rdd: SRDD, path: String, compression: Compression): Item =
    saveStitched(rdd, path, None, None, compression)

  def saveStitched(rdd: SRDD, path: String, cropBounds: Map[String, Double], compression: Compression): Item =
    saveStitched(rdd, path, Some(cropBounds), None, compression)

  def saveStitched(rdd: SRDD, path: String, compression: Compression, formatOptions: GTiffOptions): Item =
    saveStitched(rdd, path, None, None, compression, Some(formatOptions))

  def saveStitched(rdd: SRDD, path: String, cropBound: Map[String, Double], compression: Compression, formatOptions: GTiffOptions): Item =
    saveStitched(rdd, path, Some(cropBound), None, compression, Some(formatOptions))

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, compression: Compression): JList[Item] =
    saveStitchedTileGrid(rdd, path, tileGrid, None, None, compression)

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, cropBounds: Map[String, Double], compression: Compression): JList[Item] =
    saveStitchedTileGrid(rdd, path, tileGrid, Some(cropBounds), None, compression)

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, compression: Compression, formatOptions: GTiffOptions): JList[Item] =
    saveStitchedTileGrid(rdd, path, tileGrid, None, None, compression, Some(formatOptions))

  def saveStitchedTileGrid(rdd: SRDD, path: String, tileGrid: String, cropBounds: Map[String, Double], compression: Compression, formatOptions: GTiffOptions): JList[Item] =
    saveStitchedTileGrid(rdd, path, tileGrid, Some(cropBounds), None, compression, Some(formatOptions))

  def saveRDDTiled(rdd: MultibandTileLayerRDD[SpaceTimeKey], path: String, zLevel: Int = 6, cropBounds: Option[Extent] = Option.empty[Extent]): Unit = {
    val layout = rdd.metadata.layout
    rdd.foreach(key_t => {
      val filename = s"X${key_t._1.col}Y${key_t._1.col}_${DateTimeFormatter.ISO_DATE.format(key_t._1.time)}.tif"
      GeoTiff(key_t._2, key_t._1.spatialKey.extent(layout), rdd.metadata.crs).write(path + filename, true)
    })
  }

  def saveRDDTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                      path: String,
                      zLevel: Int = 6,
                      cropBounds: Option[Extent] = Option.empty[Extent],
                      formatOptions: GTiffOptions = new GTiffOptions): JList[(String, String, Extent)] = {
    saveRDDTemporalInternal(rdd, path, zLevel, cropBounds, formatOptions)
  }

  private[geotiff] def saveRDDTemporalInternal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                               path: String,
                                               zLevel: Int = 6,
                                               cropBounds: Option[Extent] = Option.empty[Extent],
                                               formatOptions: GTiffOptions = new GTiffOptions,
                                               overviewReductions: (GTiffOptions, Int, Int, Int, Int) => List[Int] = defaultOverviewReductions
                                              ): JList[(String, String, Extent)] = {
    rdd.sparkContext.setCallSite(s"save_result(GTiff, temporal)")
    formatOptions.assertNoConflicts()
    val ret = saveRDDTemporalAllowAssetPerBandInternal(rdd, path, zLevel, cropBounds, formatOptions, overviewReductionsFunction = overviewReductions)
    logger.warn("Calling backwards compatibility version for saveRDDTemporalConsiderAssetPerBand")
    ret.stream()
      .flatMap { item =>
        item.assets.values().stream()
          .map[(String, String, Extent)] { asset => (asset.path, item.datetime, item.bbox) }
      }
      .collect(Collectors.toList())
  }

  private val executorAttemptDirectoryPrefix = "executorAttemptDirectory"

  private def createExecutorAttemptDirectory(parentDirectory: String): Path = {
    createExecutorAttemptDirectory(Path.of(parentDirectory))
  }

  private def createExecutorAttemptDirectory(parentDirectory: Path): Path = {
    // Multiple executors with the same task can run at the same time.
    // Writing their output to the same path would create a racing condition.
    // Let's provide a unique directory for each executor:
    val rand = new java.security.SecureRandom().nextLong()
    val uniqueFolderName = executorAttemptDirectoryPrefix + java.lang.Long.toUnsignedString(rand)
    val executorAttemptDirectory = Paths.get(parentDirectory + "/" + uniqueFolderName)
    if (!CreoS3Utils.isS3(parentDirectory.toString)) {
      Files.createDirectories(executorAttemptDirectory)
    }
    executorAttemptDirectory
  }

  private def updateGdalInfoJsonFile(jsonFilePath: String, tiffFilePath: String): Unit = {
    val str = CreoS3Utils.readFileAsString(jsonFilePath)
    val mapper = new ObjectMapper()
    val node = mapper.readTree(str)
    if (!node.isInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode])
      throw new Exception(s"not able to update gdal info file. Expected ObjectNode, but got ${node.getClass}.")
    node.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].put("description", tiffFilePath)
    val filesNode = node.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].putArray("files")
    filesNode.add(tiffFilePath)

    val strAgain = mapper.writeValueAsString(node)

    CreoS3Utils.writeStringToFile(jsonFilePath, strAgain)
  }

  private def extractExecutorAttemptDirectory(parentDirectory: Path, geoTiffResultObject: GeoTiffResultObject): String = {
    val relativeFilePath = parentDirectory.relativize(Path.of(geoTiffResultObject.correctPath)).toString
    if (!relativeFilePath.startsWith(executorAttemptDirectoryPrefix)) throw new Exception("Bad relativeFilePath:" + relativeFilePath)
    parentDirectory + "/" + relativeFilePath.substring(0, relativeFilePath.indexOf("/"))
  }

  private def moveFromExecutorAttemptDirectory(parentDirectory: Path, geoTiffResultObject: GeoTiffResultObject): String = {
    // Move output file to standard location. (On S3, a move is more a copy and delete):
    val relativeFilePath = parentDirectory.relativize(Path.of(geoTiffResultObject.correctPath)).toString
    val destinationPathCleaned = if (relativeFilePath.startsWith(executorAttemptDirectoryPrefix)) {
      // Remove the executorAttemptDirectory part from the path:
      val destinationPath = parentDirectory.resolve(relativeFilePath.substring(relativeFilePath.indexOf("/") + 1))
      if (geoTiffResultObject.fileExists) {
        CreoS3Utils.waitTillPathAvailable(geoTiffResultObject.correctPath)
        if (!CreoS3Utils.isS3(parentDirectory.toString)) {
          Files.createDirectories(destinationPath.getParent)
        }
        CreoS3Utils.moveOverwriteWithRetries(geoTiffResultObject.correctPath, destinationPath.toString)
      }

      geoTiffResultObject.gdalInfoPath match {
        case Some(gdalInfoPath) =>
          CreoS3Utils.waitTillPathAvailable(gdalInfoPath)
          updateGdalInfoJsonFile(gdalInfoPath, destinationPath.toString)
          val gdalInfoDestinationPath = gdalInfoPath.replaceFirst(executorAttemptDirectoryPrefix + "\\d+/", "")
          CreoS3Utils.moveOverwriteWithRetries(gdalInfoPath, gdalInfoDestinationPath)
        case None => // do nothing
      }
      destinationPath
    } else parentDirectory.resolve(relativeFilePath)
    if (CreoS3Utils.isS3(destinationPathCleaned.toString)) {
      destinationPathCleaned.toString.replaceFirst("s3:/(?!/)", "s3://")
    } else {
      destinationPathCleaned.toString
    }
  }

  private def defaultOverviewReductions(options: GTiffOptions, totalCols: Int, totalRows: Int, tileCols: Int, tileRows: Int): List[Int] = {
    options.overviews.toUpperCase() match {
      case "AUTO" =>
        val overviewLevels: Int = {
          val pixels = math.max(totalCols, totalRows).toDouble
          val blocks = pixels / 1024
          math.ceil(math.log(blocks) / math.log(2)).toInt
        }

        val start = options.overviews.toUpperCase() match {
          case "AUTO" => 1
          case "ALL" => 0
        }
        (start until overviewLevels).map { l => math.pow(2, l + 1).toInt }.toList.filter(
          r => (tileCols / r) > 16 && (tileRows / r) > 16
        )
      case "ALL" =>
        val overviewLevels: Int = {
          val pixels = math.max(totalCols, totalRows).toDouble
          val blocks = pixels / 256
          math.ceil(math.log(blocks) / math.log(2)).toInt
        }

        val start = options.overviews.toUpperCase() match {
          case "AUTO" => 1
          case "ALL" => 0
        }
        (start until overviewLevels).map { l => math.pow(2, l + 1).toInt }.toList.filter(
          r => (tileCols / r) > 16 && (tileRows / r) > 16
        )
      case _ => List.empty
    }
  }

  /**
   * Save temporal rdd, on the executors
   *
   * @param rdd
   * @param path
   * @param zLevel
   * @param cropBounds
   * @param formatOptions
   */
  //noinspection ScalaWeakerAccess
  def saveRDDTemporalAllowAssetPerBand(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                       path: String,
                                       zLevel: Int = 6,
                                       cropBounds: Option[Extent] = Option.empty[Extent],
                                       formatOptions: GTiffOptions = new GTiffOptions,
                                      ): JList[Item] = {
    saveRDDTemporalAllowAssetPerBandInternal(rdd, path, zLevel, cropBounds, formatOptions, overviewReductionsFunction = defaultOverviewReductions)
  }

  /**
   * Save temporal rdd, on the executors
   *
   * @param rdd
   * @param path
   * @param zLevel
   * @param cropBounds
   * @param formatOptions
   */
  //noinspection ScalaWeakerAccess
  private[geotiff] def saveRDDTemporalAllowAssetPerBandInternal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                                                path: String,
                                                                zLevel: Int = 6,
                                                                cropBounds: Option[Extent] = Option.empty[Extent],
                                                                formatOptions: GTiffOptions = new GTiffOptions,
                                                                overviewReductionsFunction: (GTiffOptions, Int, Int, Int, Int) => List[Int] = defaultOverviewReductions,
                                                               ): JList[Item] = {
    formatOptions.assertNoConflicts()
    val preProcessResult: (GridBounds[Int], Extent, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) = preProcess(rdd, cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = preProcessResult._3

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
    val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

    logger.info(s"Write Geotiff per date ${croppedExtent}, ${gridBounds}, ${tileLayout}")

    val compression = determineCompression(formatOptions)
    val bandSegmentCount = totalCols * totalRows
    val bandLabels = formatOptions.tags.bandTags.map(_("DESCRIPTION"))


    val toBeGrouped = preprocessedRdd.flatMap { case (key: SpaceTimeKey, multibandTile: MultibandTile) =>
      var bandIndex = -1
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val theCompressor: Compressor = compression.createCompressor(multibandTile.bandCount)
      multibandTile.bands.map {
        tile =>
          bandIndex += 1
          val layoutCol = key.getComponent[SpatialKey]._1
          val layoutRow = key.getComponent[SpatialKey]._2
          val bandSegmentOffset = bandSegmentCount * (if (formatOptions.separateAssetPerBand) 0 else bandIndex)
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
          //tiff format seems to require that we provide 'full' tiles
          val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
          val compressedBytes = theCompressor.compress(bytes, 0)

          val isDays = Duration.between(fixedTimeOffset, key.time).getSeconds % secondsPerDay == 0
          val timePieceSlug = if (isDays) {
            DateTimeFormatter.ISO_DATE.format(key.time)
          } else {
            // ':' is not valid in a Windows filename
            DateTimeFormatter.ISO_ZONED_DATE_TIME.format(key.time).replace(":", "").replace("-", "")
          }
          val overviews = generateOverviews(formatOptions, croppedExtent, tileLayout, tile, theCompressor, overviewReductionsFunction(formatOptions, gridBounds.width, gridBounds.height, tileLayout.tileCols, tileLayout.tileRows))

          val bandPiece = if (formatOptions.separateAssetPerBand) "_" + bandLabels(bandIndex) else ""
          val filename = formatOptions.filepathPerBand match {
            case Some(filepathPerBand) => filepathPerBand.get(bandIndex).replace("<date>", timePieceSlug)
            case None => s"${formatOptions.filenamePrefix}_${timePieceSlug}${bandPiece}.tif"
          }
          val timestamp = DateTimeFormatter.ISO_ZONED_DATE_TIME.format(key.time)
          val tiffBands = if (formatOptions.separateAssetPerBand) 1 else multibandTile.bandCount
          ((filename, timestamp, tiffBands), (index, (multibandTile.cellType, compressedBytes, overviews), bandIndex))
      }
    }.persist()

    val keys = toBeGrouped.map(_._1).distinct().collect() // TODO: Extent metadata to be able to get keys
    val geotiffResults = toBeGrouped.groupByKey(new ByKeyPartitioner(keys)).map { case ((filename: String, timestamp: String, tiffBands: Int), sequence) =>
      val cellTypes = sequence.map(_._2._1).toSet
      val tiffs: Predef.Map[Int, Array[Byte]] = sequence.map(tuple => (tuple._1, tuple._2._2)).toMap
      val bandIndices = sequence.map(_._3).toSet.toList.asJava

      val segmentCount = bandSegmentCount * tiffBands

      val overviewTiles = if (formatOptions.overviews.toUpperCase == "ALL" || formatOptions.overviews.toUpperCase == "AUTO") {
        logger.info(s"Add overviews for ${filename}, with resample method ${getOverviewResampleMethod(formatOptions)}")
        val decimationFactors = overviewReductionsFunction(formatOptions, gridBounds.width, gridBounds.height, tileLayout.tileRows, tileLayout.tileCols)

        decimationFactors.indices.toList
          .map(i => {
            val decimationFactor = decimationFactors(i)
            val overviewSequence: Predef.Map[Int, Array[Byte]] = sequence.map(tuple => (tuple._1, tuple._2._3(i))).toMap
            val overviewLayout = TileLayout(tileLayout.layoutCols, tileLayout.layoutRows, tileLayout.tileCols / decimationFactor, tileLayout.tileRows / decimationFactor)
            toTiff(overviewSequence, GridBounds(0, 0, gridBounds.colMax / decimationFactor, gridBounds.rowMax / decimationFactor), overviewLayout, compression, cellTypes.head, tiffBands, segmentCount)
          })

      } else Nil
      // Each executor writes to a unique folder to avoid conflicts:
      val executorAttemptDirectory = createExecutorAttemptDirectory(path)
      val absoluteFilePath = if (TaskContext.get().attemptNumber() > 0) {
        // Each executor writes to a unique folder to avoid conflicts:
        val executorAttemptDirectory = createExecutorAttemptDirectory(path)
        executorAttemptDirectory.resolve(filename)
      } else {
        Path.of(path).resolve(filename)
      }
      absoluteFilePath.toFile.getParentFile.mkdirs()
      val thePath = absoluteFilePath.toString

      // filter band tags that match bandIndices
      val fo = formatOptions.deepClone()
      val newBandTags = formatOptions.tags.bandTags.zipWithIndex
        .filter { case (_, bandIndex) => bandIndices.contains(bandIndex) }
        .map { case (bandTags, _) => bandTags }
      fo.setBandTags(newBandTags)

      val geoTiffResultObject = writeTiff(thePath, tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs,
        tileLayout, compression, cellTypes.head, tiffBands, segmentCount, fo, overviewTiles
      )
      val assetMetadata = setupAssetMetadata(bandLabels, preProcessResult._2, preprocessedRdd.metadata.crs, Array(gridBounds.height, gridBounds.width))
      (geoTiffResultObject, timestamp, croppedExtent, bandIndices, assetMetadata)
    }.collect()
    val res = geotiffResults.map {
      case (geoTiffResultObject, timestamp, croppedExtent, bandIndices, assetMetadata) =>
        val destinationPath = moveFromExecutorAttemptDirectory(Path.of(path), geoTiffResultObject)
        (destinationPath, timestamp, croppedExtent, bandIndices, assetMetadata)
    }

    val items = res
      .groupBy { case (_, timestamp, _, _, _) => timestamp }
      .map { case (timestamp, geotiffs) =>
        val assets = geotiffs
          .map { case (path, _, _, bandIndices, assetMetadata) =>
            val assetKey = if (formatOptions.separateAssetPerBand) f"${bandLabels(bandIndices.get(0))}" else "openEO"
            assetKey -> Asset(path, bandIndices, metadata= assetMetadata)
          }
          .toMap

        Item(id = s"${UUID.randomUUID()}_$timestamp", datetime = timestamp, bbox = croppedExtent, assets.asJava)
      }

    cleanupTemporaryResults(geotiffResults.map(_._1), path)

    toBeGrouped.unpersist()

    items.toList.asJava
  }

  private def generateOverviews(formatOptions: GTiffOptions, croppedExtent: Extent, tileLayout: TileLayout, tile: Tile, compressor: Compressor, overviewReductions: List[Int]): List[Array[Byte]] = {
    var overviewBytes = List[Array[Byte]]()

    if (formatOptions.overviews == "OFF" || overviewReductions.isEmpty) {
      // do nothing
    } else {
      val resampleMethod = getOverviewResampleMethod(formatOptions)
      var previousTile = tile
      var reductionFactor = 2
      if (formatOptions.overviews == "AUTO") {
        // skip the first overview level for AUTO
        previousTile = tile.resample(croppedExtent, tileLayout.tileCols / 2, tileLayout.tileRows / 2, resampleMethod)
        reductionFactor *= 2
      }
      while (overviewReductions.last >= reductionFactor) {
        val resampledTile = previousTile.resample(croppedExtent, tileLayout.tileCols / reductionFactor, tileLayout.tileRows / reductionFactor, resampleMethod)
        if (overviewReductions.contains(reductionFactor)) {
          overviewBytes = overviewBytes :+ {
            val croppedBytes = raster.CroppedTile(resampledTile, raster.GridBounds(0, 0, tileLayout.tileCols / reductionFactor - 1, tileLayout.tileRows / reductionFactor - 1)).toBytes()
            compressor.compress(croppedBytes, 0)
          }
        }
        previousTile = resampledTile
        reductionFactor *= 2
      }
    }
    overviewBytes
  }

  def saveRDD(rdd: MultibandTileLayerRDD[SpatialKey],
              bandCount: Int,
              path: String,
              zLevel: Int = 6,
              cropBounds: Option[Extent] = Option.empty[Extent],
              formatOptions: GTiffOptions = new GTiffOptions
             ): JList[String] = {
    rdd.sparkContext.setCallSite(s"save_result(GTiff, spatial, $bandCount)")
    val tmp = saveRDDAllowAssetPerBand(rdd, bandCount, path, zLevel, cropBounds, formatOptions)
    logger.warn("Calling backwards compatibility version for saveRDDAllowAssetPerBand")
    //    if (tmp.size() > 1) {
    //      throw new Exception("Multiple returned files, probably meant to call saveRDDAllowAssetPerBand")
    //    }
    tmp.stream()
      .flatMap { item =>
        item.assets.values().stream()
          .map[String] { asset => asset.path }
      }
      .collect(Collectors.toList())
  }

  //noinspection ScalaWeakerAccess
  def saveRDDAllowAssetPerBand(rdd: MultibandTileLayerRDD[SpatialKey],
                               bandCount: Int,
                               path: String,
                               zLevel: Int = 6,
                               cropBounds: Option[Extent] = Option.empty[Extent],
                               formatOptions: GTiffOptions = new GTiffOptions
                              ): JList[Item] = {
    formatOptions.assertNoConflicts()
    if (formatOptions.separateAssetPerBand) {
      val bandLabels = formatOptions.tags.bandTags.map(_("DESCRIPTION"))
      val layout = rdd.metadata.layout
      val crs = rdd.metadata.crs
      val extent = rdd.metadata.extent
      val compression = determineCompression(formatOptions)

      val rdd_per_band = rdd.flatMap { case (key: SpatialKey, multibandTile: MultibandTile) =>
        var bandIndex = -1
        multibandTile.bands.map {
          tile =>
            bandIndex += 1
            val t = _root_.geotrellis.raster.MultibandTile(Seq(tile))
            val name = formatOptions.filepathPerBand match {
              case Some(filepathPerBand) => filepathPerBand.get(bandIndex)
              case None => formatOptions.filenamePrefix + "_" + bandLabels(bandIndex) + ".tif"
            }
            ((name, bandIndex), (key, t))
        }
      }
      val partitioner = new ByKeyPartitionerKnowKeyAmount(bandLabels.length) {
        override def getPartition(key: Any): Int = {
          key.asInstanceOf[(String, Int)]._2
        }
      }
      // groupByKey does a shuffle, so we can partition at the same time
      val geotiffResults = rdd_per_band.groupByKey(partitioner).map { case ((name, bandIndex), tiles) =>
        val fixedPath =
          if (path.endsWith("out")) {
            if (TaskContext.get().attemptNumber() > 0) {
              val executorAttemptDirectory = createExecutorAttemptDirectory(path.substring(0, path.length - 3))
              executorAttemptDirectory + "/" + name
            } else {
              path.substring(0, path.length - 3) + "/" + name
            }
          }
          else {
            path
          }
        Path.of(fixedPath).toFile.getParentFile.mkdirs()
        val fo = formatOptions.deepClone()
        // Keep only one band tag
        val newBandTags = List(formatOptions.tags.bandTags(bandIndex))
        fo.setBandTags(newBandTags)
        if (formatOptions.filepathPerBand.isDefined) {
          fo.setFilepathPerBand(Some(new ArrayList[String](Collections.singletonList(
            formatOptions.filepathPerBand.get.get(bandIndex)
          ))))
        }
        val assetMetadata = setupAssetMetadata(List(name),extent,crs,Array(layout.rows.toInt,layout.cols.toInt))

        (stitchAndWriteToTiff(tiles, fixedPath, layout, crs, extent, Some(extent), None, compression, Some(fo)),
          Collections.singletonList(bandIndex),assetMetadata)
      }.collect()
      val res = geotiffResults.map {
        case (geoTiffResultObject, bandIndices, assetMetadata) =>
          if (path.endsWith("out")) {
            val beforeOut = path.substring(0, path.length - "out".length)
            val destinationPath = moveFromExecutorAttemptDirectory(Path.of(beforeOut), geoTiffResultObject)
            (destinationPath, extent, bandIndices, assetMetadata)
          } else {
            (geoTiffResultObject.correctPath, extent, bandIndices, assetMetadata)
          }
      }

      if (path.endsWith("out")) {
        val beforeOut = path.substring(0, path.length - "out".length)
        cleanupTemporaryResults(geotiffResults.map(_._1), beforeOut)
      }

      val assets = res.map { case (path, _, bandIndices, assetMetadata) =>
        val bandNames = bandIndices.asScala.map(bandLabels.apply)
        s"${bandNames mkString "_"}" -> Asset(path, bandIndices, assetMetadata)
      }.toMap.asJava

      Collections.singletonList(Item(id = UUID.randomUUID().toString, datetime = null, bbox = extent, assets))
      // TODO: restore asset ordering?
    } else {
      val (tiffPath, extent, assetMetadata) = saveRDDGeneric(rdd, bandCount, path, zLevel, cropBounds, formatOptions)
      val assets = Collections.singletonMap("openEO", Asset(tiffPath, (0 until bandCount).asJava, metadata = assetMetadata))

      Collections.singletonList(Item(id = UUID.randomUUID().toString, datetime = null, bbox = extent, assets))
    }
  }

  private def cleanupTemporaryResults(geotiffResults: Array[GeoTiffResultObject], outputDirectory: String): Unit = {
    for (geotiffResult <- geotiffResults) {
      val outputDirectoryPath = Path.of(outputDirectory)
      val relativeFilePath = outputDirectoryPath.relativize(Path.of(geotiffResult.correctPath)).toString
      if (relativeFilePath.startsWith(executorAttemptDirectoryPrefix)) {
        val successfulExecutorAttemptDirectory = extractExecutorAttemptDirectory(outputDirectoryPath, geotiffResult)
        CreoS3Utils.assetDeleteFolders(List(successfulExecutorAttemptDirectory))
      }
    }
  }

  def saveRDDTileGrid(rdd: MultibandTileLayerRDD[SpatialKey], bandCount: Int, path: String, tileGrid: String, zLevel: Int = 6, cropBounds: Option[Extent] = Option.empty[Extent]) = {
    saveRDDGenericTileGrid(rdd, path, tileGrid, cropBounds = cropBounds)
  }

  private def gridBoundsFor(re: RasterExtent, subExtent: Extent, clamp: Boolean = true): GridBounds[Int] = {
    // West and North boundaries are a simple mapToGrid call.
    val colMin: Int = re.mapXToGrid(subExtent.xmin)
    val rowMin: Int = re.mapYToGrid(subExtent.ymax)

    // If South East corner is on grid border lines, we want to still only include
    // what is to the West and\or North of the point. However if the border point
    // is not directly on a grid division, include the whole row and/or column that
    // contains the point.
    val colMax: Long = Integral[Long].fromLong {
      val colMaxDouble = re.mapXToGridDouble(subExtent.xmax)

      if (math.abs(colMaxDouble - GridExtent.floorWithTolerance(colMaxDouble)) < GridExtent.epsilon)
        GridExtent.floorWithTolerance(colMaxDouble).toLong - 1L
      else
        GridExtent.floorWithTolerance(colMaxDouble).toLong
    }

    val rowMax: Long = Integral[Long].fromLong {
      val rowMaxDouble = re.mapYToGridDouble(subExtent.ymin)

      if (math.abs(rowMaxDouble - GridExtent.floorWithTolerance(rowMaxDouble)) < GridExtent.epsilon)
        GridExtent.floorWithTolerance(rowMaxDouble).toLong - 1L
      else
        GridExtent.floorWithTolerance(rowMaxDouble).toLong
    }

    if (clamp)
      GridBounds(
        colMin = colMin.max(0).min(re.cols - 1).intValue(),
        rowMin = rowMin.max(0).min(re.rows - 1).intValue(),
        colMax = colMax.max(0).min(re.cols - 1).intValue(),
        rowMax = rowMax.max(0).min(re.rows - 1).intValue())
    else
      GridBounds[Int](colMin, rowMin, colMax.toInt, rowMax.toInt)
  }

  def preProcess[K: SpatialComponent : Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], cropBounds: Option[Extent]): (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = {
    val re = rdd.metadata.toRasterExtent()
    /**
     * CLAMPING EP-4150
     * Gridbounds are clamped to the actually available rasterextent, this means that we don't add empty data if somehow a cropping bounds is provided that is larger than the actual datacube.
     * CroppedExtent needs to match exactly with whatever gridbounds that we got, so there we do not clamp. So even though we clamp when computing gridbounds, it can still have an extent that is larger than rasterextent!!!
     */
    var gridBounds = gridBoundsFor(re, cropBounds.getOrElse(rdd.metadata.extent), clamp = true)
    val croppedExtent = re.extentFor(gridBounds, clamp = false)
    val filtered = new OpenEOProcesses().filterEmptyTile(rdd)
    val preprocessedRdd = {
      if (gridBounds.colMin != 0 || gridBounds.rowMin != 0) {
        logger.info(s"Gridbounds requires reprojection: ${gridBounds}")
        val geotiffLayout: LayoutDefinition = LayoutDefinition(RasterExtent(croppedExtent, re.cellSize), rdd.metadata.tileCols, rdd.metadata.tileRows)
        val retiledRDD = filtered.reproject(rdd.metadata.crs, geotiffLayout)._2.crop(croppedExtent, Options(force = false))

        gridBounds = gridBoundsFor(retiledRDD.metadata.toRasterExtent(), cropBounds.getOrElse(retiledRDD.metadata.extent), clamp = true)
        retiledRDD
      } else {
        // Buffering or not keeps the bottom line NaN.
        // However, buffering could make SentinelHub tiles to become almost empty.
        filtered.crop(croppedExtent, Options(force = false))
      }
    }
    val tileLayout = rdd.metadata.tileLayout
    val fullRDD = preprocessedRdd.withContext {
      _.mapValues[MultibandTile]((mbt: MultibandTile) => mbt.mapBands((i: Int, t: Tile) => raster.CroppedTile(t, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1))))
    }
    (gridBounds, croppedExtent, fullRDD)
  }

  class PowerOfTwoLocalLayoutScheme extends LayoutScheme {

    def zoomOut(level: LayoutLevel): LayoutLevel = {
      val LayoutLevel(zoom, LayoutDefinition(extent, tileLayout)) = level
      require(zoom > 0)
      // layouts may be uneven, don't let the short dimension go to 0
      val currentSize = level.layout.cellSize
      val outLayout = LayoutDefinition(RasterExtent(extent, CellSize(currentSize.width * 2.0, currentSize.height * 2.0)), level.layout.tileCols)

      LayoutLevel(zoom - 1, outLayout)
    }

    // not used in Pyramiding
    def zoomIn(level: LayoutLevel): LayoutLevel = ???

    def levelFor(extent: Extent, cellSize: CellSize): LayoutLevel = ???
  }

  private def saveRDDGeneric[K: SpatialComponent : Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], bandCount: Int, path: String, zLevel: Int = 6, cropBounds: Option[Extent] = None, formatOptions: GTiffOptions = new GTiffOptions): (String, Extent, util.Map[String, Any]) = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd, cropBounds)
    val gridBounds: GridBounds[Int] = preProcessResult._1
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"saveRDDGeneric with cropBounds:$cropBounds, layout: ${preprocessedRdd.metadata.tileLayout}, filenamePrefix: ${formatOptions.filenamePrefix} ")
    val assetMetadata = setupAssetMetadata(List(), croppedExtent, preprocessedRdd.metadata.crs, Array(gridBounds.height, gridBounds.width))
    try {
      val compression = determineCompression(formatOptions)
      val (tiffs: _root_.scala.collection.Map[Int, _root_.scala.Array[Byte]], cellType: CellType, detectedBandCount: Double, segmentCount: Int) = getCompressedTiles(preprocessedRdd, gridBounds, compression)

      val overviews =
        if (formatOptions.overviews.toUpperCase == "ALL" || (formatOptions.overviews.toUpperCase == "AUTO" && (gridBounds.width > 1024 || gridBounds.height > 1024))) {
          //create overviews
          val method = getOverviewResampleMethod(formatOptions)
          val levels = LocalLayoutScheme.inferLayoutLevel(preprocessedRdd.metadata)

          if (levels > 1) {
            val scheme = new PowerOfTwoLocalLayoutScheme()

            var nextOverviewLevel: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preprocessedRdd
            var nextZoom = -1
            val overviews = (1 to levels).reverse.map(level => {
              var zoom_rdd = Pyramid.up(nextOverviewLevel, scheme, level, Pyramid.Options(resampleMethod = method))
              nextOverviewLevel = zoom_rdd._2
              val overViewGridBounds = nextOverviewLevel.metadata.gridBoundsFor(croppedExtent, clamp = true).toGridType[Int]
              val (overViewTiffs: _root_.scala.collection.Map[Int, _root_.scala.Array[Byte]], cellType: CellType, detectedBandCount: Double, overViewSegmentCount: Int) = getCompressedTiles(nextOverviewLevel, overViewGridBounds, compression)
              val overviewTiff = toTiff(overViewTiffs, overViewGridBounds, nextOverviewLevel.metadata.tileLayout, compression, cellType, detectedBandCount, overViewSegmentCount)
              overviewTiff
            })

            overviews.toList

            /*if(levels>2) {
              val (lowerZoom,lowerOverviewLevel) = Pyramid.up(nextOverviewLevel,scheme,nextZoom)
              val stitched: Option[Raster[MultibandTile]] = lowerOverviewLevel.withContext(_.map(t=>(t._1.getComponent[SpatialKey](),t._2))).sparseStitch()
              List(overviewTiff,GeoTiffMultibandTile(stitched.get.tile))
            }else{*/
            //List(overviewTiff)
            //}
          } else {
            Nil
          }

        } else {
          Nil
        }

      val fixedPath =
        if (path.endsWith("out")) {
          path.substring(0, path.length - 3) + formatOptions.filenamePrefix + ".tif"
        } else {
          //what if this is a directory?
          path
        }
      val stacItemPath = FilenameUtils.removeExtension(fixedPath) + "_item.json"
      val metadata = new STACItem()
      metadata.asset(fixedPath)
      metadata.write(stacItemPath)
      val geoTiffResultObject = writeTiff(fixedPath, tiffs, gridBounds, croppedExtent, preprocessedRdd.metadata.crs, preprocessedRdd.metadata.tileLayout, compression, cellType, detectedBandCount, segmentCount, formatOptions = formatOptions, overviews = overviews)
      geoTiffResultObject.gdalInfoPath match {
        case Some(gdalInfoPath) =>
          updateGdalInfoJsonFile(gdalInfoPath, geoTiffResultObject.correctPath)
        case None => // do nothing
      }

      (geoTiffResultObject.correctPath, croppedExtent, assetMetadata)
    } finally {
      preprocessedRdd.unpersist()
    }
  }

  private def determinePredictor(formatOptions: GTiffOptions, geoTiffImageData: GeoTiffImageData): Option[Predictor] = {
    if (formatOptions.compressionPredictor > 1) {
      Some(Predictor(geoTiffImageData))
    } else {
      None
    }
  }

  private def determineCompressionForTile(tile: MultibandTile, formatOptions: GTiffOptions): Compression = {
    determineCompression(tile.toGeoTiffTile(), formatOptions)
  }

  private def determineCompression(formatOptions: GTiffOptions): Compression = {
    val compression = {
      formatOptions.compressionMethod match {
        case "zstd" => ZStdCompression(formatOptions.compressionLevel)
        case "deflate" => DeflateCompression(formatOptions.compressionLevel)
        case _ => throw new IllegalArgumentException(f"Compression method ${formatOptions.compressionMethod} is not supported, supported methods are: (zstd, deflate (default))")
      }
    }
    compression
  }


  private def determineCompression(geoTiffImageData: GeoTiffImageData, formatOptions: GTiffOptions): Compression = {
    val compression = {
      formatOptions.compressionMethod match {
        case "zstd" => ZStdCompression(formatOptions.compressionLevel)
        case "deflate" => DeflateCompression(formatOptions.compressionLevel)
        case _ => throw new IllegalArgumentException(f"Compression method ${formatOptions.compressionMethod} is not supported, supported methods are: (zstd, deflate (default))")
      }
    }
    if (formatOptions.compressionPredictor > 1) {
      val predictor = Predictor(geoTiffImageData)
      compression.withPredictor(predictor)
    } else {
      compression
    }
  }

  private def getOverviewResampleMethod(formatOptions: GTiffOptions): ResampleMethod = {
    formatOptions.resampleMethod match {
      case "near" => NearestNeighbor
      case "mode" => Mode
      case "average" => Average
      case "bilinear" => Bilinear
      case "max" => Max
      case "min" => Min
      case "med" => Median
      case _ => NearestNeighbor
    }
  }

  private def getCompressedTiles[K: SpatialComponent : Boundable : ClassTag](preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]], gridBounds: GridBounds[Int], compression: Compression): (collection.Map[Int, Array[Byte]], CellType, Double, Int) = {
    val tileLayout = preprocessedRdd.metadata.tileLayout

    val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
    val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

    val cols = tileLayout.tileCols
    val rows = tileLayout.tileRows

    val bandSegmentCount = totalCols * totalRows

    preprocessedRdd.sparkContext.setJobDescription(s"Write geotiff ${preprocessedRdd.metadata.toRasterExtent()} of type ${preprocessedRdd.metadata.cellType}")
    val totalBandCount = preprocessedRdd.sparkContext.longAccumulator("TotalBandCount")
    val typeAccumulator = new SetAccumulator[CellType]()
    preprocessedRdd.sparkContext.register(typeAccumulator, "CellType")
    val tiffs: collection.Map[Int, Array[Byte]] = preprocessedRdd.flatMap { case (key: K, multibandTile: MultibandTile) => {
      var bandIndex = -1
      if (multibandTile.bandCount > 0) {
        totalBandCount.add(multibandTile.bandCount)
      }
      typeAccumulator.add(multibandTile.cellType)
      //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
      //Not sure how this works out for other types of compression!!!

      val layoutCol = key.getComponent[SpatialKey]._1
      val layoutRow = key.getComponent[SpatialKey]._2
      if (layoutCol >= totalCols || layoutRow >= totalRows || layoutCol < 0 && layoutRow < 0) {
        logger.warn(f"Unexpected key: (c=$layoutCol, r=$layoutRow) should be between (0,0) and (c=$totalCols, r=$totalRows)")
      }

      val theCompressor = compression.createCompressor(multibandTile.bandCount)
      multibandTile.bands.map {
        tile => {
          bandIndex += 1
          val bandSegmentOffset = bandSegmentCount * bandIndex
          val index = totalCols * layoutRow + layoutCol + bandSegmentOffset

          val bytes =
            if (cols != tile.cols || rows != tile.rows) {
              logger.error(s"Incorrect tile size in geotiff: ${tile.cols}x${tile.rows} ")
              tile.crop(cols, rows, Options(clamp = false, force = true)).toBytes()
            } else {
              tile.toBytes()
            }
          //tiff format seems to require that we provide 'full' tiles
          val compressedBytes = theCompressor.compress(bytes, 0)
          (index, compressedBytes)
        }

      }
    }
    }.collectAsMap()


    preprocessedRdd.sparkContext.clearJobGroup()

    val cellType = {
      if (typeAccumulator.value.isEmpty) {
        preprocessedRdd.metadata.cellType
      } else {
        typeAccumulator.value.head
      }
    }
    println("Saving geotiff with Celltype: " + cellType)
    val detectedBandCount = if (totalBandCount.avg > 0) totalBandCount.avg else 1
    val segmentCount = (bandSegmentCount * detectedBandCount).toInt
    (tiffs, cellType, detectedBandCount, segmentCount)
  }


  private def setupAssetMetadata(bandNames: List[String], bbox:Extent, crs:CRS, shape: Array[Int]): util.Map[String, Any] = {
    val assetMetadata = new util.HashMap[String,Any]()
    val bands = new util.ArrayList[java.util.HashMap[String,Any]]()
    bandNames.foreach(name => {
      val rasterBands = new java.util.HashMap[String,Any]()
      rasterBands.put("name", name)
      bands.add(rasterBands)
    })
    if (!bands.isEmpty) assetMetadata.put("bands", bands)
    assetMetadata.put("proj:bbox",Array(bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax))
    crs.epsgCode.foreach(epsg => assetMetadata.put("proj:epsg", epsg))
    assetMetadata.put("proj:shape", shape)
    assetMetadata
  }

  // This implementation does not properly work, output tiffs are not properly aligned and colors are also incorrect
  private def saveRDDGenericTileGrid[K: SpatialComponent : Boundable : ClassTag](rdd: MultibandTileLayerRDD[K], path: String, tileGrid: String, cropBounds: Option[Extent] = Option.empty[Extent], options: GTiffOptions = new GTiffOptions): List[String] = {
    val preProcessResult: (GridBounds[Int], Extent, RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]) = preProcess(rdd, cropBounds)
    val croppedExtent: Extent = preProcessResult._2
    val preprocessedRdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = preProcessResult._3

    val tileLayout = preprocessedRdd.metadata.tileLayout

    val compression = determineCompression(options)

    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(preprocessedRdd.metadata.extent, preprocessedRdd.metadata.crs))

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      s"${path.substring(0, index)}-$tileId${path.substring(index)}"
    }

    preprocessedRdd
      .flatMap {
        case (key, tile) => features.map { case (name, extent) =>
          val tileBounds = preprocessedRdd.metadata.layout.mapTransform(extent)

          (name, extent, tileBounds)
        }.filter { case (_, _, tileBounds) =>
          KeyBounds(tileBounds).includes(key.getComponent[SpatialKey])
        }.map { case (name, extent, tileBounds) =>
          val re = preprocessedRdd.metadata.toRasterExtent()
          val gridBounds = re.gridBoundsFor(extent, clamp = true)
          val croppedExtent = re.extentFor(gridBounds, clamp = true)
          ((name, croppedExtent, tileBounds, gridBounds), (key, tile))
        }
      }.groupByKey()
      .map { case ((name, extent, tileBounds, gridBounds), tiles) =>
        //The part below is probably wrong: each tile in a fixed tilegrid, will have it's own 'tilelayout', while here
        //we use the global tilelayout of the RDD.
        val keyBounds = KeyBounds(tileBounds)
        val minKey = keyBounds.get.minKey.getComponent[SpatialKey]

        val totalCols = math.ceil(gridBounds.width.toDouble / tileLayout.tileCols).toInt
        val totalRows = math.ceil(gridBounds.height.toDouble / tileLayout.tileRows).toInt

        val bandSegmentCount = totalCols * totalRows
        val someTile = tiles.head._2
        val detectedBandCount = someTile.bandCount
        val cellType = someTile.cellType

        val tiffs = tiles.flatMap { case (key: K, multibandTile: MultibandTile) => {
          var bandIndex = -1

          //Warning: for deflate compression, the segmentcount and index is not really used, making it stateless.
          //Not sure how this works out for other types of compression!!!

          val theCompressor = compression.createCompressor(multibandTile.bandCount)
          multibandTile.bands.map {
            tile => {
              bandIndex += 1
              val layoutCol = key.getComponent[SpatialKey]._1 - minKey._1
              val layoutRow = key.getComponent[SpatialKey]._2 - minKey._2
              val bandSegmentOffset = bandSegmentCount * bandIndex
              val index = totalCols * layoutRow + layoutCol + bandSegmentOffset
              //tiff format seems to require that we provide 'full' tiles
              val bytes = raster.CroppedTile(tile, raster.GridBounds(0, 0, tileLayout.tileCols - 1, tileLayout.tileRows - 1)).toBytes()
              val compressedBytes = theCompressor.compress(bytes, 0)
              (index, compressedBytes)
            }
          }
        }
        }.toMap

        println("Saving geotiff with Celltype: " + cellType)

        val segmentCount = bandSegmentCount * detectedBandCount
        val newPath = newFilePath(path, name)
        val geoTiffResultObject = writeTiff(newPath, tiffs, gridBounds, extent.intersection(croppedExtent).get, preprocessedRdd.metadata.crs, tileLayout, compression, cellType, detectedBandCount, segmentCount, formatOptions = options)
        geoTiffResultObject.gdalInfoPath match {
          case Some(gdalInfoPath) =>
            updateGdalInfoJsonFile(gdalInfoPath, geoTiffResultObject.correctPath)
          case None => // do nothing
        }
        geoTiffResultObject.correctPath
      }.collect()
      .toList
  }

  private def writeTiff(path: String, tiffs: collection.Map[Int, Array[Byte]],
                        gridBounds: GridBounds[Int], croppedExtent: Extent, crs: CRS,
                        tileLayout: TileLayout, compression: Compression, cellType: CellType,
                        detectedBandCount: Double, segmentCount: Int,
                        formatOptions: GTiffOptions = new GTiffOptions, overviews: List[GeoTiffMultibandTile] = Nil
                       ): GeoTiffResultObject = {
    logger.info(s"Writing geotiff to $path with type ${cellType.toString()} and bands $detectedBandCount")
    val tiffTile: GeoTiffMultibandTile = toTiff(tiffs, gridBounds, tileLayout, compression, cellType, detectedBandCount, segmentCount)
    val options = if (formatOptions.colorMap.isDefined) {
      new GeoTiffOptions(colorMap = formatOptions.colorMap.map(IndexedColorMap.fromColorMap), colorSpace = ColorSpace.Palette)
    } else {
      val theColorspace = if (detectedBandCount == 3) {
        ColorSpace.RGB
      } else {
        ColorSpace.BlackIsZero
      }
      new GeoTiffOptions(colorSpace = theColorspace)
    }

    val theGeoTiff = new MultibandGeoTiff(tiffTile, croppedExtent, crs, formatOptions.tags, options, overviews = overviews.map(o => MultibandGeoTiff(o, croppedExtent, crs, options = options.copy(subfileType = Some(ReducedImage)))))
      .withCompression(formatOptions)

    writeGeoTiff(theGeoTiff, path, Some(formatOptions))
  }

  private def toTiff(tiffs: collection.Map[Int, Array[Byte]], gridBounds: GridBounds[Int], tileLayout: TileLayout, compression: Compression, cellType: CellType, detectedBandCount: Double, segmentCount: Int) = {
    val compressor = compression.createCompressor(segmentCount)
    lazy val emptySegment =
      ArrayTile.empty(cellType, tileLayout.tileCols, tileLayout.tileRows).toBytes

    val segments: Array[Array[Byte]] = Array.ofDim(segmentCount)
    val emptySegmentCompressed = compressor.compress(emptySegment, 0)
    cfor(0)(_ < segmentCount, _ + 1) { index => {
      val maybeBytes = tiffs.get(index)
      if (maybeBytes.isEmpty) {
        segments(index) = emptySegmentCompressed
      } else {
        segments(index) = maybeBytes.get
      }
    }
    }

    val segmentLayout = GeoTiffSegmentLayout(
      totalCols = gridBounds.width,
      totalRows = gridBounds.height,
      Tiled(tileLayout.tileCols, tileLayout.tileRows),
      BandInterleave,
      BandType.forCellType(cellType))

    val tiffTile: GeoTiffMultibandTile = GeoTiffMultibandTile(
      new ArraySegmentBytes(segments),
      compressor.createDecompressor(),
      segmentLayout,
      compression,
      detectedBandCount.toInt,
      cellType
    )
    tiffTile
  }

  // This implementation should not be used anymore (deprecated)
  def saveStitched(
                    rdd: SRDD,
                    path: String,
                    cropBounds: Option[Map[String, Double]],
                    cropDimensions: Option[ArrayList[Int]],
                    compression: Compression,
                    formatOptions: Option[GTiffOptions] = None,
                  ): Item = {
    val contextRDD = ContextRDD(rdd, rdd.metadata)

    val stitched: Raster[MultibandTile] = contextRDD.stitch()

    val adjusted = {
      val cropped =
        cropBounds match {
          case Some(extent) => stitched.crop(toExtent(extent))
          case None => stitched
        }

      val resampled =
        cropDimensions.map(_.asScala.toArray) match {
          case Some(dimensions) =>
            cropped.resample(dimensions(0), dimensions(1))
          case None =>
            cropped
        }

      resampled
    }
    val fo = formatOptions match {
      case Some(fo) => fo
      case None => new GTiffOptions()
    }
    fo.assertNoConflicts()

    val geoTiff = MultibandGeoTiff(adjusted, contextRDD.metadata.crs, GeoTiffOptions(compression))
      .withOverviews(getOverviewResampleMethod(fo), blockSize = fo.tileSize)
      .withCompression(formatOptions.getOrElse(new GTiffOptions))

    writeGeoTiff(geoTiff, path, gtiffOptions = formatOptions)
    val assetMetadata = setupAssetMetadata(List(), adjusted.extent, contextRDD.metadata.crs, Array(adjusted.rows,adjusted.cols))

    Item(id = UUID.randomUUID().toString, datetime = null, bbox = adjusted.extent,
      Collections.singletonMap("openEO", Asset(path, metadata = assetMetadata)))
  }

  def saveStitchedTileGrid(
                            rdd: SRDD,
                            path: String,
                            tileGrid: String,
                            cropBounds: Option[Map[String, Double]],
                            cropDimensions: Option[ArrayList[Int]],
                            compression: Compression,
                            formatOptions: Option[GTiffOptions] = None,
                          )
  : JList[Item] = {
    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent, rdd.metadata.crs))

    def newFilePath(path: String, tileId: String) = {
      val index = path.lastIndexOf(".")
      val extension = if (index >= 0) path.substring(index) else ".tiff"
      val prefix = if (index >= 0) path.substring(0, index) else "openEO"
      s"$prefix-$tileId$extension"
    }

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs
    val groupedRDD = rdd.flatMap {
      case (key, tile) => features.filter { case (_, extent) =>
        val tileBounds = layout.mapTransform(extent)

        KeyBounds(tileBounds).includes(key)
      }.map { case (name, extent) =>
        ((name, extent), (key, tile))
      }
    }.groupByKey()
    val geotiffResults = groupedRDD.map {
      case ((tileId, extent), tiles) =>
        // Each executor writes to a unique folder to avoid conflicts:
        val filePath = {
          if (TaskContext.get().attemptNumber() > 0) {
            // Each executor writes to a unique folder to avoid conflicts:
            createExecutorAttemptDirectory(Path.of(path).getParent)

          } else {
            Path.of(path).getParent
          }
        }
          .resolve(newFilePath(Path.of(path).getFileName.toString, tileId)).toString


        (stitchAndWriteToTiff(tiles, filePath, layout, crs, extent, croppedExtent, cropDimensions, compression, formatOptions), tileId, extent)
    }.collect()
    val res = geotiffResults.map {
      case (geoTiffResultObject, tileId, croppedExtent) =>
        val destinationPath = moveFromExecutorAttemptDirectory(Path.of(path).getParent, geoTiffResultObject)
        (destinationPath, tileId, croppedExtent)
    }

    val items = res.map { case (path, tileId, extent) =>
      val assetMetadata = setupAssetMetadata(List(), extent, crs, Array(layout.rows.toInt,layout.cols.toInt))
      Item(id = s"${UUID.randomUUID()}_$tileId", datetime = null, bbox = extent,
        assets = Collections.singletonMap("openEO", Asset(path, metadata= assetMetadata)))
    }

    cleanupTemporaryResults(geotiffResults.map(_._1), Path.of(path).getParent.toString)


    items.toList.asJava
  }

  private def stitchAndWriteToTiff(tiles: Iterable[(SpatialKey, MultibandTile)], filePath: String,
                                   layout: LayoutDefinition, crs: CRS, geometry: Geometry,
                                   croppedExtent: Option[Extent], cropDimensions: Option[java.util.ArrayList[Int]],
                                   compression: Compression, formatOptions: Option[GTiffOptions] = None
                                  ): GeoTiffResultObject = {
    val raster: Raster[MultibandTile] = ContextSeq(tiles, layout).sparseStitch(geometry.extent) match {
      case Some(stitched) => stitched
      case _ => {
        logger.error("stitchAndWriteToTiff(): sparseStitch returned None. Recovering by writing an empty raster.")
        if (tiles.isEmpty) {
          val noDataTile = UByteConstantTile(ubyteNODATA, 256, 256)
          Raster(MultibandTile(noDataTile), layout.extent)
        } else {
          Raster(tiles.head._2, layout.extent)
        }
      }
    }

    val re = raster.rasterExtent
    val alignedExtent = re.createAlignedGridExtent(geometry.extent).extent

    val stitched: Raster[MultibandTile] = raster.mask(geometry).crop(alignedExtent)

    //TODO this additional cropping + resampling might not be needed, as a tile grid already defines a clear cropping
    val adjusted = {
      val cropped =
        croppedExtent match {
          case Some(extraExtent) => stitched.crop(extraExtent, Crop.Options(clamp = false))
          case None => stitched
        }

      val resampled =
        cropDimensions.map(_.asScala.toArray) match {
          case Some(dimensions) =>
            cropped.resample(dimensions(0), dimensions(1))
          case None =>
            cropped
        }

      resampled
    }

    logger.info(f"stitchAndWriteToTiff with layout: $layout, croppedExtent: $croppedExtent, geometry: $geometry, cols & rows: ${adjusted.cols} & ${adjusted.rows} ")
    val fo = formatOptions match {
      case Some(fo) => fo
      case None =>
        val fo = new GTiffOptions()
        // If no formatOptions was specified, the default was to generate pyramids
        fo.overviews = "ALL"
        fo
    }
    fo.assertNoConflicts()
    var geotiff = MultibandGeoTiff(adjusted.tile, adjusted.extent, crs,
      fo.tags, GeoTiffOptions(compression)).withCompression(formatOptions.getOrElse(new GTiffOptions))
    val gridBounds = adjusted.extent
    if (fo.overviews.toUpperCase == "ALL" ||
      fo.overviews.toUpperCase == "AUTO" && (gridBounds.width > 1024 || gridBounds.height > 1024)
    ) {
      val resampleMethod = getOverviewResampleMethod(fo)
      val tileCols = adjusted.cols
      val tileRows = adjusted.rows
      var overviews = List[MultibandGeoTiff]()
      var overview = geotiff
      var reductionFactor = 2
      if (fo.overviews == "AUTO") {
        // skip the first overview level for AUTO
        overview = overview.buildOverview(resampleMethod, 2, blockSize = fo.tileSize)
        reductionFactor *= 2
      }
      val overviewReductions: List[Int] = defaultOverviewReductions(fo, geotiff.tile.cols, geotiff.tile.rows, tileCols, tileRows)
      while (overviewReductions.nonEmpty && overviewReductions.last >= reductionFactor) {
        overview = overview.buildOverview(resampleMethod, 2, blockSize = fo.tileSize)
        if (overviewReductions.contains(reductionFactor)) {
          overviews = overviews :+ overview
        }
        reductionFactor *= 2
      }
      geotiff = MultibandGeoTiff(geotiff.tile, geotiff.extent, geotiff.crs, geotiff.tags, geotiff.options, overviews)
        .withCompression(formatOptions.getOrElse(new GTiffOptions))
    }
    writeGeoTiff(geotiff, filePath, Some(fo))
  }

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                  formatOptions: GTiffOptions,
                 ): JList[Item] =
    saveSamples(rdd, path, polygons, sampleNames, compression, Some(formatOptions))

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                 ): JList[Item] =
    saveSamples(rdd, path, polygons, sampleNames, compression, None)

  def saveSamples(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                  formatOptions: Option[GTiffOptions],
                 ): JList[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons, rdd.metadata.crs)
    val features = sampleNames.asScala.toSeq.zip(reprojected.polygons)
    groupByFeatureAndWriteToTiff(rdd, cropBounds = None, features, path, cropDimensions = None, compression, formatOptions)
  }

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                  formatOptions: GTiffOptions,
                 ): JList[Item] =
    saveSamplesSpatial(rdd, path, polygons, sampleNames, compression, Some(formatOptions))

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                 ): JList[Item] =
    saveSamplesSpatial(rdd, path, polygons, sampleNames, compression, None)

  def saveSamplesSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                  path: String,
                  polygons: ProjectedPolygons,
                  sampleNames: JList[String],
                  compression: Compression,
                  formatOptions: Option[GTiffOptions],
                 ): JList[Item] = {
    val reprojected = ProjectedPolygons.reproject(polygons, rdd.metadata.crs)
    val features = sampleNames.asScala.toSeq.zip(reprojected.polygons)
    groupByFeatureAndWriteToTiffSpatial(rdd, cropBounds = None, features, path, cropDimensions = None, compression, formatOptions)
  }


  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   compression: Compression,
                                   filenamePrefix: Option[String],
                                  ): JList[Item] = {
    val formatOptions =
      if (filenamePrefix.isDefined) {
        val formatOptions = new GTiffOptions
        formatOptions.setFilenamePrefix(filenamePrefix.get)
        Some(formatOptions)
      } else None
    geotrellis.geotiff.saveStitchedTileGridTemporal(rdd, path, tileGrid, Option.empty, Option.empty, compression, formatOptions)
  }

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   compression: Compression,
                                  ): JList[Item] =
    geotrellis.geotiff.saveStitchedTileGridTemporal(rdd, path, tileGrid, Option.empty, Option.empty, compression)

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   compression: Compression,
                                   options: GTiffOptions,
                                  ): JList[Item] =
    geotrellis.geotiff.saveStitchedTileGridTemporal(rdd, path, tileGrid, Option.empty, Option.empty, compression, Some(options))

  def saveStitchedTileGridTemporal(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                   path: String,
                                   tileGrid: String,
                                   cropBounds: Option[Map[String, Double]],
                                   cropDimensions: Option[ArrayList[Int]],
                                   compression: Compression,
                                   formatOptions: Option[GTiffOptions] = None,
                                  ): JList[Item] = {
    val features = TileGrid.computeFeaturesForTileGrid(tileGrid, ProjectedExtent(rdd.metadata.extent, rdd.metadata.crs))
      .map { case (name, extent) => (name, extent.toPolygon()) }
    groupByFeatureAndWriteToTiff(rdd, cropBounds = None, features, path, cropDimensions = None, compression, formatOptions)
  }

  private def groupByFeatureAndWriteToTiff(rdd: MultibandTileLayerRDD[SpaceTimeKey],
                                           cropBounds: Option[java.util.Map[String, Double]],
                                           features: Seq[(String, Geometry)],
                                           path: String,
                                           cropDimensions: Option[ArrayList[Int]],
                                           compression: Compression,
                                           formatOptions: Option[GTiffOptions] = None,
                                          ): JList[Item] = {
    val featuresBC: Broadcast[Seq[(String, Geometry)]] = SparkContext.getOrCreate().broadcast(features)

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs

    val filenamePrefix = formatOptions match {
      case Some(fo) => fo.filenamePrefix
      case None => new GTiffOptions().filenamePrefix
    }
    val ret = rdd
      .flatMap { case (key, tile) => featuresBC.value
        .filter { case (_, geometry) => layout.mapTransform.keysForGeometry(geometry) contains key.spatialKey }
        .map { case (name, geometry) => ((name, (geometry, key.time)), (key.spatialKey, tile)) }
      }
      .groupByKey()
      .map { case ((name, (geometry, time)), tiles) =>
        val filename = s"${filenamePrefix}_${DateTimeFormatter.ISO_DATE.format(time)}_$name.tif"
        val filePath = Paths.get(path).resolve(filename).toString
        val timestamp = time format DateTimeFormatter.ISO_ZONED_DATE_TIME
        val assetMetadata = setupAssetMetadata(List(), croppedExtent.getOrElse(geometry.extent), crs, Array(layout.rows.toInt,layout.cols.toInt))
        (stitchAndWriteToTiff(tiles, filePath, layout, crs, geometry, croppedExtent, cropDimensions, compression, formatOptions).correctPath,
          timestamp, geometry.extent, name, assetMetadata)
      }
      .collect()

    val items = for {
      (path, timestamp, extent, name, assetMetadata) <- ret
    } yield Item(id = f"${UUID.randomUUID()}_${timestamp}_$name", datetime = timestamp, bbox = extent,
      assets = Collections.singletonMap("openEO", Asset(path, metadata = assetMetadata)))

    items.toList.asJava
  }

  private def groupByFeatureAndWriteToTiffSpatial(rdd: MultibandTileLayerRDD[SpatialKey],
                                                  cropBounds: Option[java.util.Map[String, Double]],
                                                  features: Seq[(String, Geometry)],
                                                  path: String,
                                                  cropDimensions: Option[util.ArrayList[Int]],
                                                  compression: Compression,
                                                  formatOptions: Option[GTiffOptions] = None,
                                          ): JList[Item] = {
    val featuresBC: Broadcast[Seq[(String, Geometry)]] = SparkContext.getOrCreate().broadcast(features)

    val croppedExtent = cropBounds.map(toExtent)

    val layout = rdd.metadata.layout
    val crs = rdd.metadata.crs

    val filenamePrefix = formatOptions match {
      case Some(fo) => fo.filenamePrefix
      case None => new GTiffOptions().filenamePrefix
    }
    val ret = rdd
      .flatMap { case (key, tile) => featuresBC.value
        .filter { case (_, geometry) => layout.mapTransform.keysForGeometry(geometry) contains key }
        .map { case (name, geometry) => ((name, geometry), (key, tile)) }
      }
      .groupByKey()
      .map { case ((name, geometry), tiles) =>
        val filename = s"${filenamePrefix}_$name.tif"
        val filePath = Paths.get(path).resolve(filename).toString
        val assetMetadata = setupAssetMetadata(List(), croppedExtent.getOrElse(geometry.extent), crs, Array(layout.rows.toInt,layout.cols.toInt))
        (stitchAndWriteToTiff(tiles, filePath, layout, crs, geometry, croppedExtent, cropDimensions, compression, formatOptions).correctPath,
          geometry.extent, assetMetadata)
      }
      .collect()

    val items = for {
      (path, extent, assetMetadata) <- ret
    } yield Item(id = f"${UUID.randomUUID()}", datetime = null, bbox = extent,
      assets = Collections.singletonMap("openEO", Asset(path, metadata= assetMetadata)))

    items.toList.asJava
  }

  private[geotrellis] case class GeoTiffResultObject(correctPath: String, fileExists: Boolean, gdalInfoPath: Option[String])

  private[geotrellis] def writeGeoTiff(geoTiff: MultibandGeoTiff, path: String, gtiffOptions: Option[GTiffOptions]): GeoTiffResultObject = {
    val tempFile = getTempFile(FilenameUtils.getBaseName(path) + "_", ".tif")
    geoTiff.write(tempFile.toString, optimizedOrder = true)
    val fileExists = Files.exists(tempFile)
    var gdalInfoPathName: Option[Path] = None

    if (fileExists) {
      gtiffOptions.foreach { options =>
        val lowerCaseTagNames = for {
          bandTags <- options.tags.bandTags
          (key, _) <- bandTags
        } yield key.toLowerCase

        if (lowerCaseTagNames.contains("scale") || lowerCaseTagNames.contains("offset")) {
          val (tileWidth, tileHeight) = (
            geoTiff.imageData.segmentLayout.tileLayout.tileCols,
            geoTiff.imageData.segmentLayout.tileLayout.tileRows
          )
          if (tileWidth != tileHeight) throw new AssertionError(s"tile width $tileWidth != tile height $tileHeight")
        }
      }

      gdalInfoPathName = createGdalInfo(tempFile)
    } else {
      logger.warn("writeGeoTiff() File was not created: " + path)
    }
    var gdalInfoPathNameStr = gdalInfoPathName.map(_.toString)
    if (CreoS3Utils.isS3(path)) {
      // Converting to Path and back could change the s3:// prefix to s3:/
      // The following line corrects this:
      val correctS3Path = path.replaceFirst("s3:/(?!/)", "s3://")
      if (fileExists) {
        CreoS3Utils.uploadToS3TryFirstWithStreaming(tempFile, path)
      }
      gdalInfoPathName match {
        case Some(gdalInfoPath) =>
          CreoS3Utils.uploadToS3TryFirstWithStreaming(gdalInfoPath, correctS3Path + GDALINFO_SUFFIX)
          gdalInfoPathNameStr = Some(correctS3Path + GDALINFO_SUFFIX)
        case None => // do nothing
      }
      GeoTiffResultObject(correctS3Path, fileExists, gdalInfoPathName.map(_.toString.replaceFirst("s3:/(?!/)", "s3://")))
    } else {
      // Retry should not be needed at this point, but it is almost free to keep it.
      if (fileExists) {
        CreoS3Utils.moveOverwriteWithRetries(tempFile.toString, path)
      }
      gdalInfoPathName match {
        case Some(gdalInfoPath) =>
          CreoS3Utils.moveOverwriteWithRetries(gdalInfoPath.toString, path + GDALINFO_SUFFIX)
          gdalInfoPathNameStr = Some(path + GDALINFO_SUFFIX)
        case None => // do nothing
      }
    }
    GeoTiffResultObject(path, fileExists, gdalInfoPathNameStr)
  }

  val GDALINFO_SUFFIX = "_gdalinfo.json"

  private def createGdalInfo(rasterFilePath: Path): Option[Path] = {
    // gdalinfo json files are used to generate stac metadata
    // A gdalinfo file is generated just after the tiff file is written to avoid re-downloading it from S3.
    // Some users might like to load the gdalinfo files directly, they can use attach_gdalinfo_assets=True
    val gdalinfo_on_executor = sys.env.getOrElse("GDALINFO_ON_EXECUTOR", "true").toBoolean
    if (!gdalinfo_on_executor) {
      // Allow to quickly disable gdalinfo on executor if something goes wrong
      // openeo-geopyspark-driver will then call gedalinfo by itself.
      return None
    }
    import java.nio.charset._
    import scala.sys.process._

    val outputBuffer = new StringBuilder
    val cerrBuffer = new StringBuilder
    val processLogger = ProcessLogger(
      line => outputBuffer appendAll line + "\n",
      line => cerrBuffer appendAll line + "\n",
    )

    val args = Seq("gdalinfo", rasterFilePath.toString, "-json", "-stats", "--config", "GDAL_IGNORE_ERRORS", "ALL")
    val exitCode = args ! processLogger

    if (cerrBuffer.nonEmpty) {
      logger.info(s"gdalinfo warnings: ${cerrBuffer.toString()}") // Mostly harmless messages
    }
    val outputBufferString = outputBuffer.toString().trim
    if (exitCode == 0) {
      val gdalInfoPath = Path.of(rasterFilePath.toString + GDALINFO_SUFFIX)
      Files.write(gdalInfoPath, outputBufferString.getBytes(StandardCharsets.UTF_8))
      Some(gdalInfoPath)
    }
    else {
      logger.warn(s"${args mkString " "} failed; output was: $outputBufferString")
      None
    }
  }


  case class ContextSeq[K, V, M](tiles: Iterable[(K, V)], metadata: LayoutDefinition) extends Seq[(K, V)] with Metadata[LayoutDefinition] {
    override def length: Int = tiles.size

    override def apply(idx: Int): (K, V) = tiles.toSeq(idx)

    override def iterator: Iterator[(K, V)] = tiles.iterator
  }

  def assertSafeToUseInFilePath(filepath: String): Unit = {
    val name = filepath.split("/").last
    assertValidWindowsFilename(name)
    if (filepath.contains("..") || filepath.contains("%") || filepath.contains("|")) {
      throw new IllegalArgumentException("Invalid filepath: " + filepath)
    }
  }


  /**
   * http://msdn.microsoft.com/en-us/library/aa365247.aspx
   */
  def assertValidWindowsFilename(filename: String): Unit = {
    // TODO: Is there a standard library function for this?
    val filenameLower = filename.toLowerCase
    val invalidCharacters = Seq("<", ">", ":", "\"", "/", "\\", "|", "?", "*")
    if (invalidCharacters.exists(filenameLower.contains)) {
      throw new IllegalArgumentException("Invalid characters in filename: " + filename)
    }

    val filenameWithoutExtension = filename.split('.').head
    val invalidNames = Seq("CON", "PRN", "AUX", "NUL", "COM0", "COM1", "COM2",
      "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2",
      "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9")
    if (invalidNames.contains(filenameWithoutExtension.toUpperCase())) {
      throw new IllegalArgumentException("Invalid filename: " + filename)
    }
  }
}
