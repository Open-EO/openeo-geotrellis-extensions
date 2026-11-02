package org.openeo.geotrellis.zarr

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.github.luben.zstd.Zstd
import geotrellis.layer.{Boundable, SpaceTimeKey, SpatialComponent, SpatialKey}
import geotrellis.raster.{BitCellType, ByteCellType, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, NODATA, ShortCellType, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, Tile, UByteCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, shortNODATA, ubyteNODATA, ushortNODATA}
import geotrellis.spark.MultibandTileLayerRDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import java.nio.{ByteBuffer, ByteOrder}
import scala.reflect.ClassTag

/**
 * Writes a datacube as a Zarr v3 store.
 *
 * Layout:
 * {{{
 *   <store>.zarr/
 *     zarr.json            ← root group, with inline consolidated metadata
 *     <band>/              ← one array per band (or a single array named after the store)
 *       zarr.json
 *       c/[t/]<row>/<col>  ← one chunk per tile
 *     x/  y/  [time/]      ← coordinate arrays
 * }}}
 *
 * Chunks are encoded with the "bytes" codec (little-endian) followed by "zstd" (level 3).
 */
object ZarrWriter {

  private val logger = LoggerFactory.getLogger(ZarrWriter.getClass)

  val ZarrFormat: Int = 3
  val ZstdLevel: Int = 3
  val MetadataFileName: String = "zarr.json"
  private val ChunkKeySeparator = "/"

  private lazy val objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

  def saveZarr(rdd:MultibandTileLayerRDD[SpaceTimeKey],path:String, zarrOptions: ZarrOptions):Unit = {
    saveZarrGeneric(rdd,path,zarrOptions)
  }

  def saveZarrSpatial(rdd:MultibandTileLayerRDD[SpatialKey],path:String, zarrOptions: ZarrOptions):Unit = {
    saveZarrGeneric(rdd,path,zarrOptions)
  }

  def saveZarrGeneric[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K],path:String, zarrOptions: ZarrOptions):Unit= {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val arrayNames: Array[String] =
      if (zarrOptions.numberBands > 1) {
        checkBandNames(zarrOptions.bandNames)
        zarrOptions.bandNames
      } else Array(getGroupName(path))

    // relative node path -> node metadata, written inline in the root zarr.json
    val consolidated = new java.util.TreeMap[String, Object]()

    val metadata = rdd.metadata
    val cellType = metadata.cellType
    val (zarrType: String, fillValue: Option[Number]) = toZarrType(cellType)

    val tileRows = metadata.tileRows
    val tileCols = metadata.tileCols

    val xValues = for (x <- 0 until metadata.cols.toInt) yield metadata.extent.xmin + x * metadata.cellwidth + metadata.cellwidth / 2.0
    val yValues = for (y <- 0 until metadata.rows.toInt) yield metadata.extent.ymax - y * metadata.cellheight - metadata.cellheight / 2.0

    val keys = rdd.keys.collect()
    val shapeOri = Array[Int](metadata.rows.toInt, metadata.cols.toInt)
    val chunkOri = Array[Int](tileRows, tileCols)
    val (timeValues: Map[Long,Int],shape,chunk,hasTemp) = keys match {
      case m: Array[SpaceTimeKey] =>
        val dist = m.map(_.temporalKey.instant).distinct.sorted
        writeCoordinate(path, "time", dist, consolidated)
        (dist.zipWithIndex.toMap, dist.length+:shapeOri, 1+:chunkOri,true)
      case _ => (Map[Long,Int](),shapeOri,chunkOri, false)
    }

    val dataAttributes = new dataAttribute(metadata, zarrOptions, hasTemp)
    // _FillValue lets xarray mask no-data; only set when the cell type defines no-data
    val noDataAttributes: Map[String, Object] =
      fillValue.map(v => Map[String, Object]("_FillValue" -> fillValueJson(zarrType, Some(v)))).getOrElse(Map.empty)
    val dataArrayMetadata = arrayMetadata(shape, chunk, zarrType, fillValueJson(zarrType, fillValue),
      dataAttributes, noDataAttributes)
    for (arrayName <- arrayNames) {
      writeNodeMetadata(path, arrayName, dataArrayMetadata, consolidated)
    }

    writeCoordinate(path, "x", xValues.toArray, consolidated)
    writeCoordinate(path, "y", yValues.toArray, consolidated)

    rdd.foreach { case (k, multibandTile) =>
      writeData(multibandTile, k, path, arrayNames, tileRows, tileCols, cellType, zarrType, timeValues)
    }

    // Root group last, so its consolidated metadata describes a complete store.
    val consolidatedMetadata = new java.util.LinkedHashMap[String, Object]()
    consolidatedMetadata.put("kind", "inline")
    consolidatedMetadata.put("must_understand", java.lang.Boolean.FALSE)
    consolidatedMetadata.put("metadata", consolidated)
    val rootMetadata = groupMetadata()
    rootMetadata.put("consolidated_metadata", consolidatedMetadata)
    writeJson(Paths.get(path), rootMetadata, MetadataFileName)
  }

  private def toZarrType(cellType: CellType): (String, Option[Number])= {
    cellType match {
      case BitCellType => ("uint8",None)
      case ByteCellType => ("int8",None)
      case UByteCellType => ("uint8",None)
      case ShortCellType => ("int16",None)
      case UShortCellType => ("uint16",None)
      case IntCellType => ("int32",None)
      case FloatCellType => ("float32",None)
      case DoubleCellType => ("float64",None)
      case ByteConstantNoDataCellType => ("int8",Some(byteNODATA))
      case UByteConstantNoDataCellType => ("uint8",Some(ubyteNODATA))
      case ShortConstantNoDataCellType => ("int16",Some(shortNODATA))
      case UShortConstantNoDataCellType => ("uint16",Some(ushortNODATA))
      case IntConstantNoDataCellType => ("int32",Some(NODATA))
      case FloatConstantNoDataCellType => ("float32",Some(floatNODATA))
      case DoubleConstantNoDataCellType => ("float64",Some(doubleNODATA))
      case ct: ByteUserDefinedNoDataCellType => ("int8",Some(ct.noDataValue))
      case ct: UByteUserDefinedNoDataCellType => ("uint8",Some(ct.widenedNoData.asInt))
      case ct: ShortUserDefinedNoDataCellType => ("int16",Some(ct.noDataValue))
      case ct: UShortUserDefinedNoDataCellType => ("uint16",Some(ct.widenedNoData.asInt))
      case ct: IntUserDefinedNoDataCellType => ("int32",Some(ct.noDataValue))
      case ct: FloatUserDefinedNoDataCellType => ("float32",Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => ("float64",Some(ct.noDataValue))
      case ct => throw new IllegalArgumentException(s"save_result zarr: unsupported cell type $ct")
    }
  }

  /** Zarr v3 JSON encoding of a fill value: NaN/Infinity are encoded as strings. */
  private def fillValueJson(zarrType: String, fillValue: Option[Number]): Object = {
    val value: Number = fillValue.getOrElse(Int.box(0))
    if (zarrType.startsWith("float")) {
      val d = value.doubleValue()
      if (d.isNaN) "NaN"
      else if (d.isPosInfinity) "Infinity"
      else if (d.isNegInfinity) "-Infinity"
      else Double.box(d)
    } else Long.box(value.longValue())
  }

  private def byteSize(zarrType: String): Int = zarrType match {
    case "int8" | "uint8" => 1
    case "int16" | "uint16" => 2
    case "int32" | "float32" => 4
    case "int64" | "float64" => 8
  }

  private def groupMetadata(): java.util.LinkedHashMap[String, Object] = {
    val group = new java.util.LinkedHashMap[String, Object]()
    group.put("zarr_format", Int.box(ZarrFormat))
    group.put("node_type", "group")
    group.put("attributes", new java.util.LinkedHashMap[String, Object]())
    group
  }

  private def arrayMetadata(shape: Array[Int], chunkShape: Array[Int], zarrType: String, fillValue: Object,
                            attributes: ZarrAttributes,
                            extraAttributes: Map[String, Object] = Map.empty): java.util.LinkedHashMap[String, Object] = {
    val chunkGridConfig = new java.util.LinkedHashMap[String, Object]()
    chunkGridConfig.put("chunk_shape", chunkShape)
    val chunkGrid = new java.util.LinkedHashMap[String, Object]()
    chunkGrid.put("name", "regular")
    chunkGrid.put("configuration", chunkGridConfig)

    val chunkKeyConfig = new java.util.LinkedHashMap[String, Object]()
    chunkKeyConfig.put("separator", ChunkKeySeparator)
    val chunkKeyEncoding = new java.util.LinkedHashMap[String, Object]()
    chunkKeyEncoding.put("name", "default")
    chunkKeyEncoding.put("configuration", chunkKeyConfig)

    val bytesConfig = new java.util.LinkedHashMap[String, Object]()
    bytesConfig.put("endian", "little")
    val bytesCodec = new java.util.LinkedHashMap[String, Object]()
    bytesCodec.put("name", "bytes")
    bytesCodec.put("configuration", bytesConfig)

    val zstdConfig = new java.util.LinkedHashMap[String, Object]()
    zstdConfig.put("level", Int.box(ZstdLevel))
    zstdConfig.put("checksum", java.lang.Boolean.FALSE)
    val zstdCodec = new java.util.LinkedHashMap[String, Object]()
    zstdCodec.put("name", "zstd")
    zstdCodec.put("configuration", zstdConfig)

    val array = new java.util.LinkedHashMap[String, Object]()
    array.put("zarr_format", Int.box(ZarrFormat))
    array.put("node_type", "array")
    array.put("shape", shape)
    array.put("data_type", zarrType)
    array.put("chunk_grid", chunkGrid)
    array.put("chunk_key_encoding", chunkKeyEncoding)
    array.put("fill_value", fillValue)
    array.put("codecs", java.util.Arrays.asList(bytesCodec, zstdCodec))
    val attributeMap = new java.util.LinkedHashMap[String, Object](attributes.toMap)
    extraAttributes.foreach { case (k, v) => attributeMap.put(k, v) }
    array.put("attributes", attributeMap)
    array.put("dimension_names", attributes.dimensionNames)
    array
  }

  private def writeNodeMetadata(rootPath: String, relPath: String, nodeMetadata: java.util.Map[String, Object],
                                consolidated: java.util.Map[String, Object]): Unit = {
    consolidated.put(relPath, nodeMetadata)
    writeJson(Paths.get(rootPath, relPath), nodeMetadata, MetadataFileName)
  }

  private def writeJson(dir: Path, content: Object, fileName: String): Unit = {
    val file = dir.resolve(fileName)
    try {
      Files.createDirectories(dir)
      objectMapper.writeValue(file.toFile, content)
    } catch {
      case t: Throwable =>
        logger.error(s"save_result zarr: Failed to write to $file : ${t.getMessage}", t)
        throw t
    }
  }

  private def writeChunk(arrayDir: Path, chunkIndex: Seq[Int], data: Array[Byte]): Unit = {
    val chunkFile = chunkIndex.foldLeft(arrayDir.resolve("c"))((p, i) => p.resolve(i.toString))
    try {
      Files.createDirectories(chunkFile.getParent)
      Files.write(chunkFile, Zstd.compress(data, ZstdLevel))
    } catch {
      case t: Throwable =>
        logger.error(s"save_result zarr: Failed to write chunk $chunkFile : ${t.getMessage}", t)
        throw t
    }
  }

  private def writeCoordinate(path: String, name: String, values: Array[_],
                              consolidated: java.util.Map[String, Object]): Unit = {
    val (zarrType, fillValue, bytes) = values match {
      case longs: Array[Long] =>
        val buf = ByteBuffer.allocate(longs.length * 8).order(ByteOrder.LITTLE_ENDIAN)
        longs.foreach(v => buf.putLong(v))
        ("int64", Long.box(0L): Object, buf.array())
      case doubles: Array[Double] =>
        val buf = ByteBuffer.allocate(doubles.length * 8).order(ByteOrder.LITTLE_ENDIAN)
        doubles.foreach(v => buf.putDouble(v))
        ("float64", "NaN": Object, buf.array())
      case other => throw new IllegalArgumentException(s"save_result zarr: unsupported coordinate type ${other.getClass}")
    }
    val shape = Array(values.length)
    writeNodeMetadata(path, name, arrayMetadata(shape, shape, zarrType, fillValue, new variableAttribute(name)), consolidated)
    writeChunk(Paths.get(path, name), Seq(0), bytes)
  }

  /** Raw (no-data agnostic) little-endian, row-major bytes of a tile. */
  private def encodeTile(tile: Tile, zarrType: String): Array[Byte] = {
    val raw = tile.withNoData(None)
    val buf = ByteBuffer.allocate(raw.cols * raw.rows * byteSize(zarrType)).order(ByteOrder.LITTLE_ENDIAN)
    zarrType match {
      case "int8" | "uint8" => raw.foreach { v => buf.put(v.toByte); () }
      case "int16" | "uint16" => raw.foreach { v => buf.putShort(v.toShort); () }
      case "int32" => raw.foreach { v => buf.putInt(v); () }
      case "float32" => raw.foreachDouble { v => buf.putFloat(v.toFloat); () }
      case "float64" => raw.foreachDouble { v => buf.putDouble(v); () }
    }
    buf.array()
  }

  private def writeData[K](multibandTile: MultibandTile, k: K, path: String,
                           arrayNames: Array[String], tileRows: Int, tileCols: Int,
                           cellType: CellType, zarrType: String,
                           timeValues: Map[Long, Int]): Unit = {
    val (spatialKey, timeIndex) = k match {
      case key: SpatialKey => (key, None)
      case key: SpaceTimeKey => (key.spatialKey, Some(timeValues(key.temporalKey.instant)))
      case other => throw new IllegalArgumentException(s"save_result zarr: unsupported key type ${other.getClass}")
    }
    val bandCount = multibandTile.bandCount
    if (bandCount != arrayNames.length) {
      throw new Exception(s"save_result zarr: the expected number of band is ${arrayNames.length}, but was $bandCount")
    }
    val chunkIndex = timeIndex.toSeq ++ Seq(spatialKey.row, spatialKey.col)

    (0 until bandCount).foreach { b =>
      val band = multibandTile.band(b)
      if (band.cols != tileCols || band.rows != tileRows) {
        throw new Exception(s"save_result zarr: expected tile size ${tileCols}x$tileRows, but was ${band.cols}x${band.rows}")
      }
      val converted = if (band.cellType == cellType) band else band.convert(cellType)
      writeChunk(Paths.get(path, arrayNames(b)), chunkIndex, encodeTile(converted, zarrType))
    }
  }

  private def getGroupName(path:String):String = {
    val split = path.split("/").last
    split.substring(0,split.length-5)
  }

  private def checkBandNames(bandNames: Array[String]):Unit = {
    for (bandName <- bandNames){
      if (bandName == "Undefined") throw new IllegalArgumentException("save_result zarr: Band names are not defined")
      if (bandName == "") throw new IllegalArgumentException("save_result zarr: Band names cannot be empty")
      for (character <- bandName) {
        if (!(character.isLetterOrDigit || character=='-' || character == '_'))
          throw new IllegalArgumentException(s"save_result zarr: Band names can only contain a-z, A-Z, 0-9, - or _, but had character $character")
      }
    }
  }
}

