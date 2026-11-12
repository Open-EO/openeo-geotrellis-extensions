package org.openeo.geotrellis.zarr

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import dev.zarr.zarrjava.core.Attributes
import dev.zarr.zarrjava.store.FilesystemStore
import dev.zarr.zarrjava.v3.{Array => ZArray, DataType => ZDataType}
import geotrellis.layer.{Boundable, SpaceTimeKey, SpatialComponent, SpatialKey}
import geotrellis.raster.{BitCellType, ByteCellType, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, NODATA, ShortCellType, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, Tile, UByteCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, shortNODATA, ubyteNODATA, ushortNODATA}
import geotrellis.spark.MultibandTileLayerRDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import scala.reflect.ClassTag

object ZarrWriter {

  private val logger = LoggerFactory.getLogger(ZarrWriter.getClass)

  val ZarrFormat: Int = 3
  val ZstdLevel: Int = 3
  val MetadataFileName: String = "zarr.json"

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

    val metadata = rdd.metadata
    val cellType = metadata.cellType
    val (zarrType: String, fillValue: Option[Number]) = toZarrType(cellType)

    val tileRows = metadata.tileRows
    val tileCols = metadata.tileCols

    val xValues = Array.tabulate(metadata.cols.toInt)(x => metadata.extent.xmin + x * metadata.cellwidth + metadata.cellwidth / 2.0)
    val yValues = Array.tabulate(metadata.rows.toInt)(y => metadata.extent.ymax - y * metadata.cellheight - metadata.cellheight / 2.0)

    val keys = rdd.keys.collect()
    val shapeOri = Array[Int](metadata.rows.toInt, metadata.cols.toInt)
    val chunkOri = Array[Int](tileRows, tileCols)
    val (timeValues: Map[Long,Int],shape,chunk,hasTemp) = keys match {
      case m: Array[SpaceTimeKey] =>
        val dist = m.map(_.temporalKey.instant).distinct.sorted
        writeCoordinate(path, "time", "int64", Long.box(0L), dist)
        (dist.zipWithIndex.toMap, dist.length+:shapeOri, 1+:chunkOri,true)
      case _ => (Map[Long,Int](),shapeOri,chunkOri, false)
    }

    val dataAttributes = new dataAttribute(metadata, zarrOptions, hasTemp)
    // _FillValue lets xarray mask no-data; only set when the cell type defines no-data
    val noDataAttributes: Map[String, Object] =
      fillValue.map(v => Map[String, Object]("_FillValue" -> fillValueJson(zarrType, Some(v)))).getOrElse(Map.empty)
    for (arrayName <- arrayNames) {
      createArray(path, arrayName, shape, chunk, zarrType, fillValueJson(zarrType, fillValue),
        dataAttributes, noDataAttributes)
    }

    writeCoordinate(path, "x", "float64", "NaN", xValues)
    writeCoordinate(path, "y", "float64", "NaN", yValues)

    rdd.foreachPartition { partition =>
      if (partition.hasNext) {
        // Array handles are not serializable: open them once per partition.
        val store = new FilesystemStore(path)
        val arrays = arrayNames.map(name => ZArray.open(store.resolve(name)))
        partition.foreach { case (k, multibandTile) =>
          writeData(multibandTile, k, arrays, tileRows, tileCols, cellType, zarrType, timeValues)
        }
      }
    }

    // Root group last, so its consolidated metadata describes a complete store.
    // zarr-java does not write consolidated metadata, so it is assembled from the node metadata files.
    val nodeNames = arrayNames.toSeq ++ Seq("x", "y") ++ (if (hasTemp) Seq("time") else Seq.empty)
    val consolidated = new java.util.TreeMap[String, Object]()
    nodeNames.foreach { name =>
      consolidated.put(name, objectMapper.readValue(Paths.get(path, name, MetadataFileName).toFile,
        classOf[java.util.LinkedHashMap[String, Object]]))
    }
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

  private def zarrDataType(zarrType: String): ZDataType = ZDataType.valueOf(zarrType.toUpperCase)

  private def groupMetadata(): java.util.LinkedHashMap[String, Object] = {
    val group = new java.util.LinkedHashMap[String, Object]()
    group.put("zarr_format", Int.box(ZarrFormat))
    group.put("node_type", "group")
    group.put("attributes", new java.util.LinkedHashMap[String, Object]())
    group
  }

  /**
   * Creates an array node (writes its zarr.json).
   * Sharding can later be enabled here: use the shard shape as chunk shape and
   * `c.withSharding(innerChunkShape, inner => inner.withBytes("LITTLE").withZstd(ZstdLevel, false))` as codecs.
   */
  private def createArray(path: String, name: String, shape: Array[Int], chunkShape: Array[Int], zarrType: String,
                          fillValue: Object, attributes: ZarrAttributes,
                          extraAttributes: Map[String, Object] = Map.empty): ZArray = {
    val attributeMap = new Attributes()
    attributeMap.putAll(attributes.toMap)
    extraAttributes.foreach { case (k, v) => attributeMap.put(k, v) }
    try {
      val arrayMetadata = ZArray.metadataBuilder()
        .withShape(shape.map(_.toLong): _*)
        .withDataType(zarrDataType(zarrType))
        .withChunkShape(chunkShape: _*)
        .withFillValue(fillValue)
        .withCodecs(c => c.withBytes("LITTLE").withZstd(ZstdLevel, false))
        .withDimensionNames(attributes.dimensionNames: _*)
        .withAttributes(attributeMap)
        .build()
      ZArray.create(new FilesystemStore(path).resolve(name), arrayMetadata, true)
    } catch {
      case t: Throwable =>
        logger.error(s"save_result zarr: Failed to create array $name in $path : ${t.getMessage}", t)
        throw t
    }
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

  /** Writes a 1D coordinate array as a single chunk. `values` must be an Array[Long] or Array[Double]. */
  private def writeCoordinate(path: String, name: String, zarrType: String, fillValue: Object, values: AnyRef): Unit = {
    val length = java.lang.reflect.Array.getLength(values)
    val array = createArray(path, name, Array(length), Array(length), zarrType, fillValue, new variableAttribute(name))
    array.write(Array(0L), ucar.ma2.Array.factory(zarrDataType(zarrType).getMA2DataType, Array(length), values), false)
  }

  /** Raw (no-data agnostic), row-major values of a tile, as a ucar array of the given shape. */
  private def toUcarArray(tile: Tile, zarrType: String, shape: Array[Int]): ucar.ma2.Array = {
    val raw = tile.withNoData(None)
    val storage: AnyRef = zarrType match {
      case "int8" | "uint8" => raw.toArray().map(_.toByte)
      case "int16" | "uint16" => raw.toArray().map(_.toShort)
      case "int32" => raw.toArray()
      case "float32" => raw.toArrayDouble().map(_.toFloat)
      case "float64" => raw.toArrayDouble()
    }
    ucar.ma2.Array.factory(zarrDataType(zarrType).getMA2DataType, shape, storage)
  }

  private def writeData[K](multibandTile: MultibandTile, k: K, arrays: Array[ZArray],
                           tileRows: Int, tileCols: Int,
                           cellType: CellType, zarrType: String,
                           timeValues: Map[Long, Int]): Unit = {
    val (spatialKey, timeIndex) = k match {
      case key: SpatialKey => (key, None)
      case key: SpaceTimeKey => (key.spatialKey, Some(timeValues(key.temporalKey.instant)))
      case other => throw new IllegalArgumentException(s"save_result zarr: unsupported key type ${other.getClass}")
    }
    val bandCount = multibandTile.bandCount
    if (bandCount != arrays.length) {
      throw new Exception(s"save_result zarr: the expected number of band is ${arrays.length}, but was $bandCount")
    }
    val offset: Array[Long] = (timeIndex.map(_.toLong).toSeq ++ Seq(spatialKey.row.toLong * tileRows, spatialKey.col.toLong * tileCols)).toArray
    val shape: Array[Int] = (timeIndex.map(_ => 1).toSeq ++ Seq(tileRows, tileCols)).toArray

    (0 until bandCount).foreach { b =>
      val band = multibandTile.band(b)
      if (band.cols != tileCols || band.rows != tileRows) {
        throw new Exception(s"save_result zarr: expected tile size ${tileCols}x$tileRows, but was ${band.cols}x${band.rows}")
      }
      val converted = if (band.cellType == cellType) band else band.convert(cellType)
      arrays(b).write(offset, toUcarArray(converted, zarrType, shape), false)
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

