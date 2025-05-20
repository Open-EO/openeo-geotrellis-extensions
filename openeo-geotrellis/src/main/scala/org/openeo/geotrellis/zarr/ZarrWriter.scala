package org.openeo.geotrellis.zarr

import com.bc.zarr.ZarrConstants.{FILENAME_DOT_ZARRAY, FILENAME_DOT_ZATTRS, FILENAME_DOT_ZGROUP, ZARR_FORMAT}
import com.bc.zarr.chunk.ChunkReaderWriter
import com.bc.zarr.{CompressorFactory, DataType, ZarrHeader, ZarrUtils}
import com.bc.zarr.storage.FileSystemStore
import geotrellis.layer.{Boundable, SpaceTimeKey, SpatialComponent, SpatialKey}
import geotrellis.raster.{BitCellType, ByteCellType, ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, CellType, DoubleCellType, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, MultibandTile, NODATA, ShortCellType, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, UByteCellType, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType, byteNODATA, doubleNODATA, floatNODATA, shortNODATA, ubyteNODATA, ushortNODATA}
import geotrellis.spark.MultibandTileLayerRDD
import ucar.ma2.Array.factory

import java.io.OutputStreamWriter
import java.nio.ByteOrder
import java.nio.file.Paths
import scala.reflect.ClassTag

object ZarrWriter {

  def saveZarr(rdd:MultibandTileLayerRDD[SpaceTimeKey],path:String, zarrOptions: ZarrOptions):Unit = {
    saveZarrGeneric(rdd,path,zarrOptions)
  }

  def saveZarrSpatial(rdd:MultibandTileLayerRDD[SpatialKey],path:String, zarrOptions: ZarrOptions):Unit = {
    saveZarrGeneric(rdd,path,zarrOptions)
  }

  def saveZarrGeneric[K: SpatialComponent: Boundable : ClassTag](rdd:MultibandTileLayerRDD[K],path:String, zarrOptions: ZarrOptions):Unit= {
    if (zarrOptions.numberBands > 1) {
      checkBandNames(zarrOptions.bandNames)
      for (bandName <- zarrOptions.bandNames){
        val groupPath = path + "/" + bandName
        writeFile(groupPath, FILENAME_DOT_ZGROUP, null)
      }
    } else{
      val groupPath = path + "/" + getGroupName(path)
      writeFile(groupPath, FILENAME_DOT_ZGROUP, null)
    }
    val metadata = rdd.metadata
    val cellType = metadata.cellType
    val (zarrType: DataType,fillValue:Option[Number]) = toZarrType(cellType)

    val tileRows = metadata.tileRows
    val tileCols = metadata.tileCols
    val byteOrder = ByteOrder.BIG_ENDIAN
    val compressor = CompressorFactory.createDefaultCompressor()


    val xValues = for (x <- 0 until metadata.cols.toInt) yield metadata.extent.xmin + x * metadata.cellwidth + metadata.cellwidth / 2.0
    val yValues = for (y <- 0 until metadata.rows.toInt) yield metadata.extent.ymax - y * metadata.cellheight - metadata.cellheight / 2.0
    val keys = rdd.keys.collect()
    val shapeOri = Array[Int](metadata.rows.toInt, metadata.cols.toInt)
    val chunkOri = Array[Int](tileRows, tileCols)
    val (timeValues: Map[Long,Int],shape,chunk,hasTemp) = keys match {
      case m: Array[SpaceTimeKey] =>
        val tempKey = m.map(_.temporalKey.instant)
        val dist = tempKey.distinct
        writeVariables(path,"time",dist)
        (dist.zipWithIndex.toMap, dist.length+:shapeOri, 1+:chunkOri,true)
      case _ => (Map[Long,Int](),shapeOri,chunkOri, false)
    }

    if (zarrOptions.numberBands > 1) {
      for (bandName <- zarrOptions.bandNames){
        val groupPath = path + "/" + bandName
        writeFile(groupPath,FILENAME_DOT_ZATTRS, new dataAttribute(metadata,zarrOptions,hasTemp))
      }
    } else{
      val groupPath = path + "/" + getGroupName(path)
      writeFile(groupPath,FILENAME_DOT_ZATTRS, new dataAttribute(metadata,zarrOptions,hasTemp))
    }
    val zarrHeader = new ZarrHeader(shape, chunk, zarrType.toString, byteOrder, fillValue.getOrElse(0), compressor, ".")
    if (zarrOptions.numberBands > 1) {
      for (bandName <- zarrOptions.bandNames){
        val groupPath = path + "/" + bandName
        writeFile(groupPath, FILENAME_DOT_ZARRAY, zarrHeader)
      }
    } else{
      val groupPath = path + "/" + getGroupName(path)
      writeFile(groupPath, FILENAME_DOT_ZARRAY, zarrHeader)
    }

    writeVariables(path,"x",xValues.toArray)
    writeVariables(path,"y",yValues.toArray)
    writeFile(path,FILENAME_DOT_ZGROUP,null)


    rdd.foreach{ case (k, multibandTileLayer) =>
      writeData(multibandTileLayer,k,path,zarrOptions,tileRows,tileCols,zarrType,fillValue,timeValues)
    }

  }

  private def toZarrType(cellType: CellType): (DataType, Option[Number])= {
    cellType match {
      case BitCellType => (DataType.u1,None)
      case ByteCellType => (DataType.i1,None)
      case UByteCellType => (DataType.u1,None)
      case ShortCellType => (DataType.i2,None)
      case UShortCellType => (DataType.u2,None)
      case IntCellType => (DataType.i4,None)
      case FloatCellType => (DataType.f4,None)
      case DoubleCellType => (DataType.f8,None)
      case ByteConstantNoDataCellType => (DataType.i1,Some(byteNODATA))
      case UByteConstantNoDataCellType => (DataType.u1,Some(ubyteNODATA))
      case ShortConstantNoDataCellType => (DataType.i2,Some(shortNODATA))
      case UShortConstantNoDataCellType => (DataType.u2,Some(ushortNODATA))
      case IntConstantNoDataCellType => (DataType.i4,Some(NODATA))
      case FloatConstantNoDataCellType => (DataType.f4,Some(floatNODATA))
      case DoubleConstantNoDataCellType => (DataType.f8,Some(doubleNODATA))
      case ct: ByteUserDefinedNoDataCellType => (DataType.i1,Some(ct.noDataValue))
      case ct: UByteUserDefinedNoDataCellType => (DataType.u1,Some(ct.widenedNoData.asInt))
      case ct: ShortUserDefinedNoDataCellType => (DataType.i2,Some(ct.noDataValue))
      case ct: UShortUserDefinedNoDataCellType => (DataType.u2,Some(ct.widenedNoData.asInt.toShort))
      case ct: IntUserDefinedNoDataCellType => (DataType.i4,Some(ct.noDataValue))
      case ct: FloatUserDefinedNoDataCellType => (DataType.f4,Some(ct.noDataValue))
      case ct: DoubleUserDefinedNoDataCellType => (DataType.f8,Some(ct.noDataValue))
    }
  }

  private def writeFile(path:String,fileExtension:String,content:Any):Unit = {
    val toWrite = content match {
      case attributes: ZarrAttributes => attributes.toMap
      case zarrHeader: ZarrHeader => zarrHeader
      case null =>
        val varMap = new java.util.HashMap[String,Int]()
        varMap.put(ZARR_FORMAT, 2)
        varMap
    }
    try {
      val os = new FileSystemStore(Paths.get(path)).getOutputStream(fileExtension)
      val writer = new OutputStreamWriter(os)
      try ZarrUtils.toJson(toWrite, writer, true)
      finally {
        if (os != null) os.close()
        if (writer != null) writer.close()
      }
    }
  }

  private def writeVariables[T <: AnyVal](path:String, name: String, value:Array[T]): Unit = {
    val variablePath = path + "/" + name
    writeFile(variablePath, FILENAME_DOT_ZATTRS,new variableAttribute(name))
    val store = new FileSystemStore(Paths.get(variablePath))
    val compressor = CompressorFactory.createDefaultCompressor()
    val byteOrder = ByteOrder.BIG_ENDIAN
    val length = value.length
    val shape = Array[Int](length)
    val dataType = ucar.ma2.DataType.getType(value.getClass.getComponentType, false)
    val source = factory(dataType, shape, value)
    val dataTypeZarr = value match {
      case _:Array[Long] => DataType.i8
      case _:Array[Double] => DataType.f4
    }
    val zarHeader = new ZarrHeader(shape, shape, dataTypeZarr.toString, byteOrder, 0, compressor, ".")
    writeFile(variablePath, FILENAME_DOT_ZARRAY, zarHeader)
    val chunkReaderWriter = ChunkReaderWriter.create(compressor, dataTypeZarr, byteOrder, shape, 0, store)
    chunkReaderWriter.write("0", source)
  }

  private def writeData[K: SpatialComponent: Boundable : ClassTag](multibandTileLayer: MultibandTile, k:K, path:String, options: ZarrOptions, tileRows:Int, tileCols:Int, zarrType:DataType, fillValue:Option[Number], timeValues:Map[Long,Int]): Unit = {
    val spatialKey:SpatialKey = k match {
      case key: SpatialKey => key
      case key: SpaceTimeKey => key.spatialKey
    }
    val bandCount = multibandTileLayer.bandCount
    val compressor = CompressorFactory.createDefaultCompressor()

    val byteOrder = ByteOrder.BIG_ENDIAN

    (0 until bandCount).foreach(nTiles => {
      val band = multibandTileLayer.band(nTiles)
      val dataType = ucar.ma2.DataType.getType(band.toArray().getClass.getComponentType, false)
      val chunkOri = Array[Int](tileRows, tileCols)
      val indexOri = Array(spatialKey.row, spatialKey.col)
      val (chunk,index) = k match {
        case _:SpatialKey => (chunkOri,indexOri)
        case key:SpaceTimeKey =>
          val timeIndex= timeValues(key.temporalKey.instant)
          (1 +: chunkOri, timeIndex +: indexOri)
      }
      val source = factory(dataType, chunk, band.toArray())
      val chunkFilename = ZarrUtils.createChunkFilename(index, ".")
      if (bandCount > 1) {
        if (bandCount!= options.numberBands) throw new Exception(s"the expected number of band is ${options.numberBands}, but was $bandCount")
        val bandName = options.bandNames(nTiles)
        val store = new FileSystemStore(Paths.get(path + "/" + bandName))
        val chunkReaderWriter = ChunkReaderWriter.create(compressor, zarrType, byteOrder, chunk, fillValue.getOrElse(0), store)
        chunkReaderWriter.write(chunkFilename, source)
      } else{
        val store = new FileSystemStore(Paths.get(path + "/" + getGroupName( path)))
        val chunkReaderWriter = ChunkReaderWriter.create(compressor, zarrType, byteOrder, chunk, fillValue.getOrElse(0), store)
        chunkReaderWriter.write(chunkFilename, source)
      }

    })
  }

  private def getGroupName(path:String):String = {
    val split = path.split("/").last
    split.substring(0,split.length-5)
  }

  private def checkBandNames(bandNames: Array[String]):Unit = {
    for (bandName <- bandNames){
      if (bandName == "Undefined") throw new IllegalArgumentException("Band names are not defined")
      if (bandName == "") throw new IllegalArgumentException("Band names cannot be empty")
      for (character <- bandName) {
        if (!(character.isLetterOrDigit || character=='-' || character == '_'))
          throw new IllegalArgumentException(s"Band names can only contain a-z, A-Z, 0-9, - or _, but had character $character")
      }
    }
  }
}

