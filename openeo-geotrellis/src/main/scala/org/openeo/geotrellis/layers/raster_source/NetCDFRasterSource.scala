package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster._
import geotrellis.vector.Extent
import org.slf4j.LoggerFactory
import ucar.ma2.{InvalidRangeException, Range, Section}
import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDatasets

import java.io.IOException
import scala.jdk.CollectionConverters._

object NetCDFRasterSource {
  private val logger = LoggerFactory.getLogger(getClass)

  case class ParsedSource(path: String, variableName: String)

  def fromSource(source: String, targetCellType: Option[TargetCellType] = None): NetCDFRasterSource = {
    val parsed = parseSource(source)
    NetCDFRasterSource(parsed.path, parsed.variableName, targetCellType)
  }

  def parseSource(source: String): ParsedSource = {
    val prefix = "NETCDF:"
    require(source.startsWith(prefix), s"Unsupported NetCDF source syntax: $source")

    val withoutPrefix = source.substring(prefix.length)
    if (withoutPrefix.startsWith("\"")) {
      val closingQuote = withoutPrefix.indexOf('"', 1)
      require(closingQuote > 1 && closingQuote + 1 < withoutPrefix.length && withoutPrefix.charAt(closingQuote + 1) == ':',
        s"Unsupported quoted NetCDF source syntax: $source")
      val path = withoutPrefix.substring(1, closingQuote)
      val variableName = withoutPrefix.substring(closingQuote + 2)
      ParsedSource(path, variableName)
    } else {
      val separator = withoutPrefix.lastIndexOf(':')
      require(separator > 0 && separator < withoutPrefix.length - 1, s"Unsupported NetCDF source syntax: $source")
      val path = withoutPrefix.substring(0, separator)
      val variableName = withoutPrefix.substring(separator + 1)
      ParsedSource(path, variableName)
    }
  }

  private case class MetadataBundle(
    crs: CRS,
    gridExtent: GridExtent[Long],
    cellType: CellType,
    bandCount: Int,
    noDataValue: Option[Double],
    rank: Int
  )
}

case class NetCDFRasterSource(path: String, variableName: String, override val targetCellType: Option[TargetCellType] = None) extends RasterSource {
  import NetCDFRasterSource._

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val baseMetadata: MetadataBundle = loadMetadata()
  private lazy val sourcePath: String = s"""NETCDF:"$path":$variableName"""

  override def metadata: RasterMetadata = this

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    new NetCDFReprojectRasterSource(this, targetCRS, resampleTarget, method, strategy, targetCellType = targetCellType)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    new NetCDFResampleRasterSource(this, resampleTarget, method, strategy, targetCellType)
  }

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
    read(bounds, bands)
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds.intersection(gridExtent.dimensions).map { intersection =>
      val requestedBands = if (bands.isEmpty) (0 until bandCount) else bands
      validateBandIndices(requestedBands)

      val tiles = requestedBands.map(band => readBand(intersection, band))
      Raster(MultibandTile(tiles), gridExtent.extentFor(intersection))
    }
  }

  override def convert(targetCellType: TargetCellType): RasterSource = copy(targetCellType = Some(targetCellType))

  override def name: SourceName = OpenEoSourcePath(sourcePath)

  override def crs: CRS = baseMetadata.crs

  override def bandCount: Int = baseMetadata.bandCount

  override def cellType: CellType = targetCellType.map(_.cellType).getOrElse(baseMetadata.cellType)

  override def gridExtent: GridExtent[Long] = baseMetadata.gridExtent

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map("path" -> path, "variable" -> variableName)

  override def attributesForBand(band: Int): Map[String, String] = attributes + ("band_index" -> band.toString)

  private def validateBandIndices(bands: Seq[Int]): Unit = {
    bands.foreach { band =>
      require(band >= 0 && band < bandCount, s"Invalid band index $band for $sourcePath with $bandCount band(s)")
    }
  }

  private def readBand(bounds: GridBounds[Long], bandIndex: Int): Tile = {
    withDataset { dataset =>
      val variable = requiredVariable(dataset, variableName)
      val section = buildSection(bounds, bandIndex)
      val values = try {
        val raw = variable.read(section)
        if (baseMetadata.rank == 3) raw.reduce(0) else raw
      } catch {
        case e: InvalidRangeException =>
          throw new IOException(s"Invalid read bounds $bounds for $sourcePath", e)
      }

      val width = bounds.width.toInt
      val height = bounds.height.toInt
      val data = Array.ofDim[Double](width * height)
      val index = values.getIndex
      val noDataValue = baseMetadata.noDataValue

      var row = 0
      while (row < height) {
        var col = 0
        while (col < width) {
          val value = values.getDouble(index.set(row, col))
          data(row * width + col) =
            if (noDataValue.exists(nd => java.lang.Double.compare(nd, value) == 0)) Double.NaN else value
          col += 1
        }
        row += 1
      }

      DoubleArrayTile(data, width, height).convert(cellType)
    }
  }

  private def buildSection(bounds: GridBounds[Long], bandIndex: Int): Section = {
    val rowRange = new Range(bounds.rowMin.toInt, bounds.rowMax.toInt)
    val colRange = new Range(bounds.colMin.toInt, bounds.colMax.toInt)
    val ranges =
      if (baseMetadata.rank == 3) Seq(new Range(bandIndex, bandIndex), rowRange, colRange)
      else Seq(rowRange, colRange)
    new Section(ranges.asJava)
  }

  private def loadMetadata(): MetadataBundle = {
    withDataset { dataset =>
      val variable = requiredVariable(dataset, variableName)
      val rank = variable.getRank
      require(rank == 2 || rank == 3, s"Only rank-2 or rank-3 NetCDF variables are supported, got rank $rank for $sourcePath")

      val dimensions = variable.getDimensions.asScala
      val yDimension = dimensions(rank - 2).getShortName
      val xDimension = dimensions(rank - 1).getShortName

      val xValues = readCoordinateVariable(dataset, xDimension)
      val yValues = readCoordinateVariable(dataset, yDimension)

      val xResolution = if (xValues.length > 1) math.abs(xValues(1) - xValues(0)) else 1.0
      val yResolution = if (yValues.length > 1) math.abs(yValues(1) - yValues(0)) else 1.0

      val xmin = xValues.min - xResolution / 2.0
      val xmax = xValues.max + xResolution / 2.0
      val ymin = yValues.min - yResolution / 2.0
      val ymax = yValues.max + yResolution / 2.0
      val gridExtent = GridExtent[Long](Extent(xmin, ymin, xmax, ymax), CellSize(xResolution, yResolution))

      val bandCount = if (rank == 3) dimensions.head.getLength else 1
      val noData = readNumericAttribute(variable, Seq("_FillValue", "missing_value"))
      val cellType = inferCellType(variable, noData)
      val crs = resolveCrs(dataset, variable)

      MetadataBundle(crs, gridExtent, cellType, bandCount, noData, rank)
    }
  }

  private def inferCellType(variable: Variable, noData: Option[Double]): CellType = {
    val unsigned = Option(variable.findAttributeIgnoreCase("_Unsigned")).exists(_.getStringValue.equalsIgnoreCase("true"))

    variable.getDataType match {
      case ucar.ma2.DataType.UBYTE =>
        noData.map(nd => UByteUserDefinedNoDataCellType(nd.toInt.toByte)).getOrElse(UByteCellType)
      case ucar.ma2.DataType.USHORT =>
        noData.map(nd => UShortUserDefinedNoDataCellType(nd.toInt.toShort)).getOrElse(UShortCellType)
      case ucar.ma2.DataType.BYTE =>
        if (unsigned) noData.map(nd => UByteUserDefinedNoDataCellType(nd.toInt.toByte)).getOrElse(UByteCellType)
        else noData.map(nd => ByteUserDefinedNoDataCellType(nd.toByte)).getOrElse(ByteCellType)
      case ucar.ma2.DataType.SHORT =>
        if (unsigned) noData.map(nd => UShortUserDefinedNoDataCellType(nd.toInt.toShort)).getOrElse(UShortCellType)
        else noData.map(nd => ShortUserDefinedNoDataCellType(nd.toShort)).getOrElse(ShortCellType)
      case ucar.ma2.DataType.UINT =>
        IntCellType
      case ucar.ma2.DataType.INT =>
        noData.map(nd => IntUserDefinedNoDataCellType(nd.toInt)).getOrElse(IntCellType)
      case ucar.ma2.DataType.FLOAT =>
        noData.map(nd => FloatUserDefinedNoDataCellType(nd.toFloat)).getOrElse(FloatCellType)
      case ucar.ma2.DataType.DOUBLE =>
        noData.map(nd => DoubleUserDefinedNoDataCellType(nd)).getOrElse(DoubleCellType)
      case _ =>
        logger.warn(s"Unsupported NetCDF datatype ${variable.getDataType} for $sourcePath. Falling back to DoubleConstantNoDataCellType.")
        DoubleConstantNoDataCellType
    }
  }

  private def resolveCrs(dataset: ucar.nc2.dataset.NetcdfDataset, variable: Variable): CRS = {
    val mappedVariableName = Option(variable.findAttributeString("grid_mapping", null))
    val mappedVariable = mappedVariableName.flatMap(name => Option(dataset.findVariable(name)))
    val wkt = mappedVariable.flatMap(v =>
      Option(v.findAttributeString("spatial_ref", null))
        .orElse(Option(v.findAttributeString("crs_wkt", null)))
    )

    wkt.flatMap(CRS.fromWKT).orElse {
      val isGeographic = mappedVariable.exists(v => Option(v.findAttributeString("grid_mapping_name", null)).contains("latitude_longitude"))
      if (isGeographic) Some(LatLng) else None
    }.getOrElse {
      logger.warn(s"No CRS metadata found for $sourcePath, defaulting to EPSG:4326.")
      LatLng
    }
  }

  private def readCoordinateVariable(dataset: ucar.nc2.dataset.NetcdfDataset, name: String): Array[Double] = {
    val variable = requiredVariable(dataset, name)
    val values = variable.read()
    val index = values.getIndex
    val size = variable.getSize.toInt
    val out = Array.ofDim[Double](size)
    var i = 0
    while (i < size) {
      out(i) = values.getDouble(index.set(i))
      i += 1
    }
    out
  }

  private def readNumericAttribute(variable: Variable, names: Seq[String]): Option[Double] = {
    names.view.flatMap { name =>
      Option(variable.findAttributeIgnoreCase(name))
        .flatMap(attr => Option(attr.getNumericValue))
        .map(_.doubleValue())
    }.headOption
  }

  private def requiredVariable(dataset: ucar.nc2.dataset.NetcdfDataset, name: String): Variable =
    Option(dataset.findVariable(name)).getOrElse(
      throw new IllegalArgumentException(s"Variable '$name' not found in $sourcePath")
    )

  private def withDataset[T](f: ucar.nc2.dataset.NetcdfDataset => T): T = {
    val dataset = NetcdfDatasets.openDataset(path)
    try {
      f(dataset)
    } finally {
      dataset.close()
    }
  }
}
