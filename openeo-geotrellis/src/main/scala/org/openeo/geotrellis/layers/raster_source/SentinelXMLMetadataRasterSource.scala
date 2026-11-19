package org.openeo.geotrellis.layers.raster_source

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.{Extent, ProjectedExtent}
import geotrellis.store.s3.AmazonS3URI
import org.openeo.geotrellis.creo.CreoS3Utils
import org.openeo.geotrellis.layers.raster_source.SentinelXMLMetadataRasterSource.logger
import org.openeo.opensearch.withRetries
import org.slf4j.LoggerFactory
import org.xml.sax.SAXParseException

import java.io.{FileInputStream, FileNotFoundException, InputStream}
import java.net.URI
import java.nio.file.Paths
import java.util.Locale
import scala.language.postfixOps
import scala.util.Try
import scala.xml.{Node, XML}

object SentinelXMLMetadataRasterSource {
  private val logger = LoggerFactory.getLogger(SentinelXMLMetadataRasterSource.getClass)

  /**
   * Opens a metadata file given as https://, s3://bucket/key, /eodata/key (optionally /vsis3/ prefixed) or local path.
   * Can return null if the file does not exist.
   */
  def loadMetadata(pathArg: String): InputStream = withRetries {
    val path = pathArg.replace("/vsis3/", "/")
    if (path.startsWith("https://")) {
      val uri = new URI(path)
      uri.resolve(uri.getPath).toURL
        .openConnection.asInstanceOf[java.net.HttpURLConnection]
        .getInputStream
    } else if (path.startsWith("s3://")) {
      CreoS3Utils.readFromS3(new AmazonS3URI(path))
    } else if (path.startsWith("/eodata/")) {
      if (sys.env.contains("AWS_S3_ENDPOINT")) {
        CreoS3Utils.readFromS3(new AmazonS3URI("s3:/" + path))
      } else {
        val uri = new URI(path.replace("/eodata", "https://zipper.creodias.eu/get-object?path="))
        try {
          uri.resolve(uri.toString).toURL.openConnection.getInputStream
        } catch {
          case e: FileNotFoundException =>
            logger.warn(e.toString)
            null
        }
      }
    } else {
      new FileInputStream(Paths.get(path).toFile)
    }
  }

  def forAngleBand(xlmPath: String,
                   angleBandIndex: Int,
                   targetProjectedExtent: Option[ProjectedExtent] = None,
                   cellSize: Option[CellSize] = None,
                  ): SentinelXMLMetadataRasterSource = {
    // TODO: write forAngleBands in terms of forAngleBand instead
    forAngleBands(xlmPath, Seq(angleBandIndex), targetProjectedExtent, cellSize).head
  }

  /**
   * Returns SAA,SZA,VAA,VZA selected by the bands argument.
   */
  def forAngleBands(xlmPath: String,
                    angleBandIndices: Seq[Int] = Seq(0, 1, 2, 3), // SAA, SZA, VAA, VZA
                    targetProjectedExtent: Option[ProjectedExtent] = None,
                    cellSize: Option[CellSize] = None,
                   ): Seq[SentinelXMLMetadataRasterSource] = {
    require(angleBandIndices.forall(index => index >= 0 && index <= 3))

    val theResolution = cellSize.getOrElse(CellSize(10, 10))

    val path = loadMetadata(xlmPath)
    if (path == null) throw new FileNotFoundException(s"metadata file for angle bands $xlmPath does not exist")

    try {
      val xmlDoc = XML.load(path)
      val angles = xmlDoc \\ "Tile_Angles"
      val meanSun = angles \ "Mean_Sun_Angle"
      val mSZA = ( meanSun \ "ZENITH_ANGLE").text.toFloat
      val mSAA = ( meanSun \ "AZIMUTH_ANGLE").text.toFloat
      val meanViewing = angles \ "Mean_Viewing_Incidence_Angle_List" \ "Mean_Viewing_Incidence_Angle" filter (va=>(va \ "@bandId" toString) == "4")
      val mVZA = (meanViewing \ "ZENITH_ANGLE").text.toFloat
      val mVAA = (meanViewing \ "AZIMUTH_ANGLE").text.toFloat

      val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
      val crs = CRS.fromName((geoCoding\"HORIZONTAL_CS_CODE").text)
      val position = geoCoding \ "Geoposition"  filter (va=>(va \ "@resolution" toString) == "10")
      val ulx = (position \ "ULX").text.toDouble
      val uly = (position \ "ULY").text.toDouble
      // TODO: Looking at the cloud locations in the accompanying cloud mask file
      val maxExtent = Extent(ulx, uly - (theResolution.width * 10980), ulx + (theResolution.height * 10980), uly)
      val currentPe = ProjectedExtent(maxExtent, crs)
      val projectedExtent = targetProjectedExtent match {
        case None => currentPe
        case Some(pe) =>
          //val currentPeReproject = currentPe.reproject(pe.crs)
          //assert(currentPeReproject.contains(pe.extent)) // This assert fails
          pe
      }
      val gridExtent = GridExtent[Long](projectedExtent.extent, theResolution)

      val constantAngleBandValues = Seq(mSAA, mSZA, mVAA, mVZA)

      for {
        index <- angleBandIndices
        angleBandValue = constantAngleBandValues(index)
      } yield new SentinelXMLMetadataRasterSource(angleBandValue, projectedExtent.crs, gridExtent, OpenEoSourcePath(xlmPath + s"_${(angleBandValue*1000.0).toInt}"))
    } catch {
      case e@(_: SAXParseException | _: IllegalArgumentException) =>
        throw new IllegalArgumentException(s"invalid Sentinel-2 tile metadata $xlmPath: ${e.getMessage}", e)
    }
  }

  // Tile metadata for per-pixel sun and viewing angles
  // (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/916).

  /** Resolutions, in metres, whose tile geometry is read from the metadata. */
  val tileResolutions: Seq[Int] = Seq(10, 20, 60)

  /**
   * Zenith and azimuth angles in degrees on a regular grid of nodes, stored row-major. NaN marks a missing value.
   * Node (0, 0) lies on the tile's upper-left corner and nodes are `colStep`/`rowStep` metres apart.
   */
  final class AngleGrid(val rows: Int,
                        val cols: Int,
                        val colStep: Double,
                        val rowStep: Double,
                        val zenith: Array[Double],
                        val azimuth: Array[Double]) extends Serializable {
    require(rows > 0 && cols > 0, s"empty angle grid: $rows x $cols")
    require(zenith.length == rows * cols && azimuth.length == rows * cols, "angle grid values do not match its shape")

    def zenithAt(row: Int, col: Int): Double = zenith(row * cols + col)

    def azimuthAt(row: Int, col: Int): Double = azimuth(row * cols + col)

    /** A node is valid only if both its zenith and its azimuth are present. */
    def isValid(row: Int, col: Int): Boolean = !zenithAt(row, col).isNaN && !azimuthAt(row, col).isNaN
  }

  sealed trait DetectorMaskFormat

  object DetectorMaskFormat {
    case object Jp2 extends DetectorMaskFormat

    case object Gml extends DetectorMaskFormat
  }

  /** An `MSK_DETFOO` reference. `path` is relative to the SAFE root. */
  final case class DetectorMaskRef(path: String, format: DetectorMaskFormat)

  /** What one band's per-pixel viewing angles need: its grids by detectorId, and its detector mask. */
  final case class BandViewing(grids: Map[Int, AngleGrid], detectorMask: DetectorMaskRef)

  /**
   * The parts of a Sentinel-2 L1C or L2A tile metadata file (MTD_TL.xml) that per-pixel angles need.
   * Band IDs are 0-based (B01 = 0, B08 = 7, B8A = 8, B12 = 12).
   *
   * The tile identity and geometry are needed by every output, so problems there fail the whole parse. Problems in
   * the sun grid only affect the sun outputs, and problems in a band's viewing grids or detector mask only affect
   * that band's viewing outputs. Such problems are kept as `Left(message)` for the caller to report.
   *
   * @param level              "L1C" or "L2A"
   * @param processingBaseline for example "04.00", from the `_Nxx.yy` suffix of `tileId`
   * @param extent             the tile extent in `crs`, the same at every resolution
   * @param gridExtents        the tile's pixel grid at each of [[tileResolutions]]
   * @param sunGrid            the sun angle grid, or why it is unusable
   * @param viewing            for each requested bandId, its viewing grids and detector mask, or why they are
   *                           unusable
   */
  final case class TileMetadata(level: String,
                                tileId: String,
                                processingBaseline: Option[String],
                                crs: CRS,
                                extent: Extent,
                                gridExtents: Map[Int, GridExtent[Long]],
                                sunGrid: Either[String, AngleGrid],
                                viewing: Map[Int, Either[String, BandViewing]])

  /**
   * Loads and parses tile metadata from any path that [[loadMetadata]] accepts; see [[parseTileMetadata]].
   * Throws FileNotFoundException if the file does not exist, IOException if it cannot be read, and
   * IllegalArgumentException if its XML syntax, tile identity or geometry is invalid.
   */
  def loadTileMetadata(xmlPath: String, viewBandIds: Set[Int]): TileMetadata = {
    val stream = loadMetadata(xmlPath)
    if (stream == null) throw new FileNotFoundException(s"tile metadata $xmlPath does not exist")
    try {
      val xml = try XML.load(stream) finally stream.close()
      parseTileMetadata(xml, viewBandIds)
    } catch {
      case e@(_: SAXParseException | _: IllegalArgumentException) =>
        throw new IllegalArgumentException(s"invalid Sentinel-2 tile metadata $xmlPath: ${e.getMessage}", e)
    }
  }

  /**
   * Parses a `Level-1C_Tile_ID` or `Level-2A_Tile_ID` element, whatever its namespace prefix. Viewing grids and
   * detector masks are only parsed for `viewBandIds`. Throws IllegalArgumentException if the tile identity or
   * geometry is invalid; other problems are returned in [[TileMetadata]].
   */
  def parseTileMetadata(root: Node, viewBandIds: Set[Int]): TileMetadata = {
    val level = root.label match {
      case "Level-1C_Tile_ID" => "L1C"
      case "Level-2A_Tile_ID" => "L2A"
      case other => invalid(s"unexpected root element <$other>, expected Level-1C_Tile_ID or Level-2A_Tile_ID")
    }

    val tileId = text(single(root, "General_Info"), "TILE_ID")
    val processingBaseline = ProcessingBaselineSuffix.findFirstMatchIn(tileId).map(_.group(1))

    val geometricInfo = single(root, "Geometric_Info")
    val (crs, extent, gridExtents) = parseTileGeometry(single(geometricInfo, "Tile_Geocoding"))

    val angles = attempt(single(geometricInfo, "Tile_Angles"))
    val sunGrid = angles.flatMap(tileAngles =>
      attempt(parseAngleGrid(single(tileAngles, "Sun_Angles_Grid"), "Sun_Angles_Grid")))
    val detectorMasks = (root \ "Quality_Indicators_Info" \ "Pixel_Level_QI" \ "MASK_FILENAME")
      .filter(mask => (mask \@ "type") == "MSK_DETFOO")
    val viewing = viewBandIds.map(bandId => bandId -> angles.flatMap { tileAngles =>
      attempt(parseBandViewing(tileAngles \ "Viewing_Incidence_Angles_Grids", detectorMasks, bandId))
    }).toMap

    TileMetadata(level, tileId, processingBaseline, crs, extent, gridExtents, sunGrid, viewing)
  }

  /**
   * Parses one band's viewing grids and detector mask. Grids and masks whose bandId cannot be read might belong to
   * this band, so they make it unusable too.
   */
  private def parseBandViewing(grids: Seq[Node], masks: Seq[Node], bandId: Int): BandViewing = {
    def ofBand(nodes: Seq[Node], what: String): Seq[Node] = {
      for (node <- nodes if (node \@ "bandId").toIntOption.isEmpty)
        invalid(s"$what has an invalid bandId attribute: '${node \@ "bandId"}'")
      nodes.filter(node => (node \@ "bandId").toInt == bandId)
    }

    val bandGrids = ofBand(grids, "Viewing_Incidence_Angles_Grids").map { grid =>
      val detectorId = intAttribute(grid, "detectorId")
      detectorId -> parseAngleGrid(grid, s"Viewing_Incidence_Angles_Grids bandId=$bandId detectorId=$detectorId")
    }
    if (bandGrids.isEmpty) invalid(s"no Viewing_Incidence_Angles_Grids for bandId=$bandId")
    for ((detectorId, duplicates) <- bandGrids.groupBy(_._1) if duplicates.size > 1)
      invalid(s"${duplicates.size} Viewing_Incidence_Angles_Grids for bandId=$bandId detectorId=$detectorId")

    val detectorMask = ofBand(masks, "MSK_DETFOO MASK_FILENAME") match {
      case Seq(mask) =>
        val path = mask.text.trim
        val lowerCasePath = path.toLowerCase(Locale.ROOT)
        if (lowerCasePath.endsWith(".jp2")) DetectorMaskRef(path, DetectorMaskFormat.Jp2)
        else if (lowerCasePath.endsWith(".gml")) DetectorMaskRef(path, DetectorMaskFormat.Gml)
        else invalid(s"MSK_DETFOO bandId=$bandId: unsupported mask file '$path', expected .jp2 or .gml")
      case Seq() => invalid(s"no MSK_DETFOO MASK_FILENAME for bandId=$bandId")
      case several => invalid(s"${several.size} MSK_DETFOO MASK_FILENAME entries for bandId=$bandId")
    }

    BandViewing(bandGrids.toMap, detectorMask)
  }

  private def attempt[T](parse: => T): Either[String, T] =
    try Right(parse)
    catch {
      case e: IllegalArgumentException => Left(e.getMessage)
    }

  private val ProcessingBaselineSuffix = """_N(\d{2}\.\d{2})$""".r

  private def invalid(message: String): Nothing = throw new IllegalArgumentException(message)

  private def single(parent: Node, label: String): Node = parent \ label match {
    case Seq(node) => node
    case nodes => invalid(s"expected one <$label> in <${parent.label}>, found ${nodes.size}")
  }

  private def text(parent: Node, label: String): String = {
    val value = single(parent, label).text.trim
    if (value.isEmpty) invalid(s"<$label> in <${parent.label}> is empty")
    value
  }

  private def finiteNumber(parent: Node, label: String): Double = {
    val value = text(parent, label)
    value.toDoubleOption.filter(v => !v.isNaN && !v.isInfinite)
      .getOrElse(invalid(s"<$label> in <${parent.label}> is not a finite number: '$value'"))
  }

  private def positiveInt(parent: Node, label: String): Int = {
    val value = text(parent, label)
    value.toIntOption.filter(_ > 0)
      .getOrElse(invalid(s"<$label> in <${parent.label}> is not a positive integer: '$value'"))
  }

  private def intAttribute(node: Node, name: String): Int = {
    val value = node \@ name
    value.toIntOption.getOrElse(invalid(s"<${node.label}> has an invalid $name attribute: '$value'"))
  }

  private def uniqueMap[K, V](entries: Seq[(K, V)], what: String): Map[K, V] = {
    val duplicates = entries.groupBy(_._1).collect { case (key, values) if values.size > 1 => key }
    if (duplicates.nonEmpty) invalid(s"duplicate $what for ${duplicates.mkString(", ")}")
    entries.toMap
  }

  /** Returns the CRS, the tile extent and the pixel grid at each of [[tileResolutions]]. */
  private def parseTileGeometry(geocoding: Node): (CRS, Extent, Map[Int, GridExtent[Long]]) = {
    val crsCode = text(geocoding, "HORIZONTAL_CS_CODE")
    val crs = Try(CRS.fromName(crsCode)).getOrElse(invalid(s"unsupported HORIZONTAL_CS_CODE '$crsCode'"))

    def byResolution(label: String): Map[Int, Node] =
      uniqueMap((geocoding \ label).map(node => intAttribute(node, "resolution") -> node), s"<$label>")

    val sizes = byResolution("Size")
    val positions = byResolution("Geoposition")

    val extents = tileResolutions.map { resolution =>
      val size = sizes.getOrElse(resolution, invalid(s"missing <Size resolution=\"$resolution\">"))
      val position = positions.getOrElse(resolution, invalid(s"missing <Geoposition resolution=\"$resolution\">"))
      val (xDim, yDim) = (finiteNumber(position, "XDIM"), finiteNumber(position, "YDIM"))
      if (xDim != resolution || yDim != -resolution)
        invalid(s"the $resolution m geometry is not north-up with $resolution m pixels: XDIM=$xDim, YDIM=$yDim")
      val (ulx, uly) = (finiteNumber(position, "ULX"), finiteNumber(position, "ULY"))
      val (rows, cols) = (positiveInt(size, "NROWS"), positiveInt(size, "NCOLS"))
      resolution -> Extent(ulx, uly - rows.toDouble * resolution, ulx + cols.toDouble * resolution, uly)
    }

    val (firstResolution, extent) = extents.head
    for ((resolution, other) <- extents if other != extent)
      invalid(s"the $resolution m geometry $other does not match the $firstResolution m geometry $extent")

    val gridExtents = tileResolutions.map(r => r -> GridExtent[Long](extent, CellSize(r, r))).toMap
    (crs, extent, gridExtents)
  }

  /** Parses a grid with `Zenith` and `Azimuth` children, as in `Sun_Angles_Grid`. */
  private def parseAngleGrid(grid: Node, context: String): AngleGrid = {
    val (zenithSteps, zenithRows) = parseAngleValues(single(grid, "Zenith"), s"$context Zenith")
    val (azimuthSteps, azimuthRows) = parseAngleValues(single(grid, "Azimuth"), s"$context Azimuth")
    if (zenithSteps != azimuthSteps)
      invalid(s"$context: zenith steps $zenithSteps differ from azimuth steps $azimuthSteps")

    val (rows, cols) = (zenithRows.length, zenithRows.head.length)
    if (azimuthRows.length != rows || azimuthRows.head.length != cols)
      invalid(s"$context: the zenith grid is $rows x $cols but the azimuth grid is " +
        s"${azimuthRows.length} x ${azimuthRows.head.length}")

    val (colStep, rowStep) = zenithSteps
    new AngleGrid(rows, cols, colStep, rowStep, zenithRows.flatten, azimuthRows.flatten)
  }

  /** Returns ((COL_STEP, ROW_STEP), rows of values) of a `Zenith` or `Azimuth` element. */
  private def parseAngleValues(component: Node, context: String): ((Double, Double), Array[Array[Double]]) = {
    val steps = (gridStep(component, "COL_STEP", context), gridStep(component, "ROW_STEP", context))
    val rows = (single(component, "Values_List") \ "VALUES")
      .map(row => row.text.trim.split("\\s+").map(token => parseAngle(token, context)))
      .toArray
    if (rows.isEmpty) invalid(s"$context has no VALUES")

    val cols = rows.head.length
    for ((row, index) <- rows.zipWithIndex if row.length != cols)
      invalid(s"$context: row $index has ${row.length} values, expected $cols")
    (steps, rows)
  }

  private def gridStep(component: Node, label: String, context: String): Double = {
    val unit = single(component, label) \@ "unit"
    if (unit.nonEmpty && unit != "m") invalid(s"$context: $label unit is '$unit', expected 'm'")
    val step = finiteNumber(component, label)
    if (step <= 0) invalid(s"$context: $label must be positive, got $step")
    step
  }

  /** Parses an angle value. Any non-finite value, including ESA's `INF`, is missing (NaN). */
  private def parseAngle(token: String, context: String): Double =
    token.toUpperCase(Locale.ROOT) match {
      case "NAN" | "INF" | "+INF" | "-INF" => Double.NaN
      case _ => token.toDoubleOption match {
        case Some(value) if !value.isNaN && !value.isInfinite => value
        case Some(_) => Double.NaN
        case None => invalid(s"$context: malformed value '$token'")
      }
    }
}

case class SentinelXMLMetadataRasterSource(
                                            value: Float, // TODO: rename this to a more generic constant value RasterSource?
                                            crs: CRS,
                                            gridExtent: GridExtent[Long],
                                            sourcePathName: OpenEoSourcePath,
                                          ) extends RasterSource {
  val targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = this

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val gridExtentNew = resampleTarget(gridExtent.reproject(crs, targetCRS))
    new SentinelXMLMetadataRasterSource(value, targetCRS, gridExtentNew, sourcePathName)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    reprojection(crs, resampleTarget, method, strategy)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val supportedBandIndices = Seq(0)
    require(bands == supportedBandIndices)

    extent.intersection(gridExtent.extent)
      .map { intersection =>
        val intersectionGridBounds = gridExtent.gridBoundsFor(intersection).toGridType[Int]
        val tile = FloatConstantTile(value, intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(tile), intersection)
      }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    bounds.intersection(gridExtent.dimensions)
      .map { intersection =>
        val intersectionGridBounds = intersection.toGridType[Int]
        val tile = FloatConstantTile(value, intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(tile), gridExtent.extentFor(intersection))
      }
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    if(targetCellType.cellType != FloatConstantNoDataCellType) {
      logger.warn("Ignoring cellType: " + cellType)
    }
    this.copy()
  }

  override def name: SourceName = sourcePathName

  override def bandCount: Int = 1

  override def cellType: CellType = FloatConstantNoDataCellType

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty[String,String]

  override def attributesForBand(band: Int): Map[String, String] = Map.empty[String,String]
}
