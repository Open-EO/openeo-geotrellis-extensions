package org.openeo.geotrellis.file

import java.net.URI
import java.time.ZonedDateTime
import java.util

import geotrellis.raster.{CellType, ShortUserDefinedNoDataCellType}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.openeo.geotrellis.file.AbstractPyramidFactory.{FileIMGeoTiffAttributeStore, FilePathTemplate}
import org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory._

import scala.collection.JavaConverters._

object Sentinel2RadiometryPyramidFactory {
  object Band extends Enumeration {
    // Jesus Christ almighty
    private[file] case class Val(fileMarker: String) extends super.Val
    implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

    val B01 = Val("TOC-B01_60M")
    val B02 = Val("TOC-B02_10M")
    val B03 = Val("TOC-B03_10M")
    val B04 = Val("TOC-B04_10M")
    val B05 = Val("TOC-B05_20M")
    val B06 = Val("TOC-B06_20M")
    val B07 = Val("TOC-B07_20M")
    val B08 = Val("TOC-B08_10M")
    val B11 = Val("TOC-B11_20M")
    val B12 = Val("TOC-B12_20M")
    val B8A = Val("TOC-B8A_20M")
  }
}

/**
 * Deprecated: hardcoded to work for V102, which is phased out.
 */
class Sentinel2RadiometryPyramidFactory extends AbstractPyramidFactory[Band.Value] {
  override protected val cellType: CellType = ShortUserDefinedNoDataCellType(32767)

  override protected def bandsFromIndices(band_indices: util.List[Int]): Seq[Band.Value] = {
    if (band_indices == null) Band.values.toSeq
    else band_indices.asScala map Band.apply
  }

  override protected def overlappingFilePathTemplates(at: ZonedDateTime, bbox: ProjectedExtent): Iterable[FilePathTemplate] = {
    val (year, month, day) = (at.getYear, at.getMonthValue, at.getDayOfMonth)

    val arbitraryBandId = Band.B01.fileMarker
    val arbitraryBandGlob = new Path(f"file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/$year/$month%02d/$day%02d/*/*/*_${arbitraryBandId}_V102.tif")

    val attributeStore = FileIMGeoTiffAttributeStore(at.toString, arbitraryBandGlob)

    def pathTemplate(uri: URI): FilePathTemplate = bandId => uri.toString.replace(arbitraryBandId, bandId)

    attributeStore.query(bbox)
      .map(md => pathTemplate(md.uri))
  }

  override protected def correspondingBandFiles(pathTemplate: FilePathTemplate, bands: Seq[Band.Value]): Seq[String] =
    bands.map(_.fileMarker).map(pathTemplate)
}
