package org.openeo.geotrellis.file

import java.net.URI
import java.time.ZonedDateTime
import java.util

import geotrellis.raster.{CellType, UByteUserDefinedNoDataCellType}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.openeo.geotrellis.file.AbstractPyramidFactory.{FileIMGeoTiffAttributeStore, FilePathTemplate}
import org.openeo.geotrellis.file.Sentinel1CoherencePyramidFactory.Sentinel1Bands._

import scala.collection.JavaConverters._

object Sentinel1CoherencePyramidFactory {
  object Sentinel1Bands {
    val allBands: Seq[Sentinel1Band] = Seq(VV, VH)

    sealed trait Sentinel1Band
    case object VV extends Sentinel1Band
    case object VH extends Sentinel1Band
  }
}

class Sentinel1CoherencePyramidFactory extends AbstractPyramidFactory[Sentinel1Band] {
  override protected val cellType: CellType = UByteUserDefinedNoDataCellType(0)

  override protected def bandsFromIndices(band_indices: util.List[Int]): Seq[Sentinel1Band] = {
    if (band_indices == null || band_indices.isEmpty) allBands
    else band_indices.asScala.map(allBands(_))
  }

  override protected def overlappingFilePathTemplates(at: ZonedDateTime, bbox: ProjectedExtent): Iterable[FilePathTemplate] = {
    val (year, month, day) = (at.getYear, at.getMonthValue, at.getDayOfMonth)

    val vvGlob = new Path(f"file:/data/MTDA/TERRASCOPE_Sentinel1/SLC_COHERENCE/$year/$month%02d/$day%02d/*/*_VV.tif")

    val attributeStore = FileIMGeoTiffAttributeStore(at.toString, vvGlob)

    def pathTemplate(uri: URI): FilePathTemplate = bandId => uri.toString.replace("_VV.tif", s"_$bandId.tif")

    attributeStore.query(bbox).map(md => pathTemplate(md.uri))
  }

  override protected def correspondingBandFiles(pathTemplate: FilePathTemplate, bands: Seq[Sentinel1Band]): Seq[String] =
    bands.map(_.toString).map(pathTemplate)
}
