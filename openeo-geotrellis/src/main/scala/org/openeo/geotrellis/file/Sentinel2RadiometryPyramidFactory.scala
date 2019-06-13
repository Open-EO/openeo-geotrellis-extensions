package org.openeo.geotrellis.file

import java.net.URI
import java.time.ZonedDateTime

import geotrellis.contrib.vlm.LayoutTileSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.{MultibandTile, ShortUserDefinedNoDataCellType}
import geotrellis.raster.io.geotiff.reader.TiffTagsReader
import geotrellis.spark.io.hadoop.{HdfsRangeReader, HdfsUtils}
import geotrellis.spark.io.hadoop.geotiff.{GeoTiffMetadata, InMemoryGeoTiffAttributeStore}
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Sentinel2RadiometryPyramidFactory {
  sealed abstract class Band(private[file] val id: String)
  case object B01 extends Band("TOC-B01_60M")
  case object B02 extends Band("TOC-B02_10M")
  case object B03 extends Band("TOC-B03_10M")
  case object B04 extends Band("TOC-B04_10M")
  case object B8A extends Band("TOC-B8A_20M")

  private val allBands = Seq(B01, B02, B03, B04, B8A)

  private object FileIMGeoTiffAttributeStore {
    def apply(name: String, path: Path): InMemoryGeoTiffAttributeStore =
      new InMemoryGeoTiffAttributeStore {
        override val metadataList: List[GeoTiffMetadata] = {
          val conf = new Configuration

          HdfsUtils
            .listFiles(path, conf)
            .map { p =>
              val tiffTags = TiffTagsReader.read(HdfsRangeReader(p, conf))
              GeoTiffMetadata(tiffTags.extent, tiffTags.crs, name, p.toUri)
            }
        }

        override def persist(uri: URI): Unit = throw new UnsupportedOperationException
      }
  }

  // you give it a band ID and get a file path back
  private type FilePathTemplate = String => String

  private def overlappingFilePathTemplates(at: ZonedDateTime, bbox: ProjectedExtent): Iterable[FilePathTemplate] = {
    val (year, month, day) = (at.getYear, at.getMonthValue, at.getDayOfMonth)

    val arbitraryBandId = B01.id
    val arbitraryBandGlob = new Path(f"file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/$year/$month%02d/$day%02d/*/*/*_${arbitraryBandId}_V102.tif")

    val attributeStore = FileIMGeoTiffAttributeStore(at.toString, arbitraryBandGlob)

    def pathTemplate(uri: URI): FilePathTemplate = bandId => uri.toString.replace(arbitraryBandId, bandId)

    attributeStore.query(bbox)
      .map(md => pathTemplate(md.uri))
  }

  private def correspondingBandFiles(pathTemplate: FilePathTemplate, bandIds: Seq[String]): Seq[String] =
    bandIds.map(pathTemplate)
}

class Sentinel2RadiometryPyramidFactory {
  import Sentinel2RadiometryPyramidFactory._

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

  def layer(zoom: Int, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[Band] = allBands)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val bandIds = bands.map(_.id)

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val overlappingFilesPerDay: RDD[(ZonedDateTime, Iterable[String => String])] = sc.parallelize(dates, dates.size)
      .map(date => (date, overlappingFilePathTemplates(date, reprojectedBoundingBox)))

    val overlappingTilesPerDay: RDD[(ZonedDateTime, Iterable[(SpatialKey, MultibandTile)])] = overlappingFilesPerDay.map { case (date, overlappingFiles) =>
      val overlappingMultibandTiles: Iterable[(SpatialKey, MultibandTile)] = overlappingFiles.flatMap(overlappingFile => {
        val bandTileSources: Seq[LayoutTileSource] = correspondingBandFiles(overlappingFile, bandIds)
          .map(bandFile => GeoTiffRasterSource(bandFile).reproject(targetCrs).tileToLayout(layout))

        val overlappingKeys = layout.mapTransform.keysForGeometry(reprojectedBoundingBox.extent.toPolygon())

        val multibandTilesPerFile: Seq[Iterator[(SpatialKey, MultibandTile)]] = bandTileSources.map(_.readAll(overlappingKeys.toIterator))

        val tilesCombinedBySpatialKey: Iterator[(SpatialKey, MultibandTile)] = multibandTilesPerFile.reduce((rowA, rowB) => {
          // combine rowA and rowB into one row by keeping the SpatialKey and combining the tiles for each column
          val result: Iterator[(SpatialKey, MultibandTile)] = (rowA zip rowB)
            .map { case ((commonKey, tileA), (_, tileB)) => (commonKey, MultibandTile(tileA.bands ++: tileB.bands)) }

          result
        })

        tilesCombinedBySpatialKey
      })

      (date, overlappingMultibandTiles)
    }

    val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = overlappingTilesPerDay.flatMap { case (date, tiles) =>
      tiles.map { case (SpatialKey(col, row), tile) => (SpaceTimeKey(col = col, row = row, date), tile) }
    }

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox.extent)

      TileLayerMetadata(
        cellType = ShortUserDefinedNoDataCellType(32767),
        layout = layout,
        extent = reprojectedBoundingBox.extent,
        crs = targetCrs,
        KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
      )
    }

    ContextRDD(tilesRdd, metadata)
  }
}
