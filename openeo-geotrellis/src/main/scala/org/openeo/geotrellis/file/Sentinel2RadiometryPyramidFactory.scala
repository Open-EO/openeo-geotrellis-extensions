package org.openeo.geotrellis.file

import java.net.URI
import java.time.ZonedDateTime

import geotrellis.contrib.vlm.LayoutTileSource
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.io.geotiff.reader.TiffTagsReader
import geotrellis.raster.{MultibandTile, ShortUserDefinedNoDataCellType, Tile}
import geotrellis.spark.io.hadoop.geotiff.{GeoTiffMetadata, InMemoryGeoTiffAttributeStore}
import geotrellis.spark.io.hadoop.{HdfsRangeReader, HdfsUtils}
import geotrellis.spark.partition.PartitionerIndex.SpaceTimePartitioner
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling._
import geotrellis.spark.{ContextRDD, KeyBounds, MultibandTileLayerRDD, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object Sentinel2RadiometryPyramidFactory {
  private val maxZoom = 14

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

    val arbitraryBandId = Band.B01.fileMarker
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

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int = maxZoom, bands: Seq[Band.Value] = Band.values.toSeq)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val bandFileMarkers = bands.map(_.fileMarker)

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(targetCrs), targetCrs)

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val overlappingKeys: Set[SpatialKey] = layout.mapTransform.keysForGeometry(reprojectedBoundingBox.extent.toPolygon())
    val overlappingFilesPerDay: RDD[(SpaceTimeKey, Iterable[String => String])] = sc.parallelize(dates,dates.length).cartesian(sc.parallelize[SpatialKey](overlappingKeys.toSeq, Math.max(1,overlappingKeys.size / 20)))
      .map({case (date,spatialkey) => (SpaceTimeKey(spatialkey,TemporalKey(date)), overlappingFilePathTemplates(date, reprojectedBoundingBox))})
      .filter(!_._2.isEmpty)
    //val overlappingFilesPerDay: RDD[(ZonedDateTime, Iterable[String => String])] = sc.parallelize(dates, dates.size)

    val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox.extent)
    val rddBounds = KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))

    val partitioned = overlappingFilesPerDay.partitionBy( SpacePartitioner(rddBounds))
    val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = partitioned.map { case (key, overlappingFiles) =>
      val overlappingMultibandTiles: Iterable[MultibandTile] = overlappingFiles.map(overlappingFile => {
        val bandTileSources: Seq[LayoutTileSource] = correspondingBandFiles(overlappingFile, bandFileMarkers)
          .map(bandFile => GeoTiffRasterSource(bandFile).reproject(targetCrs).tileToLayout(layout))

        val multibandTilesPerFile: Seq[Option[MultibandTile]] = bandTileSources.map(_.read(key.spatialKey))

        val singleTile = multibandTilesPerFile.filter(_.isDefined).foldLeft[Vector[Tile]](Vector[Tile]())(_ ++ _.get.bands)

        MultibandTile(singleTile)
      })

      (key, overlappingMultibandTiles.head)
    }.partitionBy( SpacePartitioner(rddBounds))


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

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[Band.Value] = Band.values.toSeq)(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, bands)
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[Band.Value] =
      if (band_indices == null) Band.values.toSeq
      else band_indices.asScala map Band.apply

    pyramid(projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }
}
