package org.openeo.geotrellis.file

import java.lang.Math.max
import java.net.URI
import java.time.ZonedDateTime

import geotrellis.layer._
import geotrellis.proj4.{CRS, WebMercator}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.tags.TiffTags
import geotrellis.raster.{CellType, MultibandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.hadoop.geotiff.{GeoTiffMetadata, InMemoryGeoTiffAttributeStore}
import geotrellis.store.hadoop.util.{HdfsRangeReader, HdfsUtils}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.OpenEOProcesses
import org.openeo.geotrellis.file.AbstractPyramidFactory._

object AbstractPyramidFactory {
  private val maxZoom = 14

  private[file] object FileIMGeoTiffAttributeStore {
    def apply(name: String, path: Path): InMemoryGeoTiffAttributeStore =
      new InMemoryGeoTiffAttributeStore {
        override val metadataList: List[GeoTiffMetadata] = {
          val conf = new Configuration

          HdfsUtils
            .listFiles(path, conf)
            .map { p =>
              val tiffTags = TiffTags.read(HdfsRangeReader(p, conf))
              GeoTiffMetadata(tiffTags.extent, tiffTags.crs, name, p.toUri)
            }
        }

        override def persist(uri: URI): Unit = throw new UnsupportedOperationException
      }
  }

  private[file] type FilePathTemplate = String => String
}

abstract class AbstractPyramidFactory[B] extends Serializable {
  protected val cellType: CellType

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

  def layer(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, zoom: Int, bands: Seq[B])(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    require(zoom >= 0)
    require(zoom <= maxZoom)

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = boundingBox.reproject(targetCrs)

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, zoom).layout

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val overlappingKeys = layout.mapTransform.keysForGeometry(reprojectedBoundingBox.extent.toPolygon())
    val overlappingFilesPerDay = sc.parallelize(dates, dates.length)
      .cartesian(sc.parallelize(overlappingKeys.toSeq, max(1, overlappingKeys.size / 20)))
      .map { case (date, spatialKey) =>
        (SpaceTimeKey(spatialKey, date), overlappingFilePathTemplates(date, ProjectedExtent(spatialKey.extent(layout), targetCrs))) }
      .filter(_._2.nonEmpty)

    val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox.extent)
    val rddBounds = KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))

    val partitioned = overlappingFilesPerDay.partitionBy(SpacePartitioner(rddBounds))
    val tilesRdd: RDD[(SpaceTimeKey, MultibandTile)] = new OpenEOProcesses().applySpacePartitioner(partitioned.flatMap { case (key, overlappingFiles) =>
      val overlappingMultibandTiles: Iterable[MultibandTile] = overlappingFiles.flatMap(overlappingFile => {
        val bandTileSources = correspondingBandFiles(overlappingFile, bands)
          .map(bandFile => GeoTiffRasterSource(bandFile).reproject(targetCrs).tileToLayout(layout))

        val multibandTilesPerFile: Seq[Option[MultibandTile]] = bandTileSources.map(_.read(key.spatialKey))
        val singleTile = multibandTilesPerFile.filter(_.isDefined).foldLeft[Vector[Tile]](Vector[Tile]())(_ ++ _.get.bands)

        if (singleTile.nonEmpty) {
          Some(MultibandTile(singleTile))
        } else {
          Option.empty[MultibandTile]
        }
      })

      overlappingMultibandTiles.reduceOption(_ merge _).map((key, _))
    }, rddBounds)

    val metadata: TileLayerMetadata[SpaceTimeKey] = {
      val gridBounds = layout.mapTransform.extentToBounds(reprojectedBoundingBox.extent)

      TileLayerMetadata(
        cellType = cellType,
        layout = layout,
        extent = reprojectedBoundingBox.extent,
        crs = targetCrs,
        KeyBounds(SpaceTimeKey(gridBounds.colMin, gridBounds.rowMin, from), SpaceTimeKey(gridBounds.colMax, gridBounds.rowMax, to))
      )
    }

    ContextRDD(tilesRdd, metadata)
  }

  def pyramid(boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime, bands: Seq[B])(implicit sc: SparkContext): Pyramid[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
    val layers = for (zoom <- maxZoom to 0 by -1) yield zoom -> layer(boundingBox, from, to, zoom, bands)
    Pyramid(layers.toMap)
  }

  def pyramid_seq(bbox: Extent, bbox_srs: String, from_date: String, to_date: String, band_indices: java.util.List[Int]): Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()

    val projectedExtent = ProjectedExtent(bbox, CRS.fromName(bbox_srs))
    val from = ZonedDateTime.parse(from_date)
    val to = ZonedDateTime.parse(to_date)

    val bands: Seq[B] = bandsFromIndices(band_indices)

    pyramid(projectedExtent, from, to, bands).levels.toSeq
      .sortBy { case (zoom, _) => zoom }
      .reverse
  }

  protected def bandsFromIndices(band_indices: java.util.List[Int]): Seq[B]

  protected def overlappingFilePathTemplates(at: ZonedDateTime, bbox: ProjectedExtent): Iterable[FilePathTemplate]

  protected def correspondingBandFiles(pathTemplate: FilePathTemplate, bands: Seq[B]): Seq[String]
}
