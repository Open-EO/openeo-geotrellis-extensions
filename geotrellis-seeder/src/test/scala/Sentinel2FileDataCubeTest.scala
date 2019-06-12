import java.net.URI
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import geotrellis.contrib.vlm.{LayoutTileSource, RasterSource}
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.TiffTagsReader
import geotrellis.raster.{MultibandTile, Raster, ShortUserDefinedNoDataCellType, Tile}
import geotrellis.spark._
import geotrellis.spark.io.hadoop.geotiff.{CollectionAttributeStore, GeoTiffMetadata, InMemoryGeoTiffAttributeStore}
import geotrellis.spark.io.hadoop.{HdfsRangeReader, HdfsUtils}
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.Test

object Sentinel2FileDataCubeTest {

  private def overlappingFiles(at: ZonedDateTime, bbox: ProjectedExtent): Iterable[URI] = {
    // e.g. with an AttributeStore

    Seq("file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B01_60M_V102.tif")
      .map(URI.create)
  }

  private def correspondingBandFiles(file: URI, bandIds: Seq[String]): Seq[URI] = {
    // e.g. replace placeholder with specific bands in file name

    val geoTiff10m = "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B02_10M_V102.tif"
    val geoTiff20m = "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B8A_20M_V102.tif"
    val geoTiff60m = "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B01_60M_V102.tif"

    Seq(geoTiff10m, geoTiff20m, geoTiff60m)
      .map(URI.create)

    /*val allBands = Seq(
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B01_60M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B02_10M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B03_10M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B04_10M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B05_20M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B06_20M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B07_20M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B08_10M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B11_20M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B12_20M_V102.tif",
      "file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/03/25/S2B_20190325T105709Z_31UDS_CGS_V102_000/S2B_20190325T105709Z_31UDS_TOC_V102/S2B_20190325T105709Z_31UDS_TOC-B8A_20M_V102.tif"
    )

    allBands
      .map(URI.create)*/
  }
}

class Sentinel2FileDataCubeTest {
  import Sentinel2FileDataCubeTest._

  object FileIMGeoTiffAttributeStore {
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

  @Test
  def test(): Unit = {
    val bbox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
    val from = ZonedDateTime.of(LocalDate.of(2019, 3, 25), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val to = from

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    implicit val sc =
      SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val rasterSourceRDD = tileLayerRdd(bbox, from, to).cache()
      val tileCount = rasterSourceRDD.count()

      println(s"got $tileCount tiles")

      rasterSourceRDD.keys
        .map(_.time)
        .distinct()
        .collect()
        .sortWith(_ isBefore _)
        .foreach(println)

      val Raster(multibandTile, extent) = rasterSourceRDD
          .toSpatial(from)
          .crop(bbox.reproject(rasterSourceRDD.metadata.crs))
          .stitch()

      MultibandGeoTiff(multibandTile, extent, rasterSourceRDD.metadata.crs).write("/tmp/stitched.geotiff")
    } finally {
      sc.stop()
    }
  }

  private def geoTiffBandUriTemplates(at: ZonedDateTime, bbox: ProjectedExtent): Seq[String] = {
    val (year, month, day) = ("2019", "03", "25")

    val arbitraryBand = "TOC-B01"
    val arbitraryBandGlob = new Path(s"file:/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/$year/$month/$day/*/*/*_${arbitraryBand}_*_V102.tif")

    val attributeStore = FileIMGeoTiffAttributeStore("???", arbitraryBandGlob)

    attributeStore.query("???", bbox)
      .map(_.uri.toString.replace(arbitraryBand, "!!!"))
  }

  private def sequentialDates(from: ZonedDateTime): Stream[ZonedDateTime] = from #:: sequentialDates(from plusDays 1)

  private def tileLayerRdd(bbox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime)(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] = {
    val bandIds = Seq("2", "8A", "1")

    val dates = sequentialDates(from)
      .takeWhile(date => !(date isAfter to))

    val targetCrs: CRS = WebMercator
    val reprojectedBoundingBox = ProjectedExtent(bbox.reproject(targetCrs), targetCrs)

    val layout = ZoomedLayoutScheme(targetCrs).levelForZoom(targetCrs.worldExtent, 14).layout

    val overlappingFilesPerDay: RDD[(ZonedDateTime, Iterable[URI])] = sc.parallelize(dates, dates.size)
      .map(date => (date, overlappingFiles(date, reprojectedBoundingBox)))

    val overlappingTilesPerDay: RDD[(ZonedDateTime, Iterable[(SpatialKey, MultibandTile)])] = overlappingFilesPerDay.map { case (date, overlappingFiles) =>
      val overlappingMultibandTiles: Iterable[(SpatialKey, MultibandTile)] = overlappingFiles.flatMap(overlappingFile => {
        val bandTileSources: Seq[LayoutTileSource] = correspondingBandFiles(overlappingFile, bandIds)
          .map(bandFile => GeoTiffRasterSource(bandFile.toString).reproject(targetCrs).tileToLayout(layout))

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

    /*val dailyAttributeStores: RDD[(ZonedDateTime, CollectionAttributeStore[GeoTiffMetadata])] = sc.parallelize(dates, dates.size)
      .map(date => (date, FileIMGeoTiffAttributeStore(date.toString, new Path("/path/to/year/month/day/.../TOC-B01.tif")))) // arbitrary, just used to get files by bbox

    val dailyOverlappingGeoTiffs: RDD[(ZonedDateTime, Seq[URI])] = dailyAttributeStores
      .mapValues(attributeStore => attributeStore.query(reprojectedBoundingBox).map(_.uri))

    val dailyOverlappingBandGeoTiffs: RDD[(ZonedDateTime, Seq[Seq[URI]])] = dailyOverlappingGeoTiffs
      .mapValues(geoTiffs => geoTiffs.map(geoTiff => Seq(geoTiff))) // FIXME: demultiplex arbitrary GeoTiff to band GeoTiffs (bandIds)

    val dailyOverlappingBandTileSources: RDD[(ZonedDateTime, Seq[Seq[LayoutTileSource]])] = dailyOverlappingBandGeoTiffs
      .mapValues(overlappingGeoTiffs => overlappingGeoTiffs.map(bandGeoTiffs => bandGeoTiffs.map(bandGeoTiff =>
        GeoTiffRasterSource(bandGeoTiff.toString).reproject(targetCrs).tileToLayout(layout))))

    val overlappingKeys = layout.mapTransform.keysForGeometry(reprojectedBoundingBox.extent.toPolygon()).toIterator

    val dailyOverlappingMultibandTiles = RDD[(ZonedDateTime, Seq[Seq[Iterator[(SpatialKey, MultibandTile)]]])] = dailyOverlappingBandTileSources
        .mapValues(overlappingGeoTiffs => overlappingGeoTiffs.map(bandTileSources => {
          val tiles = bandTileSources.zipWithIndex.flatMap { case (bandTileSource, index) =>
            bandTileSource.readAll(overlappingKeys).map { case (key, multibandTile) => ((key, index), multibandTile.band(0)) }
          }

          val multibandTiles = tiles
            .groupBy { case ((key, index), tile) => key }

          ???
        }))*/

    val tiles: RDD[(SpaceTimeKey, MultibandTile)] = overlappingTilesPerDay.flatMap { case (date, tiles) =>
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

    ContextRDD(tiles, metadata)
  }
}
