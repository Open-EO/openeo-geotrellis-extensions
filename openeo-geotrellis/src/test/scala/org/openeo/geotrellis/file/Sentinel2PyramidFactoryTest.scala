package org.openeo.geotrellis.file

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZonedDateTime}
import java.util.Collections.singletonList
import java.util.zip.Deflater.BEST_COMPRESSION

import geotrellis.proj4.CRS
import geotrellis.raster.Raster
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.summary.polygonal.Summary
import geotrellis.raster.summary.polygonal.visitors.MeanVisitor
import geotrellis.spark._
import geotrellis.spark.summary.polygonal._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, BeforeClass, Test}

object Sentinel2PyramidFactoryTest {
    private var sc: SparkContext = _

    @BeforeClass
    def setupSpark(): Unit = {
        val sparkConf = new SparkConf()
          .set("spark.kryoserializer.buffer.max", "512m")
          .set("spark.rdd.compress","true")

        sc = SparkUtils.createLocalSparkContext("local[*]", classOf[Sentinel2PyramidFactoryTest].getName, sparkConf)
    }

    @AfterClass
    def tearDownSpark(): Unit = sc.stop()
}

class Sentinel2PyramidFactoryTest {

    @Test
    def pyramid_seq(): Unit = {
        val bbox = ProjectedExtent(Extent(373863.50, 5212258.22, 378241.73, 5216244.73), CRS.fromEpsgCode(32631))
        val date = ZonedDateTime.of(LocalDate.of(2019, 10, 11), MIDNIGHT, UTC)

        val bbox_srs = s"EPSG:${bbox.crs.epsgCode.get}"
        val from_date = DateTimeFormatter.ISO_OFFSET_DATE_TIME format date
        val to_date = from_date

        val (_, baseLayer) = sceneClassificationV200PyramidFactory.pyramid_seq(bbox.extent, bbox_srs, from_date, to_date)
          .maxBy { case (zoom, _) => zoom }

        val spatialLayer = baseLayer
          .toSpatial(date)
          .cache()

        def writeGeoTiff(path: String): Unit = {
            val Raster(tile, extent) = spatialLayer
              .crop(bbox.reproject(spatialLayer.metadata.crs))
              .stitch()

            val options = GeoTiffOptions(DeflateCompression(BEST_COMPRESSION))

            MultibandGeoTiff(tile, extent, spatialLayer.metadata.crs, Tags.empty, options)
              .write(path)
        }

        writeGeoTiff("/tmp/Sentinel2PyramidFactory_cropped_openeo.tif")

        val polygon = bbox.reproject(spatialLayer.metadata.crs).toPolygon()

        val singleBandMean = spatialLayer.polygonalSummaryValue(polygon, MeanVisitor) match {
            case Summary(values) => values.head.mean
        }

        val qgisZonalStaticsPluginResult = 6.04121061930658
        assertEquals(qgisZonalStaticsPluginResult, singleBandMean, 0.1)
    }

    private def sceneClassificationV200PyramidFactory = new Sentinel2PyramidFactory(
        oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_TOC_V2",
        oscarsLinkTitles = singletonList("SCENECLASSIFICATION_20M"),
        rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2"
    )
}
