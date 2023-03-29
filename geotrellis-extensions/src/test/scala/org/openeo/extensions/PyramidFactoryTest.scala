package org.openeo.extensions

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.SparkContext
import org.junit.Test
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.geotrellissentinelhub.{PyramidFactory, SampleType}

import java.util.Collections
import scala.collection.JavaConverters._

class PyramidFactoryTest {

  @Test
  def sentinalHubSmallAreaToTiff(): Unit = {
    val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)
    try {
      val pyramidFactory = PyramidFactory.withoutGuardedRateLimiting(
        endpoint = "https://services.sentinel-hub.com",
        collectionId = "sentinel-2-l2a",
        datasetId = "sentinel-2-l2a",
        clientId = null,
        clientSecret = null,
        zookeeperConnectionString = "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
        zookeeperAccessTokenPath = "/openeo/rlguard/access_token_default",
        processingOptions = Collections.emptyMap[String, Any],
        sampleType = SampleType.UINT16,
        maxSpatialResolution = CellSize(10.0, 10.0),
        maxSoftErrorsRatio = 0.0,
      )

      val extent = ProjectedExtent(Extent(5.071, 51.21, 5.1028, 51.23), CRS.fromEpsgCode(4326))

      val dataCubeParameters = new DataCubeParameters()
      dataCubeParameters.tileSize = 256
      dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
      dataCubeParameters.globalExtent = Some(extent)

      val crs = CRS.fromEpsgCode(32631)
      val polygons = MultiPolygon(extent.reproject(crs))
      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        polygons = Array(polygons),
        polygons_crs = crs,
        from_date = "2022-09-13T00:00:00+00:00",
        to_date = "2022-09-20T00:00:00+00:00",
        band_names = Seq("B02", "B03").asJava,
        metadata_properties = Collections.emptyMap(),
        dataCubeParameters = dataCubeParameters,
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val paths = org.openeo.geotrellis.geotiff.saveRDD(
        rdd = spatialLayer,
        bandCount = 2,
        path = "/tmp/sentinalHubSmallAreaToTiff.gtiff",
        cropBounds = Some(Extent(644594.8230399278, 5675216.271413178, 646878.5028127492, 5677503.191395153)),
      )

      val actualRaster = GeoTiffRasterSource(paths.get(0)).read().get
      assert(actualRaster.extent.equalsExact(Extent(644590.0, 5675210.0, 646880.0, 5677510.0), 0.1))
      val arr = actualRaster.tile.bands(0).toArray().slice(300, 400) // a range somewhere after the first row
      assert(arr.exists(_ != -2147483648))
    } finally sc.stop()
  }
}
