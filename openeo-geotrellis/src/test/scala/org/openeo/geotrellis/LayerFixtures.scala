package org.openeo.geotrellis

import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.{LocalDate, ZonedDateTime}
import java.util.Collections.singletonList

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.LatLng
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.openeo.geotrellis.file.Sentinel2PyramidFactory

object LayerFixtures {

  def ClearNDVILayerForSingleDate()(implicit sc: SparkContext): MultibandTileLayerRDD[SpaceTimeKey] ={
    val factory = new Sentinel2PyramidFactory(
      oscarsCollectionId = "urn:eop:VITO:TERRASCOPE_S2_NDVI_V2",
      oscarsLinkTitles = singletonList("NDVI_10M"),
      rootPath = "/data/MTDA/TERRASCOPE_Sentinel2/NDVI_V2"
    )
    val dateWithClearPostelArea = ZonedDateTime.of(LocalDate.of(2020, 5, 5), MIDNIGHT, UTC)
    val bbox = ProjectedExtent(Extent(5.176178620365679,51.24922676145928,5.258576081303179,51.27449711952613), LatLng)
    val layer = factory.layer(bbox, dateWithClearPostelArea, dateWithClearPostelArea, 11)
    return layer
  }

}
