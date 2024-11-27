package org.openeo.geotrelliscommon

import geotrellis.layer.{FloatingLayoutScheme, KeyBounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, TileLayout, UByteConstantNoDataCellType}
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.time.{LocalDate, ZoneId}

class DataCubeSupportSpec {

  @Ignore
  @Test def testLayerMetadata(): Unit = {

    val box = ProjectedExtent(Extent(660280.2335363723, 4830543.527054116, 660807.1934326619, 4830999.507525481),CRS.fromEpsgCode(32630))
    val globalBounds = ProjectedExtent(Extent(-1.0135500738874659, 43.61065306598471, -1.007392304507547, 43.61453493423263),LatLng)
    print(box.reproject(LatLng))
    val start = LocalDate.parse("2020-04-02").atStartOfDay(ZoneId.systemDefault())
    val end = LocalDate.parse("2020-04-20").atStartOfDay(ZoneId.systemDefault())
    val meta = DatacubeSupport.layerMetadata(box,start,end,10,FloatConstantNoDataCellType,FloatingLayoutScheme(256), CellSize(10, 10),Some(globalBounds))
    print(meta)
    val min = meta.bounds.get.minKey
    assert(min.col >= 0)
    assert(min.row >= 0)

  }

  @Test def testLayerMetadataBending(): Unit = {

    val box = ProjectedExtent(Extent(474275.94784671185, 5652716.683271102, 551324.9238115384, 5671345.942297149),CRS.fromEpsgCode(32631))
    val globalBounds = ProjectedExtent(Extent(2.6326987228571985, 51.02546427180431, 3.734467367171235, 51.19122012259554),LatLng)
    print(box.reproject(LatLng))
    val start = LocalDate.parse("2020-04-02").atStartOfDay(ZoneId.systemDefault())
    val end = LocalDate.parse("2020-04-20").atStartOfDay(ZoneId.systemDefault())
    val meta = DatacubeSupport.layerMetadata(box,start,end,10,FloatConstantNoDataCellType,FloatingLayoutScheme(256), CellSize(10, 10),Some(globalBounds))
    print(meta)
    val min = meta.bounds.get.minKey
    assert(min.col >= 0)
    assert(min.row >= 0)

  }

  @Test
  def optimizeChunkSize():Unit ={
    val metadata = TileLayerMetadata(UByteConstantNoDataCellType,LayoutDefinition(Extent(646660.0, 5678790.0, 649220.0, 5681350.0),TileLayout(1,1,256,256)),Extent(646668.7622376741, 5681108.99671377, 646917.3273239338, 5681335.60711388),CRS.fromEpsgCode(32631),KeyBounds(SpaceTimeKey(0,0,1588809600000L),SpaceTimeKey(0,0,1588809600000L)))
    val parameters = new DataCubeParameters
    parameters.setLayoutScheme("FloatingLayoutScheme")
    val newMetadata = DatacubeSupport.optimizeChunkSize(metadata, Array(MultiPolygon()), Some(parameters), 1).get
    print(newMetadata)
    assertEquals(128,newMetadata.tileCols)
    assertEquals(0,newMetadata.bounds.get.maxKey.col)
    assertEquals(0,newMetadata.bounds.get.maxKey.row)
    assertEquals(0,newMetadata.bounds.get.minKey.row)
    assertEquals(0,newMetadata.bounds.get.minKey.col)

  }
}
