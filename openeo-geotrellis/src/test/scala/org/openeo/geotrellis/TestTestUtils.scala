package org.openeo.geotrellis

import org.junit.jupiter.api.Test
import org.openeo.geotrellis.TestUtils.buildPlainSpatioTemporalDataCube
import org.openeo.geotrellis.geotiff.{GTiffOptions, saveRDDTemporal}
import org.openeo.geotrelliscommon.DataCubeParameters

import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

object TestTestUtils extends LocalSparkContextJupyter {}

class TestTestUtils {
  @Test def testBuildPlainSpatioTemporalDataCube(): Unit = {
    val path = "tmp/testBuildPlainSpatioTemporalDataCube/"
    new Directory(Paths.get(path).toFile).deleteRecursively()
    Files.createDirectories(Paths.get(path))

    val pe = geotrellis.vector.ProjectedExtent(
      geotrellis.vector.Extent(256000.0, 5376000.0, 757000.0, 5377000.0),
      geotrellis.proj4.CRS.fromEpsgCode(32631)
    )
    val dcParams = new DataCubeParameters()
    dcParams.layoutScheme = "FloatingLayoutScheme"

    val tileLayerRDD_10M = buildPlainSpatioTemporalDataCube(pe, 10, dcParams)
    val partitions_10m = tileLayerRDD_10M.partitions
    println("10m partitions: " + partitions_10m.length)

    val gtiffOptions = new GTiffOptions
    gtiffOptions.setTileSize(dcParams.tileSize)
    saveRDDTemporal(tileLayerRDD_10M, path + "/", formatOptions = gtiffOptions)

    //    println("Done")
    //    while (true) {
    //      Thread.sleep(1000)  // Keep the Spark context alive for debugging
    //    }
  }

}
