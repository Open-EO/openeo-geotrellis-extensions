package org.openeo.geotrellis.icor;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;

import geotrellis.layer.SpaceTimeKey;
import geotrellis.layer.TileLayerMetadata;
import geotrellis.raster.IntCells;
import geotrellis.raster.IntConstantTile;
import geotrellis.raster.MultibandTile;
import geotrellis.raster.Tile;
import geotrellis.spark.ContextRDD;

class testAtmosphericCorrectionProcess {

    @Test
    public void testAtmosphericCorrection() {

    	System.out.println("**RRRR******************************************************************************************************************");

        Tile tile0 = new IntConstantTile(1,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = tileToSpaceTimeDataCube(tile0);
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(datacube);
        System.out.println(resultRDD.getClass().toString());

        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
        
        System.out.println("**RRRR******************************************************************************************************************");

    }

}
