package org.openeo.geotrellis.icor;

import geotrellis.layer.LayoutDefinition;
import geotrellis.layer.SpaceTimeKey;
import geotrellis.layer.TileLayerMetadata;
import geotrellis.raster.*;
import geotrellis.spark.ContextRDD;
import geotrellis.vector.Extent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public class testAtmosphericCorrectionProcess {

    @BeforeClass
    public static void sparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[2]");
        //conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);
    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }

    @Test
    public void testAtmosphericCorrection() {

    	ArrayList<String> bandIds=new ArrayList<String>();
    	bandIds.add(new String("TOC-B02_10M"));
    	bandIds.add(new String("TOC-B04_10M"));
    	ArrayList<Object> scales=new ArrayList<Object>();
    	scales.add(new Double(1.));
    	scales.add(new Double(1.));
    	ArrayList<Object> params=new ArrayList<Object>();
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	params.add(new Double(0.));
    	
        Tile tile0 = new IntConstantTile(256,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = org.openeo.geotrellis.TestOpenEOProcesses.tileToSpaceTimeDataCube(tile0);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	new ContextRDD<>(datacube.rdd(),updatedMetadata),
        	"test_lut",
        	bandIds,
        	scales,
        	params
        );
        System.out.println(resultRDD.getClass().toString());

        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
        
    }

//    @Test
//    public void testAtmosphericCorrection2() {
//
//        Tile tile0 = new IntConstantTile(256,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
//        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = org.openeo.geotrellis.TestOpenEOProcesses.tileToSpaceTimeDataCube(tile0);
//        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),datacube,"lut_s2a");
//        System.out.println(resultRDD.getClass().toString());
//
//        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
//        assertFalse(result.isEmpty());
//        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();
//        
//    }

}
