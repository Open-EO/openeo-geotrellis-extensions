package org.openeo.geotrellis.icor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;

import geotrellis.layer.*;
import geotrellis.raster.*;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class testAtmosphericCorrectionProcess {

    @BeforeClass
    public static void sparkContext() {
        SparkConf conf = new SparkConf();
        conf.setAppName("OpenEOTest");
        conf.setMaster("local[4]");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext.getOrCreate(conf);
    }

    @AfterClass
    public static void shutDownSparkContext() {
        SparkContext.getOrCreate().stop();
    }

    @Test
    public void testAtmosphericCorrection() {

    	ArrayList<Object> bandIds=new ArrayList<Object>();
    	bandIds.add(new Integer(1));
    	bandIds.add(new Integer(3));
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
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new AtmosphericCorrection().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	datacube,
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
