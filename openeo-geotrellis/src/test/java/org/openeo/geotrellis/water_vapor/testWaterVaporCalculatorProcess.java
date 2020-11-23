package org.openeo.geotrellis.water_vapor;

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

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import geotrellis.layer.*;
import java.time.ZonedDateTime;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.spark.api.java.JavaPairRDD$;
import scala.Tuple2;

public class testWaterVaporCalculatorProcess {

	static String lutPath="https://artifactory.vgt.vito.be/auxdata-public/lut/S2A_all.bin";
	
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

    
    public static ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> wvTilesToSpaceTimeDataCube(
    	Tile wvTile, Tile r0Tile, Tile r1Tile,
    	Tile szaTile, Tile vzaTile, Tile vaaTile, Tile saaTile
    ) {

        ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>> datacube = 
    		(ContextRDD<SpatialKey, MultibandTile, TileLayerMetadata<SpatialKey>>) 
    		TileLayerRDDBuilders$.MODULE$.createMultibandTileLayerRDD(
    			SparkContext.getOrCreate(), new ArrayMultibandTile(
    				szaTile==null ? new Tile[]{wvTile,r0Tile,r1Tile} : new Tile[]{wvTile,r0Tile,r1Tile,szaTile,vzaTile,vaaTile,saaTile}), 
    				new TileLayout(1, 1, ((Integer) wvTile.cols()), ((Integer) wvTile.rows()))
    		);
        final ZonedDateTime minDate = ZonedDateTime.parse("2017-01-01T00:00:00Z");
        JavaPairRDD<SpaceTimeKey, MultibandTile> spacetimeDataCube = JavaPairRDD$.MODULE$.fromJavaRDD(datacube.toJavaRDD()).flatMapToPair(spatialKeyMultibandTileTuple2 -> {
            return Arrays.asList(
                    Tuple2.apply(SpaceTimeKey.apply(spatialKeyMultibandTileTuple2._1, TemporalKey.apply(minDate)), spatialKeyMultibandTileTuple2._2)
            ).iterator();
        });
        TileLayerMetadata<SpatialKey> m = datacube.metadata();
        Bounds<SpatialKey> bounds = m.bounds();

        SpaceTimeKey minKey = SpaceTimeKey.apply(bounds.get().minKey(), TemporalKey.apply(minDate));
        KeyBounds<SpaceTimeKey> updatedKeyBounds = new KeyBounds<>(minKey,minKey);
        TileLayerMetadata<SpaceTimeKey> metadata = new TileLayerMetadata<>(m.cellType(), m.layout(), m.extent(),m.crs(), updatedKeyBounds);

        return (ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>>) new ContextRDD(spacetimeDataCube.rdd(), metadata);
    }

    
    @Test
    public void testAtmosphericCorrectionOverrideAngles() {

        Tile wvTile = new IntConstantTile(425578,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r0Tile = new IntConstantTile(1129168,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r1Tile = new IntConstantTile(112061,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = wvTilesToSpaceTimeDataCube(wvTile, r0Tile, r1Tile, null, null, null, null);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new ComputeWaterVapor().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	new ContextRDD<>(datacube.rdd(),updatedMetadata),
        	lutPath,
        	Arrays.asList(new String[] {"B09","B8A","B11"}),
        	Arrays.asList(new Double[] {1.e-4,1.}),
        	Arrays.asList(new Double[] {43.5725342155,116.584011516,6.95880821756,0.,0.1,0.33})
        );
        //System.out.println(resultRDD.getClass().toString());

        
        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();

        double resultAt00=tiles.values().iterator().next().band(0).getDouble(0, 0);
        System.out.println(Double.toString(resultAt00));
		assertEquals(resultAt00,0.53937,1.e-4);

    }

    @Test
    public void testAtmosphericCorrectionAnglesFromTiles() {

        Tile wvTile = new DoubleConstantTile(425578.,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile r0Tile = new DoubleConstantTile(1129168.,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile r1Tile = new DoubleConstantTile(112061.,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile vzaTile = new DoubleConstantTile(6.95880821756,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile vaaTile = new DoubleConstantTile(0.,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile szaTile = new DoubleConstantTile(43.5725342155,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile saaTile = new DoubleConstantTile(116.584011516,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = wvTilesToSpaceTimeDataCube(wvTile, r0Tile, r1Tile, szaTile, vzaTile, vaaTile, saaTile);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new ComputeWaterVapor().correct(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	new ContextRDD<>(datacube.rdd(),updatedMetadata),
        	lutPath,
        	Arrays.asList(new String[] {"B09","B8A","B11","sunZenithAngles","viewZenithMean","viewAzimuthMean","sunAzimuthAngles"}),
        	Arrays.asList(new Double[] {1.e-4,1.}),
        	Arrays.asList(new Double[] {Double.NaN,Double.NaN,Double.NaN,Double.NaN,0.1,0.33})
        );
        //System.out.println(resultRDD.getClass().toString());
        
        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();

        double resultAt00=tiles.values().iterator().next().band(0).getDouble(0, 0);
        System.out.println(Double.toString(resultAt00));
		assertEquals(resultAt00,0.53937,1.e-4);

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
