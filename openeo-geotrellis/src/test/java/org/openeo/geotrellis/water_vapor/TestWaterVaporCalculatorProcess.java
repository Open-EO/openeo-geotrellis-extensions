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
import org.junit.Ignore;
import org.junit.Test;
import org.openeo.geotrellis.icor.LookupTable;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import geotrellis.layer.*;
import java.time.ZonedDateTime;
import geotrellis.spark.testkit.TileLayerRDDBuilders$;
import org.apache.spark.api.java.JavaPairRDD$;
import scala.Tuple2;
import scala.collection.Seq;

public class TestWaterVaporCalculatorProcess {

	static final double sza=43.5725342155;
	static final double vza=6.95880821756;
	static final double saa=116.584011516;
	static final double vaa=0.;
	static final double aot=0.1;
	static final double ozone=0.33;

	static final double cwv=42.557835*10000. *(Math.PI/Math.cos(sza*Math.PI/180.)/817.58);
	static final double r0=112.916855*10000. *(Math.PI/Math.cos(sza*Math.PI/180.)/953.93);
	static final double r1=11.206167*10000.  *(Math.PI/Math.cos(sza*Math.PI/180.)/247.08);
	
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
    public void testWaterVaporCalculatorOverrideAngles() {

        Tile wvTile = new IntConstantTile((int)cwv,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r0Tile = new IntConstantTile((int)r0,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        Tile r1Tile = new IntConstantTile((int)r1,256,256,(IntCells)CellType$.MODULE$.fromName("int32raw").withDefaultNoData()).mutable();
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = wvTilesToSpaceTimeDataCube(wvTile, r0Tile, r1Tile, null, null, null, null);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new ComputeWaterVapor().computeStandaloneCWV(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	new ContextRDD<>(datacube.rdd(),updatedMetadata),
        	Arrays.asList(new String[] {"B09","B8A","B11"}),
        	Arrays.asList(new Double[] {1.e-4,1.}),
        	Arrays.asList(new Double[] {sza,saa,vza,vaa,aot,ozone}),
        	"SENTINEL2"
        );
        //System.out.println(resultRDD.getClass().toString());

        
        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();

        double resultAt00=tiles.values().iterator().next().band(0).getDouble(0, 0);
        System.out.println(Double.toString(resultAt00));
		assertEquals(resultAt00,0.4321802854537964,1.e-3); // elevated tolerance because input wv,r0,r1 is converted to int

        double resultAtNN=tiles.values().iterator().next().band(0).getDouble(255, 255);
        System.out.println(Double.toString(resultAtNN));
		assertEquals(resultAtNN,0.45203205943107605,1.e-3); // elevated tolerance because input wv,r0,r1 is converted to int
    }

    @Test
    public void testWaterVaporCalculatorAnglesFromTiles() {

        Tile wvTile = new DoubleConstantTile(cwv,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile r0Tile = new DoubleConstantTile(r0,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile r1Tile = new DoubleConstantTile(r1,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile vzaTile = new DoubleConstantTile(vza,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile vaaTile = new DoubleConstantTile(vaa,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile szaTile = new DoubleConstantTile(sza,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();
        Tile saaTile = new DoubleConstantTile(saa,256,256,(DoubleCells)CellType$.MODULE$.fromName("float64raw").withDefaultNoData()).mutable();

        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> datacube = wvTilesToSpaceTimeDataCube(wvTile, r0Tile, r1Tile, szaTile, vzaTile, vaaTile, saaTile);
        TileLayerMetadata<SpaceTimeKey> m = datacube.metadata();
        Extent newExtent = new Extent(3.5, 50, 4.0, 51);
        TileLayerMetadata<SpaceTimeKey> updatedMetadata = m.copy(m.cellType(),new LayoutDefinition(newExtent,m.layout().tileLayout()), newExtent,m.crs(),m.bounds());
        
        ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> resultRDD=new ComputeWaterVapor().computeStandaloneCWV(
        	JavaSparkContext.fromSparkContext(SparkContext.getOrCreate()),
        	new ContextRDD<>(datacube.rdd(),updatedMetadata),
        	Arrays.asList(new String[] {"B09","B8A","B11","sunZenithAngles","viewZenithMean","viewAzimuthMean","sunAzimuthAngles"}),
        	Arrays.asList(new Double[] {1.e-4,1.}),
        	Arrays.asList(new Double[] {Double.NaN,Double.NaN,Double.NaN,Double.NaN,aot,ozone}),
        	"SENTINEL2"
        );
        //System.out.println(resultRDD.getClass().toString());
        
        JavaPairRDD<SpaceTimeKey, MultibandTile> result = JavaPairRDD.fromJavaRDD(resultRDD.toJavaRDD());
        assertFalse(result.isEmpty());
        Map<SpaceTimeKey, MultibandTile> tiles = result.collectAsMap();

        double resultAt00=tiles.values().iterator().next().band(0).getDouble(0, 0);
        System.out.println(Double.toString(resultAt00));
		assertEquals(resultAt00,0.43252936005592346,1.e-6);

        double resultAtNN=tiles.values().iterator().next().band(0).getDouble(255, 255);
        System.out.println(Double.toString(resultAtNN));
		assertEquals(resultAtNN,0.45267966389656067,1.e-6);
		
    }
    
}
